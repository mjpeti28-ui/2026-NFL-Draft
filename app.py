"""NFL Draft content ingestion pipeline — local web app."""

import os
import webbrowser
from contextlib import asynccontextmanager
from typing import Optional

import anthropic
import uvicorn
from fastapi import FastAPI, File, Form, UploadFile, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import config
from extractors import youtube, web as web_extractor, file as file_extractor
from pipeline import claims as claims_pipeline, players as player_matcher, airtable


def _raise_claude_error(exc: Exception) -> None:
    """Convert Anthropic SDK errors into readable HTTPExceptions."""
    if isinstance(exc, anthropic.AuthenticationError):
        raise HTTPException(401, "Anthropic API key is invalid or missing. Check your .env file.")
    if isinstance(exc, anthropic.PermissionDeniedError):
        raise HTTPException(403, "Anthropic API permission denied. Check your API key scopes.")
    if isinstance(exc, (anthropic.RateLimitError,)):
        raise HTTPException(429, "Anthropic rate limit hit. Wait a moment and try again.")
    if isinstance(exc, anthropic.APIStatusError):
        # Covers 402 (insufficient credits), 500s, etc.
        msg = str(exc)
        if "credit" in msg.lower() or "billing" in msg.lower():
            raise HTTPException(402, f"Anthropic billing issue: {exc.message if hasattr(exc, 'message') else msg}")
        raise HTTPException(exc.status_code, f"Anthropic API error ({exc.status_code}): {exc.message if hasattr(exc, 'message') else msg}")
    if isinstance(exc, anthropic.APIConnectionError):
        raise HTTPException(503, f"Could not connect to Anthropic API: {exc}")
    raise HTTPException(500, f"Claude error: {exc}")


# ---------------------------------------------------------------------------
# Startup / shutdown
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Pre-load caches so first request is fast
    try:
        airtable.refresh_player_cache()
        airtable.refresh_source_cache()
        print(f"  Loaded {len(airtable.get_player_cache())} players, "
              f"{len(airtable.get_source_cache())} sources from Airtable.")
    except Exception as e:
        print(f"  Warning: could not pre-load Airtable caches: {e}")
    yield


app = FastAPI(title="NFL Draft Ingestion", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Serve frontend
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index():
    template_path = os.path.join(os.path.dirname(__file__), "templates", "index.html")
    with open(template_path, encoding="utf-8") as f:
        return f.read()


# ---------------------------------------------------------------------------
# Data endpoints
# ---------------------------------------------------------------------------

@app.get("/api/players")
async def get_players():
    try:
        players = airtable.get_player_cache()
        return [{"id": p["id"], "name": p["full_name"], "position": p["position"],
                 "player_key": p["player_key"]} for p in players]
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/sources")
async def get_sources():
    try:
        sources = airtable.get_source_cache()
        return [{"id": s["id"], "name": s["name"], "platform": s["platform"],
                 "channel": s["channel"]} for s in sources]
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/refresh-cache")
async def refresh_cache():
    airtable.refresh_player_cache()
    airtable.refresh_source_cache()
    return {"players": len(airtable.get_player_cache()),
            "sources": len(airtable.get_source_cache())}


# ---------------------------------------------------------------------------
# Extract endpoint  (no Airtable writes)
# ---------------------------------------------------------------------------

@app.post("/api/extract")
async def extract(
    url: Optional[str] = Form(None),
    text: Optional[str] = Form(None),
    file: Optional[UploadFile] = File(None),
):
    """
    Step 1: Extract content → run Claude → return metadata + claims.
    Does NOT write anything to Airtable.
    """
    raw_text = ""
    hints: dict = {}  # pre-filled hints from the extractor

    try:
        if url and url.strip():
            url = url.strip()
            if youtube.is_youtube_url(url):
                result = youtube.extract(url)
                raw_text = result["text"]
                hints["title"] = result["title"]
                hints["source_name"] = result["author"]
                hints["source_platform"] = "YouTube"
                hints["content_type"] = "Video"
                hints["url"] = url
            else:
                result = web_extractor.extract(url)
                raw_text = result["text"]
                hints["title"] = result.get("title", "")
                hints["date_published"] = result.get("date")
                hints["url"] = url

        elif file is not None:
            content = await file.read()
            result = file_extractor.extract(content, file.filename or "upload")
            raw_text = result["text"]
            hints["title"] = result.get("title", "")

        elif text and text.strip():
            raw_text = text.strip()

        else:
            raise HTTPException(400, "Provide a URL, text, or file.")

    except ValueError as e:
        raise HTTPException(400, str(e))

    if not raw_text:
        raise HTTPException(400, "Could not extract any text from the provided input.")

    # Run Claude — metadata first, then claims
    try:
        meta = claims_pipeline.extract_metadata(raw_text)
    except Exception as e:
        _raise_claude_error(e)

    # Merge extractor hints (higher priority than Claude inferences)
    for k, v in hints.items():
        if v:
            meta[k] = v

    try:
        raw_claims = claims_pipeline.extract_claims(raw_text)
    except Exception as e:
        _raise_claude_error(e)

    # Run player matching on extracted claims
    enriched_claims = []
    for c in raw_claims:
        match = player_matcher.match_player(c["player_name"])
        enriched_claims.append({
            **c,
            "player_id": match["id"],
            "matched_name": match.get("matched_name", c["player_name"]),
            "match_confidence": match["confidence"],
            "match_score": match.get("score", 0),
        })

    # Sort: high confidence first, then low, then new
    order = {"high": 0, "low": 1, "new": 2}
    enriched_claims.sort(key=lambda c: order.get(c["match_confidence"], 3))

    # Fuzzy-match the detected source name against existing sources
    suggested_source_id = None
    suggested_source_name = None
    if meta.get("source_name"):
        matched_source = airtable.fuzzy_match_source(meta["source_name"])
        if matched_source:
            suggested_source_id = matched_source["id"]
            suggested_source_name = matched_source["name"]

    return {
        "metadata": meta,
        "raw_text_length": len(raw_text),
        "claims": enriched_claims,
        "player_count": len({c["player_name"] for c in enriched_claims}),
        "suggested_source_id": suggested_source_id,
        "suggested_source_name": suggested_source_name,
    }


# ---------------------------------------------------------------------------
# Submit endpoint  (writes to Airtable)
# ---------------------------------------------------------------------------

class SourceIn(BaseModel):
    id: Optional[str] = None          # existing source ID
    name: str = ""
    platform: str = "Other"
    channel: str = ""
    url: str = ""


class MetadataIn(BaseModel):
    title: str
    content_type: str = "Other"
    context_topic: str = ""
    date_published: Optional[str] = None
    url: str = ""
    raw_text: str = ""                 # transcript/notes stored in Artifact


class ClaimIn(BaseModel):
    player_name: str
    player_id: Optional[str] = None   # None = auto-create stub
    claim_text: str
    claim_type: str = "Trait"
    category: str = ""
    position: Optional[str] = None
    school: Optional[str] = None
    new_player: bool = False           # hint: definitely a new player


class SubmitRequest(BaseModel):
    source: SourceIn
    metadata: MetadataIn
    claims: list[ClaimIn]


@app.post("/api/submit")
async def submit(req: SubmitRequest):
    """
    Step 2: Write everything to Airtable.
    Creates Source (if needed) → Artifact → Claims → links players.
    """
    try:
        # 1. Resolve source
        if req.source.id:
            source_id = req.source.id
        elif req.source.name:
            source_id = airtable.find_or_create_source(
                name=req.source.name,
                platform=req.source.platform,
                channel=req.source.channel,
                url=req.source.url,
            )
        else:
            raise HTTPException(400, "Source name or ID is required.")

        # 2. Create artifact
        artifact_id = airtable.create_artifact(
            title=req.metadata.title,
            source_id=source_id,
            artifact_type=req.metadata.content_type,
            url=req.metadata.url,
            date_published=req.metadata.date_published,
            context=req.metadata.context_topic,
            notes=req.metadata.raw_text,
        )

        # 3. Resolve player IDs (create stubs for unknowns)
        resolved_claims = []
        new_players_created = []

        for c in req.claims:
            player_id = c.player_id
            if not player_id or c.new_player:
                result = player_matcher.resolve_or_create_player(
                    name=c.player_name,
                    position=c.position or "",
                    school=c.school or "",
                )
                player_id = result["id"]
                if result["confidence"] == "new":
                    new_players_created.append(c.player_name)

            resolved_claims.append({
                "player_id": player_id,
                "claim_text": c.claim_text,
                "claim_type": c.claim_type,
                "category": c.category,
            })

        # 4. Write claims + link to artifact
        claim_ids = airtable.create_claims_and_link(artifact_id, resolved_claims)

        return {
            "success": True,
            "artifact_id": artifact_id,
            "claims_created": len(claim_ids),
            "new_players_created": new_players_created,
            "source_id": source_id,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Submission failed: {e}")


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("\n NFL Draft Ingestion Tool")
    print(" Opening http://localhost:8000 ...\n")
    webbrowser.open("http://localhost:8000")
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
