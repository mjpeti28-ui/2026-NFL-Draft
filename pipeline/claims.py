"""Claude-powered claim extraction and artifact metadata detection."""

import json
import re
import anthropic
import config

_client = None


def _get_client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        _client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
    return _client


METADATA_PROMPT = """\
Analyze the following content and extract metadata. Return ONLY a valid JSON object with these keys:

- title: string (title of the content, or a descriptive name if not obvious)
- content_type: one of exactly ["Video", "Podcast Episode", "Article", "Thread", "Other"]
- context_topic: string (one sentence describing what this content covers)
- date_published: string in YYYY-MM-DD format, or null if not detectable
- source_name: string (name of the publication, channel, or author), or null
- source_platform: one of exactly ["YouTube", "Podcast", "Article", "Substack", "Twitter/X", "TikTok", "TV", "Other"]

Return only the JSON object, no explanation.

Content:
{text}
"""

CLAIMS_PROMPT = """\
You are an NFL draft analyst. Extract every distinct, atomic claim about a specific player from the content below.

Rules:
- One claim = one observation about ONE player (never combine two players in one claim)
- claim_text: concise but self-contained (1–2 sentences max). Write it as a declarative statement about the player.
- claim_type: choose the single best fit from this list exactly:
  Strength, Weakness, Projection, Trait, Scheme Fit, Red Flag, Comparison,
  Production, Context, Grade, Measurement, Medical/Character, Ranking, Development
- category: short label for the specific attribute (e.g. "Arm Talent", "Pass Rush", "Draft Range", "Combine", "Route Running", "Injury")
- player_name: full name as mentioned in the content
- source_analyst: name of the analyst/author making the claim if attributable, otherwise null
- position: position group of the player if mentioned or clearly implied (QB, RB, WR, TE, OT, IOL, EDGE, DL, LB, CB, S), otherwise null
- school: college/university of the player if mentioned, otherwise null

Only extract claims about college players projected for the 2026 NFL Draft. Ignore claims purely about current NFL veterans unless the claim is a draft comparison.

For spreadsheet content: treat each player row as a set of claims — one claim per meaningful column value.

Output: a JSON array of objects. Each object must have keys:
player_name, claim_text, claim_type, category, source_analyst, position, school

Return only the JSON array, no explanation.

Content:
{text}
"""


def _call_claude(prompt: str, max_tokens: int = 4096) -> str:
    client = _get_client()
    message = client.messages.create(
        model=config.CLAUDE_MODEL,
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}],
    )
    return message.content[0].text.strip()


def _parse_json_response(text: str) -> any:
    """Extract and parse JSON from a Claude response, handling markdown code fences."""
    # Strip markdown code blocks if present
    text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.MULTILINE)
    text = re.sub(r"\s*```$", "", text, flags=re.MULTILINE)
    return json.loads(text.strip())


def extract_metadata(text: str) -> dict:
    """
    Extract artifact metadata from content text.

    Returns dict with: title, content_type, context_topic, date_published,
                       source_name, source_platform
    """
    # Truncate very long content for metadata extraction
    truncated = text[:8000] if len(text) > 8000 else text
    prompt = METADATA_PROMPT.format(text=truncated)
    raw = _call_claude(prompt, max_tokens=512)
    try:
        data = _parse_json_response(raw)
    except json.JSONDecodeError:
        data = {}

    # Validate/default enum fields
    valid_types = config.VALID_ARTIFACT_TYPES
    valid_platforms = config.VALID_PLATFORMS

    return {
        "title": data.get("title") or "Untitled",
        "content_type": data.get("content_type") if data.get("content_type") in valid_types else "Other",
        "context_topic": data.get("context_topic") or "",
        "date_published": data.get("date_published"),
        "source_name": data.get("source_name"),
        "source_platform": data.get("source_platform") if data.get("source_platform") in valid_platforms else "Other",
    }


def extract_claims(text: str) -> list[dict]:
    """
    Extract structured claims from content text using Claude.

    Returns list of dicts with:
        player_name, claim_text, claim_type, category,
        source_analyst, position, school
    """
    # For very long content, chunk it to avoid token limits
    chunks = _chunk_text(text, max_chars=40_000)
    all_claims = []

    for i, chunk in enumerate(chunks):
        prompt = CLAIMS_PROMPT.format(text=chunk)
        raw = _call_claude(prompt, max_tokens=16000)
        try:
            claims = _parse_json_response(raw)
            if isinstance(claims, list):
                all_claims.extend(claims)
            else:
                print(f"  [claims] chunk {i+1}: unexpected response type {type(claims)}")
        except json.JSONDecodeError as e:
            # Log the failure so we can debug — don't silently swallow
            print(f"  [claims] chunk {i+1}/{len(chunks)}: JSON parse failed: {e}")
            print(f"  [claims] raw response (first 500 chars): {raw[:500]}")
            continue

    # Normalize and validate
    valid_types = set(config.VALID_CLAIM_TYPES)
    result = []
    for c in all_claims:
        if not isinstance(c, dict):
            continue
        player = (c.get("player_name") or "").strip()
        claim_text = (c.get("claim_text") or "").strip()
        if not player or not claim_text:
            continue

        claim_type = c.get("claim_type", "Trait")
        if claim_type not in valid_types:
            claim_type = "Trait"

        result.append({
            "player_name": player,
            "claim_text": claim_text,
            "claim_type": claim_type,
            "category": (c.get("category") or "").strip(),
            "source_analyst": c.get("source_analyst"),
            "position": c.get("position"),
            "school": c.get("school"),
        })

    return result


def _chunk_text(text: str, max_chars: int) -> list[str]:
    """
    Split text into chunks at paragraph or sentence boundaries.

    YouTube transcripts often have no newlines, so we fall back to
    splitting at sentence boundaries ('. ') when needed.
    """
    if len(text) <= max_chars:
        return [text]

    # Try paragraph splitting first
    paragraphs = text.split("\n")
    if len(paragraphs) > 1:
        # Multi-line text — chunk by paragraphs
        chunks = []
        current: list[str] = []
        current_len = 0
        for para in paragraphs:
            if current_len + len(para) > max_chars and current:
                chunks.append("\n".join(current))
                current = [para]
                current_len = len(para)
            else:
                current.append(para)
                current_len += len(para)
        if current:
            chunks.append("\n".join(current))
        return chunks

    # Single-line text (e.g. YouTube transcript) — split at word boundaries
    chunks = []
    start = 0
    while start < len(text):
        end = start + max_chars
        if end >= len(text):
            chunks.append(text[start:])
            break
        # Walk back to the nearest space so we don't cut mid-word
        split_at = text.rfind(" ", start, end)
        if split_at == -1 or split_at <= start:
            split_at = end  # no space found, hard cut
        chunks.append(text[start:split_at])
        start = split_at + 1  # skip the space

    return chunks
