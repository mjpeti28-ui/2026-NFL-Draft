"""Player name → Airtable record matching."""

from rapidfuzz import process, fuzz
from pipeline.airtable import get_player_cache, create_player_stub


def match_player(name: str) -> dict:
    """
    Fuzzy-match an extracted player name against the player cache.

    Returns:
        {
            "id": str | None,
            "matched_name": str,
            "confidence": "high" | "low" | "new",
            "score": float,
        }
    """
    name = name.strip()
    if not name:
        return {"id": None, "matched_name": name, "confidence": "new", "score": 0}

    players = get_player_cache()
    if not players:
        return {"id": None, "matched_name": name, "confidence": "new", "score": 0}

    full_names = [p["full_name"] for p in players]
    result = process.extractOne(
        name,
        full_names,
        scorer=fuzz.token_sort_ratio,
        score_cutoff=50,
    )

    if result is None:
        return {"id": None, "matched_name": name, "confidence": "new", "score": 0}

    matched_name, score, idx = result
    player = players[idx]

    if score >= 88:
        confidence = "high"
    else:
        confidence = "low"

    return {
        "id": player["id"],
        "matched_name": matched_name,
        "player_key": player.get("player_key", ""),
        "confidence": confidence,
        "score": score,
    }


def resolve_or_create_player(
    name: str,
    position: str = "",
    school: str = "",
) -> dict:
    """
    Match a player name. If no match, create a stub record.

    Returns same shape as match_player, always with an id.
    """
    result = match_player(name)
    if result["id"] is not None:
        return result

    # Parse first/last from extracted name
    parts = name.strip().split()
    first = parts[0] if parts else name
    last = " ".join(parts[1:]) if len(parts) > 1 else ""

    new_id = create_player_stub(first, last, position, school)
    return {
        "id": new_id,
        "matched_name": name,
        "player_key": f"{first[0]}. {last}" if first and last else name,
        "confidence": "new",
        "score": 0,
    }
