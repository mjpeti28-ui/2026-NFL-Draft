"""Airtable REST API client for the 2026 NFL Draft base."""

import re
import httpx
from typing import Optional
from rapidfuzz import fuzz, process as fuzz_process
import config

AIRTABLE_BASE = "https://api.airtable.com/v0"
HEADERS = lambda: {
    "Authorization": f"Bearer {config.AIRTABLE_PAT}",
    "Content-Type": "application/json",
}


def _url(table_id: str) -> str:
    return f"{AIRTABLE_BASE}/{config.AIRTABLE_BASE_ID}/{table_id}"


def _get_all(table_id: str, fields: list[str]) -> list[dict]:
    """Fetch all records from a table, paginating automatically.

    Uses returnFieldsByFieldId=true so response keys match our field-ID constants.
    """
    records = []
    params = {
        "fields[]": fields,
        "pageSize": 100,
        "returnFieldsByFieldId": "true",
    }
    with httpx.Client(timeout=30) as client:
        while True:
            resp = client.get(_url(table_id), headers=HEADERS(), params=params)
            resp.raise_for_status()
            data = resp.json()
            records.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
            params["offset"] = offset
    return records


# ---------------------------------------------------------------------------
# Caches (populated at startup, refreshable)
# ---------------------------------------------------------------------------

_player_cache: list[dict] = []
_source_cache: list[dict] = []


def refresh_player_cache() -> list[dict]:
    global _player_cache
    raw = _get_all(config.TABLE_PLAYER, [
        config.F_PLAYER_FIRST, config.F_PLAYER_LAST,
        config.F_PLAYER_POSITION, config.F_PLAYER_SCHOOL, config.F_PLAYER_KEY,
    ])
    _player_cache = [
        {
            "id": r["id"],
            "first": r["fields"].get(config.F_PLAYER_FIRST, ""),
            "last": r["fields"].get(config.F_PLAYER_LAST, ""),
            "position": r["fields"].get(config.F_PLAYER_POSITION, ""),
            "school": r["fields"].get(config.F_PLAYER_SCHOOL, ""),
            "player_key": r["fields"].get(config.F_PLAYER_KEY, ""),
            "full_name": (
                r["fields"].get(config.F_PLAYER_FIRST, "") + " " +
                r["fields"].get(config.F_PLAYER_LAST, "")
            ).strip(),
        }
        for r in raw
    ]
    return _player_cache


def get_player_cache() -> list[dict]:
    if not _player_cache:
        refresh_player_cache()
    return _player_cache


def refresh_source_cache() -> list[dict]:
    global _source_cache
    raw = _get_all(config.TABLE_SOURCE, [
        config.F_SOURCE_NAME, config.F_SOURCE_PLATFORM,
        config.F_SOURCE_CHANNEL, config.F_SOURCE_URL,
    ])
    _source_cache = [
        {
            "id": r["id"],
            "name": r["fields"].get(config.F_SOURCE_NAME, ""),
            "platform": r["fields"].get(config.F_SOURCE_PLATFORM, ""),
            "channel": r["fields"].get(config.F_SOURCE_CHANNEL, ""),
            "url": r["fields"].get(config.F_SOURCE_URL, ""),
        }
        for r in raw
    ]
    return _source_cache


def get_source_cache() -> list[dict]:
    if not _source_cache:
        refresh_source_cache()
    return _source_cache


# ---------------------------------------------------------------------------
# Writes
# ---------------------------------------------------------------------------

def create_record(table_id: str, fields: dict) -> str:
    """Create a single record; return its ID.

    typecast=true auto-creates new singleSelect options.
    """
    with httpx.Client(timeout=30) as client:
        resp = client.post(
            _url(table_id),
            headers=HEADERS(),
            json={"fields": fields, "typecast": True},
        )
        if not resp.is_success:
            print(f"[airtable] create error {resp.status_code}: {resp.text[:500]}")
        resp.raise_for_status()
        return resp.json()["id"]


def update_record(table_id: str, record_id: str, fields: dict) -> None:
    with httpx.Client(timeout=30) as client:
        resp = client.patch(
            f"{_url(table_id)}/{record_id}",
            headers=HEADERS(),
            json={"fields": fields},
        )
        resp.raise_for_status()


def create_records_batch(table_id: str, fields_list: list[dict]) -> list[str]:
    """Create up to 10 records per batch; return list of IDs.

    typecast=true lets Airtable auto-create new singleSelect options instead
    of returning 422 INVALID_MULTIPLE_CHOICE_OPTIONS for novel values.
    """
    ids = []
    with httpx.Client(timeout=60) as client:
        for i in range(0, len(fields_list), 10):
            batch = fields_list[i:i + 10]
            resp = client.post(
                _url(table_id),
                headers=HEADERS(),
                json={
                    "records": [{"fields": f} for f in batch],
                    "typecast": True,
                },
            )
            if not resp.is_success:
                print(f"[airtable] batch write error {resp.status_code}: {resp.text[:1000]}")
            resp.raise_for_status()
            ids.extend(r["id"] for r in resp.json()["records"])
    return ids


# ---------------------------------------------------------------------------
# High-level helpers
# ---------------------------------------------------------------------------

def _normalize_source_name(name: str) -> str:
    """Lowercase, strip punctuation/underscores/spaces for fuzzy comparison."""
    return re.sub(r"[\s_\-\.]+", "", name).lower()


def fuzzy_match_source(name: str, threshold: int = 80) -> Optional[dict]:
    """
    Return the best matching source dict from cache, or None if below threshold.
    Uses token-sort ratio on normalised names.
    """
    sources = get_source_cache()
    if not sources:
        return None
    norm_query = _normalize_source_name(name)
    # Build a mapping of normalised name → source
    norm_map = {_normalize_source_name(s["name"]): s for s in sources}
    match = fuzz_process.extractOne(
        norm_query,
        list(norm_map.keys()),
        scorer=fuzz.token_sort_ratio,
    )
    if match and match[1] >= threshold:
        return norm_map[match[0]]
    return None


def find_or_create_source(
    name: str,
    platform: str,
    channel: str = "",
    url: str = "",
) -> str:
    """Return existing source ID (fuzzy-matched) or create a new one."""
    existing = fuzzy_match_source(name)
    if existing:
        return existing["id"]
    fields: dict = {config.F_SOURCE_NAME: name}
    if platform:
        fields[config.F_SOURCE_PLATFORM] = platform
    if channel:
        fields[config.F_SOURCE_CHANNEL] = channel
    if url:
        fields[config.F_SOURCE_URL] = url
    new_id = create_record(config.TABLE_SOURCE, fields)
    refresh_source_cache()
    return new_id


def create_artifact(
    title: str,
    source_id: str,
    artifact_type: str,
    url: str = "",
    date_published: Optional[str] = None,
    context: str = "",
    notes: str = "",
) -> str:
    """Create an Artifact record and return its ID."""
    fields: dict = {
        config.F_ARTIFACT_TITLE: title,
        config.F_ARTIFACT_SOURCE: [source_id],
        config.F_ARTIFACT_TYPE: artifact_type,
    }
    if url:
        fields[config.F_ARTIFACT_URL] = url
    if date_published:
        fields[config.F_ARTIFACT_DATE] = date_published
    if context:
        fields[config.F_ARTIFACT_CONTEXT] = context
    if notes:
        # Airtable long text has a 100,000 char limit
        fields[config.F_ARTIFACT_NOTES] = notes[:99_000]
    return create_record(config.TABLE_ARTIFACT, fields)


def create_player_stub(
    first: str,
    last: str,
    position: str = "",
    school: str = "",
) -> str:
    """Create a minimal Player record; return its ID."""
    player_key = f"{first[0]}. {last}" if first else last
    fields: dict = {
        config.F_PLAYER_FIRST: first,
        config.F_PLAYER_LAST: last,
        config.F_PLAYER_KEY: player_key,
    }
    if position:
        fields[config.F_PLAYER_POSITION] = position
    if school:
        fields[config.F_PLAYER_SCHOOL] = school
    new_id = create_record(config.TABLE_PLAYER, fields)
    refresh_player_cache()
    return new_id


def create_claims_and_link(
    artifact_id: str,
    claims: list[dict],
) -> list[str]:
    """
    Create claim records and link them back to the artifact.
    Each claim dict: {player_id, claim_text, claim_type, category}
    """
    fields_list = []
    for c in claims:
        f: dict = {
            config.F_CLAIM_ARTIFACT: [artifact_id],
            config.F_CLAIM_TEXT: c["claim_text"],
        }
        if c.get("player_id"):
            f[config.F_CLAIM_PLAYER] = [c["player_id"]]
        if c.get("claim_type"):
            f[config.F_CLAIM_TYPE] = c["claim_type"]
        if c.get("category"):
            f[config.F_CLAIM_CATEGORY] = c["category"]
        fields_list.append(f)

    claim_ids = create_records_batch(config.TABLE_CLAIM, fields_list)

    # No need to separately link claims to the artifact — Airtable automatically
    # maintains the bidirectional link when F_CLAIM_ARTIFACT is set on each Claim.

    return claim_ids
