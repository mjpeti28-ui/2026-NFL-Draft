"""Extract article text from a web URL (articles, Substacks, scouting sites)."""

import re
import httpx
import trafilatura
from trafilatura.settings import use_config


def extract(url: str) -> dict:
    """
    Fetch and extract clean text from a web article URL.

    Returns:
        {
            "text": str,
            "title": str,
            "date": str | None,   # ISO date string if found
            "url": str,
        }
    Raises ValueError on fetch/parse failure.
    """
    try:
        downloaded = trafilatura.fetch_url(url)
    except Exception as e:
        raise ValueError(f"Failed to fetch URL: {e}")

    if not downloaded:
        raise ValueError(f"Could not download content from: {url}")

    # Try trafilatura first (best for articles)
    traf_config = use_config()
    traf_config.set("DEFAULT", "EXTRACTION_TIMEOUT", "30")

    text = trafilatura.extract(
        downloaded,
        config=traf_config,
        include_comments=False,
        include_tables=True,
        no_fallback=False,
    )

    metadata = trafilatura.extract_metadata(downloaded)
    title = metadata.title if metadata and metadata.title else ""
    date = metadata.date if metadata and metadata.date else None

    if not text:
        # Fallback: try to get raw text via httpx + basic cleanup
        try:
            resp = httpx.get(url, timeout=20, follow_redirects=True,
                             headers={"User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()
            # Strip HTML tags crudely
            text = re.sub(r"<[^>]+>", " ", resp.text)
            text = re.sub(r"\s+", " ", text).strip()
            if len(text) < 100:
                raise ValueError("Extracted text too short to be useful")
        except Exception as e:
            raise ValueError(f"Content extraction failed for {url}: {e}")

    return {
        "text": text.strip(),
        "title": title,
        "date": date,
        "url": url,
    }
