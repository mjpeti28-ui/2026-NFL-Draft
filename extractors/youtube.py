"""Extract transcript and metadata from a YouTube URL."""

import re
import httpx
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import (
    NoTranscriptFound,
    TranscriptsDisabled,
    VideoUnavailable,
)


def _extract_video_id(url: str) -> str | None:
    patterns = [
        r"(?:youtube\.com/watch\?v=|youtu\.be/)([A-Za-z0-9_-]{11})",
        r"youtube\.com/embed/([A-Za-z0-9_-]{11})",
        r"youtube\.com/shorts/([A-Za-z0-9_-]{11})",
    ]
    for pat in patterns:
        m = re.search(pat, url)
        if m:
            return m.group(1)
    return None


def _fetch_oembed_metadata(video_id: str) -> dict:
    """Fetch title and author via YouTube oEmbed (no API key needed)."""
    try:
        oembed_url = f"https://www.youtube.com/oembed?url=https://youtu.be/{video_id}&format=json"
        resp = httpx.get(oembed_url, timeout=10, follow_redirects=True)
        if resp.status_code == 200:
            data = resp.json()
            return {
                "title": data.get("title", ""),
                "author": data.get("author_name", ""),
            }
    except Exception:
        pass
    return {"title": "", "author": ""}


def extract(url: str) -> dict:
    """
    Extract transcript and metadata from a YouTube URL.

    Returns:
        {
            "text": str,           # full transcript as plain text
            "title": str,
            "author": str,         # channel name
            "video_id": str,
            "url": str,
        }
    Raises ValueError if the URL is not a valid YouTube URL or transcript unavailable.
    """
    video_id = _extract_video_id(url)
    if not video_id:
        raise ValueError(f"Could not extract video ID from URL: {url}")

    meta = _fetch_oembed_metadata(video_id)

    # v1.x API: instantiate the class, use .fetch()
    ytt = YouTubeTranscriptApi()

    try:
        transcript = ytt.fetch(video_id)
        snippets = list(transcript)
    except NoTranscriptFound:
        # Try listing all available transcripts and pick one
        try:
            transcript_list = ytt.list(video_id)
            # Prefer manually created, then auto-generated
            available = list(transcript_list)
            if not available:
                raise ValueError(f"No transcripts available for video {video_id}")
            chosen = available[0]
            snippets = list(chosen.fetch())
        except NoTranscriptFound:
            raise ValueError(f"No transcript found for video {video_id}")
        except Exception as e:
            raise ValueError(f"Could not retrieve transcript for {video_id}: {e}")
    except TranscriptsDisabled:
        raise ValueError(f"Transcripts are disabled for video {video_id}")
    except VideoUnavailable:
        raise ValueError(f"Video {video_id} is unavailable")
    except Exception as e:
        raise ValueError(f"Failed to fetch transcript: {e}")

    # v1.x snippets have .text attribute
    text = " ".join(
        s.text if hasattr(s, "text") else s["text"]
        for s in snippets
    )
    # Clean up common transcript artifacts
    text = re.sub(r"\[.*?\]", "", text)       # remove [Music], [Applause] etc.
    text = re.sub(r"\s+", " ", text).strip()

    return {
        "text": text,
        "title": meta["title"],
        "author": meta["author"],
        "video_id": video_id,
        "url": url,
    }


def is_youtube_url(url: str) -> bool:
    return bool(_extract_video_id(url))
