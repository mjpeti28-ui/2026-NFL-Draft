"""Extract transcript and metadata from a YouTube URL."""

import re
import httpx
from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound, TranscriptsDisabled


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
        url = f"https://www.youtube.com/oembed?url=https://youtu.be/{video_id}&format=json"
        resp = httpx.get(url, timeout=10, follow_redirects=True)
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

    try:
        transcript_list = YouTubeTranscriptApi.get_transcript(video_id)
    except NoTranscriptFound:
        # Try any available language
        try:
            t = YouTubeTranscriptApi.list_transcripts(video_id)
            transcript_list = t.find_generated_transcript(
                [tr.language_code for tr in t]
            ).fetch()
        except Exception as e:
            raise ValueError(f"No transcript available for video {video_id}: {e}")
    except TranscriptsDisabled:
        raise ValueError(f"Transcripts are disabled for video {video_id}")
    except Exception as e:
        raise ValueError(f"Failed to fetch transcript: {e}")

    text = " ".join(seg["text"] for seg in transcript_list)
    # Clean up common transcript artifacts
    text = re.sub(r"\[.*?\]", "", text)          # remove [Music], [Applause] etc.
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
