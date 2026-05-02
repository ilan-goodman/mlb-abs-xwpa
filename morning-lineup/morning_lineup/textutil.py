from __future__ import annotations

import html
import re
import unicodedata
from urllib.parse import urlsplit, urlunsplit


WHITESPACE_RE = re.compile(r"\s+")
WORD_RE = re.compile(r"[a-z0-9']+")


def clean_text(value: str) -> str:
    value = html.unescape(value or "")
    value = WHITESPACE_RE.sub(" ", value)
    return value.strip()


def ascii_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    return normalized.encode("ascii", "ignore").decode("ascii")


def canonical_url(url: str) -> str:
    parts = urlsplit(url)
    scheme = parts.scheme or "https"
    netloc = parts.netloc.lower()
    path = parts.path.rstrip("/") or "/"
    return urlunsplit((scheme, netloc, path, "", ""))


def title_key(title: str) -> str:
    words = WORD_RE.findall((title or "").lower())
    stop = {
        "a",
        "an",
        "and",
        "are",
        "at",
        "by",
        "for",
        "from",
        "in",
        "of",
        "on",
        "the",
        "to",
        "vs",
        "with",
    }
    return " ".join(word for word in words if word not in stop)


def token_set(value: str) -> set[str]:
    return set(title_key(value).split())


def slugify(value: str, fallback: str = "article") -> str:
    value = ascii_text(value).lower()
    value = re.sub(r"[^a-z0-9]+", "-", value).strip("-")
    return value[:70] or fallback
