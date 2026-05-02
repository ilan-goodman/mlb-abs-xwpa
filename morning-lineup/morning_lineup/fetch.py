from __future__ import annotations

import email.utils
import json
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import Iterable

from .models import ArticleCandidate, Source
from .textutil import canonical_url, clean_text


USER_AGENT = "MorningLineup/0.1 (+https://github.com/ilan-goodman; personal baseball reading tool)"


class FetchError(RuntimeError):
    pass


def fetch_url(url: str, timeout: int = 25) -> tuple[str, str]:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/rss+xml,application/atom+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            content_type = response.headers.get("content-type", "")
            raw = response.read()
    except (urllib.error.URLError, TimeoutError) as exc:
        raise FetchError(f"Could not fetch {url}: {exc}") from exc
    charset = "utf-8"
    match = re.search(r"charset=([^;\s]+)", content_type, re.I)
    if match:
        charset = match.group(1)
    return raw.decode(charset, errors="replace"), content_type


def parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    value = clean_text(value)
    try:
        parsed = email.utils.parsedate_to_datetime(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except (TypeError, ValueError):
        pass
    value = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except ValueError:
        return None


def find_child_text(node: ET.Element, names: Iterable[str]) -> str:
    wanted = set(names)
    for child in node.iter():
        tag = child.tag.split("}", 1)[-1].lower()
        if tag in wanted and child.text:
            return clean_text(child.text)
    return ""


def find_link(node: ET.Element) -> str:
    for child in node.iter():
        tag = child.tag.split("}", 1)[-1].lower()
        if tag != "link":
            continue
        href = child.attrib.get("href")
        if href:
            return clean_text(href)
        if child.text:
            return clean_text(child.text)
    return ""


def parse_feed(xml_text: str, source: Source) -> list[ArticleCandidate]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise FetchError(f"{source.name} did not return parseable RSS/Atom: {exc}") from exc

    item_nodes = [
        node
        for node in root.iter()
        if node.tag.split("}", 1)[-1].lower() in {"item", "entry"}
    ]
    candidates: list[ArticleCandidate] = []
    for node in item_nodes:
        title = find_child_text(node, ("title",))
        url = find_link(node)
        if not title or not url:
            continue
        published = parse_datetime(
            find_child_text(node, ("pubdate", "published", "updated", "dc:date"))
        )
        summary = find_child_text(node, ("description", "summary", "content"))
        candidates.append(
            ArticleCandidate(
                title=title,
                url=url,
                source=source,
                published=published,
                summary=summary,
            )
        )
    return candidates


class LinkIndexParser(HTMLParser):
    def __init__(self, base_url: str):
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.links: list[tuple[str, str]] = []
        self._href: str | None = None
        self._parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        attrs_dict = {key.lower(): value for key, value in attrs if value}
        href = attrs_dict.get("href")
        if not href or href.startswith("#") or href.startswith("mailto:"):
            return
        self._href = urllib.parse.urljoin(self.base_url, href)
        self._parts = []

    def handle_data(self, data: str) -> None:
        if self._href:
            self._parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or not self._href:
            return
        text = clean_text(" ".join(self._parts))
        if len(text) >= 12:
            self.links.append((self._href, text))
        self._href = None
        self._parts = []


def parse_html_index(html_text: str, source: Source) -> list[ArticleCandidate]:
    parser = LinkIndexParser(source.url)
    parser.feed(html_text)
    seen: set[str] = set()
    candidates: list[ArticleCandidate] = []
    source_host = urllib.parse.urlsplit(source.url).netloc.lower()
    source_key = canonical_url(source.url)
    blocked_path_bits = (
        "/author/",
        "/authors/",
        "/category/",
        "/newsletter",
        "/podcast",
        "/staff/",
        "/tag/",
        "/tags/",
        "/tickets",
        "/video/",
        "/videos/",
    )
    for url, title in parser.links:
        host = urllib.parse.urlsplit(url).netloc.lower()
        if host and source_host and host != source_host:
            continue
        normalized = url.split("#", 1)[0]
        path = urllib.parse.urlsplit(normalized).path.lower()
        if canonical_url(normalized) == source_key:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        if any(skip in path for skip in blocked_path_bits):
            continue
        if any(skip in path for skip in ("/scores", "/standings")):
            continue
        title_lower = title.lower().strip()
        if title_lower in {source.name.lower(), urllib.parse.urlsplit(source.url).netloc.lower()}:
            continue
        candidates.append(ArticleCandidate(title=title, url=normalized, source=source))
    return candidates[:80]


def discover_candidates(source: Source) -> list[ArticleCandidate]:
    try:
        text, content_type = fetch_url(source.url)
        if source.source_type == "rss" or "xml" in content_type:
            return parse_feed(text, source)
        if source.source_type == "json":
            return parse_json_feed(text, source)
        return parse_html_index(text, source)
    except FetchError as exc:
        print(f"warning: {exc}", file=sys.stderr)
        return []


def parse_json_feed(text: str, source: Source) -> list[ArticleCandidate]:
    data = json.loads(text)
    rows = data.get("items") if isinstance(data, dict) else data
    candidates: list[ArticleCandidate] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        title = clean_text(str(row.get("title", "")))
        url = clean_text(str(row.get("url") or row.get("link") or ""))
        if not title or not url:
            continue
        candidates.append(
            ArticleCandidate(
                title=title,
                url=url,
                source=source,
                published=parse_datetime(str(row.get("published") or row.get("date") or "")),
                summary=clean_text(str(row.get("summary") or row.get("description") or "")),
            )
        )
    return candidates
