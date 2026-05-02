from __future__ import annotations

import re
from html.parser import HTMLParser
from urllib.parse import urljoin

from .fetch import parse_datetime
from .models import Article, ArticleCandidate
from .textutil import clean_text


PAYWALL_MARKERS = (
    "subscribe to continue",
    "subscription required",
    "already a subscriber",
    "log in to continue",
    "sign in to continue",
    "to continue reading",
    "access to this article",
)


class ArticleParser(HTMLParser):
    block_tags = {"p", "li", "h1", "h2", "h3", "blockquote"}
    ignored_tags = {
        "script",
        "style",
        "noscript",
        "svg",
        "form",
        "button",
        "nav",
        "footer",
        "aside",
        "iframe",
    }

    def __init__(self, url: str):
        super().__init__(convert_charrefs=True)
        self.url = url
        self.title = ""
        self.byline = ""
        self.description = ""
        self.published = None
        self.canonical_url = url
        self.paragraphs: list[str] = []
        self.article_paragraphs: list[str] = []
        self._ignore_depth = 0
        self._article_depth = 0
        self._current_tag: str | None = None
        self._current_parts: list[str] = []
        self._in_title = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        attrs_dict = {key.lower(): value for key, value in attrs if value}
        if tag in self.ignored_tags:
            self._ignore_depth += 1
            return
        if tag == "article":
            self._article_depth += 1
        if tag == "title":
            self._in_title = True
        if tag == "link" and attrs_dict.get("rel") == "canonical" and attrs_dict.get("href"):
            self.canonical_url = urljoin(self.url, attrs_dict["href"])
        if tag == "meta":
            key = (attrs_dict.get("property") or attrs_dict.get("name") or "").lower()
            content = attrs_dict.get("content", "")
            if key in {"og:title", "twitter:title"} and content:
                self.title = clean_text(content)
            elif key in {"description", "og:description", "twitter:description"} and content:
                self.description = clean_text(content)
            elif key in {"article:published_time", "date", "pubdate"} and content:
                self.published = parse_datetime(content)
            elif key in {"author", "article:author"} and content:
                self.byline = clean_text(content)
        if tag in self.block_tags and self._ignore_depth == 0:
            self._current_tag = tag
            self._current_parts = []

    def handle_data(self, data: str) -> None:
        if self._ignore_depth:
            return
        if self._in_title and not self.title:
            self.title = clean_text(data)
        if self._current_tag:
            self._current_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in self.ignored_tags and self._ignore_depth:
            self._ignore_depth -= 1
            return
        if tag == "article" and self._article_depth:
            self._article_depth -= 1
        if tag == "title":
            self._in_title = False
        if tag == self._current_tag:
            text = clean_text(" ".join(self._current_parts))
            if self._current_tag in {"h1", "h2"} and text and not self.title:
                self.title = text
            if len(text) >= 30 and not looks_like_boilerplate(text):
                self.paragraphs.append(text)
                if self._article_depth:
                    self.article_paragraphs.append(text)
            self._current_tag = None
            self._current_parts = []


def looks_like_boilerplate(text: str) -> bool:
    lower = text.lower()
    if len(text) < 30:
        return True
    markers = (
        "advertisement",
        "privacy policy",
        "terms of use",
        "all rights reserved",
        "follow us",
        "share this article",
        "download our app",
        "sign up for our newsletter",
    )
    return any(marker in lower for marker in markers)


def looks_paywalled(text: str) -> bool:
    lower = text.lower()
    return any(marker in lower for marker in PAYWALL_MARKERS)


def extract_article(candidate: ArticleCandidate, html_text: str) -> Article | None:
    parser = ArticleParser(candidate.url)
    parser.feed(html_text)
    paragraphs = parser.article_paragraphs or parser.paragraphs
    seen: set[str] = set()
    cleaned: list[str] = []
    for paragraph in paragraphs:
        paragraph = re.sub(r"\s+", " ", paragraph).strip()
        key = paragraph.lower()
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(paragraph)
    text = "\n\n".join(cleaned)
    if looks_paywalled(text):
        return None
    if len(text) < 450:
        return None
    title = parser.title or candidate.title
    return Article(
        title=clean_text(title),
        url=parser.canonical_url or candidate.url,
        source_name=candidate.source.name,
        published=parser.published or candidate.published,
        byline=parser.byline,
        summary=parser.description or candidate.summary,
        text=text,
        score=candidate.score,
    )
