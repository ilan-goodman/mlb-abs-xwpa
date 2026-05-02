from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Team:
    id: str
    name: str
    abbreviation: str
    mlb_slug: str | None = None
    aliases: tuple[str, ...] = ()
    affiliates: tuple[str, ...] = ()
    curated_sources: tuple[str, ...] = ()

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Team":
        return cls(
            id=str(data["id"]),
            name=str(data["name"]),
            abbreviation=str(data.get("abbreviation", "")),
            mlb_slug=data.get("mlb_slug"),
            aliases=tuple(data.get("aliases", ())),
            affiliates=tuple(data.get("affiliates", ())),
            curated_sources=tuple(data.get("curated_sources", ())),
        )


@dataclass(frozen=True)
class Source:
    id: str
    name: str
    url: str
    source_type: str = "rss"
    targets: tuple[str, ...] = ()
    weight: float = 1.0

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Source":
        return cls(
            id=str(data["id"]),
            name=str(data["name"]),
            url=str(data["url"]),
            source_type=str(data.get("type", "rss")),
            targets=tuple(data.get("targets", ())),
            weight=float(data.get("weight", 1.0)),
        )


@dataclass(frozen=True)
class Subscriber:
    email: str
    name: str = ""
    team_id: str = "mlb"
    team_ids: tuple[str, ...] = ("mlb",)
    timezone: str = "America/Los_Angeles"
    send_hour: int = 5
    max_articles: int = 6
    font_size: int = 18
    include_minor_leagues: bool = True

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Subscriber":
        team_ids = tuple(str(team_id) for team_id in data.get("team_ids", ()) if team_id)
        if not team_ids:
            team_ids = (str(data.get("team_id", "mlb")),)
        return cls(
            email=str(data["email"]),
            name=str(data.get("name", "")),
            team_id=team_ids[0],
            team_ids=team_ids,
            timezone=str(data.get("timezone", "America/Los_Angeles")),
            send_hour=int(data.get("send_hour", 5)),
            max_articles=int(data.get("max_articles", 6)),
            font_size=int(data.get("font_size", 18)),
            include_minor_leagues=bool(data.get("include_minor_leagues", True)),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "email": self.email,
            "name": self.name,
            "team_id": self.team_id,
            "team_ids": list(self.team_ids),
            "timezone": self.timezone,
            "send_hour": self.send_hour,
            "max_articles": self.max_articles,
            "font_size": self.font_size,
            "include_minor_leagues": self.include_minor_leagues,
        }


@dataclass
class ArticleCandidate:
    title: str
    url: str
    source: Source
    published: datetime | None = None
    summary: str = ""
    score: float = 0.0


@dataclass
class Article:
    title: str
    url: str
    source_name: str
    published: datetime | None
    text: str
    byline: str = ""
    summary: str = ""
    score: float = 0.0
    pdf_path: Path | None = None
    metadata: dict[str, str] = field(default_factory=dict)
