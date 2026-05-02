from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from .models import Source, Subscriber, Team


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = PROJECT_ROOT / "config"


def read_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, sort_keys=True)
        handle.write("\n")


def load_teams(path: Path | None = None) -> dict[str, Team]:
    rows = read_json(path or CONFIG_DIR / "teams.json")
    teams = [Team.from_dict(row) for row in rows]
    return {team.id: team for team in teams}


def load_sources(path: Path | None = None) -> dict[str, Source]:
    rows = read_json(path or CONFIG_DIR / "sources.json")
    sources = [Source.from_dict(row) for row in rows]
    return {source.id: source for source in sources}


def load_subscribers(path: Path | None = None) -> list[Subscriber]:
    env_json = os.environ.get("MORNING_LINEUP_SUBSCRIBERS_JSON", "").strip()
    if env_json:
        rows = json.loads(env_json)
    else:
        subscribers_path = path or CONFIG_DIR / "subscribers.json"
        if not subscribers_path.exists():
            return []
        rows = read_json(subscribers_path)
    return [Subscriber.from_dict(row) for row in rows]


def save_subscriber(path: Path, subscriber: Subscriber) -> None:
    rows: list[dict[str, Any]]
    if path.exists():
        rows = read_json(path)
    else:
        rows = []
    next_row = subscriber.to_dict()
    rows = [row for row in rows if row.get("email", "").lower() != subscriber.email.lower()]
    rows.append(next_row)
    write_json(path, rows)


def official_mlb_source(team: Team) -> Source | None:
    if not team.mlb_slug:
        return None
    return Source(
        id=f"mlb_{team.id}",
        name=f"MLB.com {team.name}",
        url=f"https://www.mlb.com/{team.mlb_slug}/news",
        source_type="html_index",
        targets=(team.id,),
        weight=7.0,
    )
