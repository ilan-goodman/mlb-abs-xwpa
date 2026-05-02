from __future__ import annotations

import os
from datetime import date
from urllib.parse import urlencode

from .models import Article, Subscriber, Team


THANK_YOU_MESSAGES = [
    "Thank you for printing and sharing these stories. A good box score is better with company.",
    "Thank you for making these articles easy to hold, read, fold, and pass across the table.",
    "Thank you for turning a morning inbox into a small stack of baseball reading.",
    "Thank you for helping the game travel from screen to paper, where it can slow down a little.",
    "Thank you for sharing today's baseball reading. The best fans deserve a readable copy.",
    "Thank you for printing this and making room for one more baseball conversation today.",
    "Thank you for carrying these stories to someone who loves the game.",
]


def thank_you_for(subscriber: Subscriber, issue_date: date) -> str:
    seed = sum(ord(char) for char in f"{subscriber.email}-{issue_date.isoformat()}")
    return THANK_YOU_MESSAGES[seed % len(THANK_YOU_MESSAGES)]


def manage_link_for(subscriber: Subscriber) -> str:
    base_url = os.environ.get(
        "MORNING_LINEUP_MANAGE_URL",
        "https://ilan-goodman.github.io/mlb-abs-xwpa/morning-lineup/",
    ).strip()
    owner_email = os.environ.get("MORNING_LINEUP_OWNER_EMAIL", "").strip()
    query = {
        "mode": "edit",
        "email": subscriber.email,
        "teams": ",".join(subscriber.team_ids),
        "name": subscriber.name,
    }
    if owner_email:
        query["owner"] = owner_email
    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}{urlencode(query)}"


def unsubscribe_link_for(subscriber: Subscriber) -> str:
    base_url = os.environ.get(
        "MORNING_LINEUP_MANAGE_URL",
        "https://ilan-goodman.github.io/mlb-abs-xwpa/morning-lineup/",
    ).strip()
    owner_email = os.environ.get("MORNING_LINEUP_OWNER_EMAIL", "").strip()
    query = {
        "mode": "unsubscribe",
        "email": subscriber.email,
        "name": subscriber.name,
    }
    if owner_email:
        query["owner"] = owner_email
    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}{urlencode(query)}"


def preference_footer(subscriber: Subscriber) -> str:
    return (
        "Manage teams or delivery settings: "
        f"{manage_link_for(subscriber)}\n"
        "Stop these emails: "
        f"{unsubscribe_link_for(subscriber)}"
    )


def build_email_text(subscriber: Subscriber, team: Team, articles: list[Article], issue_date: date) -> str:
    greeting = f"Good morning {subscriber.name}," if subscriber.name else "Good morning,"
    if not articles:
        return (
            f"{greeting}\n\n"
            f"No fresh, freely available {team.name} articles were selected for {issue_date:%B %-d, %Y}.\n\n"
            f"{thank_you_for(subscriber, issue_date)}\n\n"
            f"{preference_footer(subscriber)}"
        )
    lines = [
        greeting,
        "",
        f"Attached are today's printable {team.name} baseball articles for {issue_date:%B %-d, %Y}.",
        "",
    ]
    for idx, article in enumerate(articles, start=1):
        lines.append(f"{idx}. {article.title}")
        lines.append(f"   Source: {article.source_name}")
        lines.append(f"   Link: {article.url}")
    lines.extend(["", thank_you_for(subscriber, issue_date), "", preference_footer(subscriber)])
    return "\n".join(lines)


def build_email_html(subscriber: Subscriber, team: Team, articles: list[Article], issue_date: date) -> str:
    text = build_email_text(subscriber, team, articles, issue_date)
    escaped = (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\n", "<br>")
    )
    return f"<div style=\"font-family: Georgia, serif; font-size: 18px; line-height: 1.5;\">{escaped}</div>"
