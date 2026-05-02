from __future__ import annotations

from datetime import date

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


def build_email_text(subscriber: Subscriber, team: Team, articles: list[Article], issue_date: date) -> str:
    greeting = f"Good morning {subscriber.name}," if subscriber.name else "Good morning,"
    if not articles:
        return (
            f"{greeting}\n\n"
            f"No fresh, freely available {team.name} articles were selected for {issue_date:%B %-d, %Y}.\n\n"
            f"{thank_you_for(subscriber, issue_date)}"
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
    lines.extend(["", thank_you_for(subscriber, issue_date)])
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
