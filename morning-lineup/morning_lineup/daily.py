from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from .config import CONFIG_DIR, PROJECT_ROOT, official_mlb_source, load_sources, load_subscribers, load_teams
from .curate import candidate_is_relevant, dedupe_candidates, in_window, score_candidate, select_articles
from .emailer import send_or_save
from .extract import extract_article
from .fetch import discover_candidates, fetch_url
from .messages import build_email_html, build_email_text
from .models import Article, ArticleCandidate, Source, Subscriber, Team
from .pdf import make_pdf
from .textutil import slugify


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send printable morning baseball article emails.")
    parser.add_argument("--subscribers", type=Path, default=CONFIG_DIR / "subscribers.json")
    parser.add_argument("--teams", type=Path, default=CONFIG_DIR / "teams.json")
    parser.add_argument("--sources", type=Path, default=CONFIG_DIR / "sources.json")
    parser.add_argument("--output-dir", type=Path, default=PROJECT_ROOT / "out")
    parser.add_argument("--date", help="Issue date to collect, YYYY-MM-DD. Defaults to yesterday per subscriber timezone.")
    parser.add_argument("--team", help="Preview one team id instead of reading subscribers.")
    parser.add_argument("--email", help="Preview one email address instead of reading subscribers.")
    parser.add_argument("--send", action="store_true", help="Actually send email through RESEND_API_KEY or SMTP_* env vars.")
    parser.add_argument("--max-candidates", type=int, default=80)
    return parser.parse_args()


def issue_window(subscriber: Subscriber, override: str | None) -> tuple[date, datetime, datetime]:
    tz = ZoneInfo(subscriber.timezone)
    if override:
        issue = date.fromisoformat(override)
    else:
        issue = datetime.now(tz).date() - timedelta(days=1)
    start = datetime.combine(issue, time.min, tzinfo=tz)
    end = start + timedelta(days=1)
    return issue, start, end


def sources_for(team: Team, all_sources: dict[str, Source]) -> list[Source]:
    sources: list[Source] = []
    official = official_mlb_source(team)
    if official:
        sources.append(official)
    for source in all_sources.values():
        if team.id in source.targets or team.id in team.curated_sources or source.id in team.curated_sources:
            sources.append(source)
        elif team.id == "mlb" and "mlb" in source.targets:
            sources.append(source)
    seen: set[str] = set()
    unique: list[Source] = []
    for source in sources:
        if source.id in seen:
            continue
        seen.add(source.id)
        unique.append(source)
    return unique


def collect_candidates(team: Team, sources: list[Source], subscriber: Subscriber) -> list[ArticleCandidate]:
    candidates: list[ArticleCandidate] = []
    for source in sources:
        for candidate in discover_candidates(source):
            score_candidate(
                candidate,
                team,
                include_minor_leagues=subscriber.include_minor_leagues,
                mlb_general=team.id == "mlb",
            )
            if candidate_is_relevant(candidate, team, subscriber.include_minor_leagues):
                candidates.append(candidate)
    return dedupe_candidates(candidates)


def team_label(teams: list[Team]) -> str:
    if not teams:
        return "Baseball"
    if len(teams) == 1:
        return teams[0].name
    if len(teams) == 2:
        return f"{teams[0].name} and {teams[1].name}"
    return f"{', '.join(team.name for team in teams[:-1])}, and {teams[-1].name}"


def hydrate_articles(
    candidates: list[ArticleCandidate],
    team: Team,
    subscriber: Subscriber,
    window_start: datetime,
    window_end: datetime,
    max_candidates: int,
) -> list[Article]:
    articles: list[Article] = []
    for candidate in sorted(candidates, key=lambda item: item.score, reverse=True)[:max_candidates]:
        if not in_window(candidate.published, window_start, window_end):
            continue
        try:
            html, _ = fetch_url(candidate.url)
        except Exception as exc:
            print(f"warning: skipping {candidate.url}: {exc}", file=sys.stderr)
            continue
        article = extract_article(candidate, html)
        if not article:
            continue
        if not in_window(article.published, window_start, window_end):
            continue
        score_candidate(
            ArticleCandidate(
                title=article.title,
                url=article.url,
                source=candidate.source,
                published=article.published,
                summary=article.summary,
                score=candidate.score,
            ),
            team,
            include_minor_leagues=subscriber.include_minor_leagues,
            mlb_general=team.id == "mlb",
        )
        article.score = candidate.score
        articles.append(article)
    return select_articles(articles, max_articles=subscriber.max_articles)


def render_pdfs(articles: list[Article], subscriber: Subscriber, issue: date, out_dir: Path) -> None:
    pdf_dir = out_dir / issue.isoformat() / "pdfs"
    for idx, article in enumerate(articles, start=1):
        path = pdf_dir / f"{idx:02d}-{slugify(article.title)}.pdf"
        article.pdf_path = make_pdf(article, path, font_size=subscriber.font_size)


def run_for_subscriber(
    subscriber: Subscriber,
    selected_teams: list[Team],
    all_sources: dict[str, Source],
    args: argparse.Namespace,
) -> None:
    issue, window_start, window_end = issue_window(subscriber, args.date)
    label = team_label(selected_teams)
    print(f"Collecting {label} articles for {subscriber.email} ({issue.isoformat()})")
    candidate_map: dict[str, ArticleCandidate] = {}
    for team in selected_teams:
        for candidate in collect_candidates(team, sources_for(team, all_sources), subscriber):
            existing = candidate_map.get(candidate.url)
            if not existing or candidate.score > existing.score:
                candidate_map[candidate.url] = candidate
    articles = hydrate_articles(
        list(candidate_map.values()),
        selected_teams[0] if len(selected_teams) == 1 else Team(id="bundle", name=label, abbreviation=""),
        subscriber,
        window_start,
        window_end,
        args.max_candidates,
    )
    if not articles:
        print(f"No selected articles for {subscriber.email}; skipping email.")
        return
    render_pdfs(articles, subscriber, issue, args.output_dir)
    message_team = selected_teams[0] if len(selected_teams) == 1 else Team(id="bundle", name=label, abbreviation="")
    text_body = build_email_text(subscriber, message_team, articles, issue)
    html_body = build_email_html(subscriber, message_team, articles, issue)
    subject = f"{label} Morning Lineup - {issue:%b %-d, %Y}"
    eml_path = send_or_save(
        subscriber=subscriber,
        team=message_team,
        subject=subject,
        text_body=text_body,
        html_body=html_body,
        articles=articles,
        output_dir=args.output_dir / issue.isoformat() / "emails",
        send=args.send,
    )
    if eml_path:
        print(f"Wrote preview email: {eml_path}")
    else:
        print(f"Sent {len(articles)} article PDFs to {subscriber.email}")


def main() -> int:
    args = parse_args()
    teams = load_teams(args.teams)
    sources = load_sources(args.sources)
    if args.team or args.email:
        subscriber = Subscriber(
            email=args.email or "preview@example.com",
            team_id=args.team or "sf-giants",
            team_ids=(args.team or "sf-giants",),
        )
        subscribers = [subscriber]
    else:
        subscribers = load_subscribers(args.subscribers)
    if not subscribers:
        print("No subscribers configured. Add config/subscribers.json or MORNING_LINEUP_SUBSCRIBERS_JSON.")
        return 0
    for subscriber in subscribers:
        selected_teams = [teams[team_id] for team_id in subscriber.team_ids if team_id in teams]
        if not selected_teams:
            print(f"warning: unknown team ids {subscriber.team_ids!r} for {subscriber.email}", file=sys.stderr)
            continue
        run_for_subscriber(subscriber, selected_teams, sources, args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
