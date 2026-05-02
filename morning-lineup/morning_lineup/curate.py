from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from urllib.parse import urlsplit

from .models import Article, ArticleCandidate, Team
from .textutil import canonical_url, title_key, token_set


POSITIVE_TERMS = {
    "analysis": 4,
    "breakdown": 4,
    "injury": 5,
    "injuries": 5,
    "minor league": 5,
    "minors": 4,
    "prospect": 5,
    "prospects": 5,
    "recap": 8,
    "roster": 5,
    "series preview": 4,
    "takeaways": 5,
    "trade": 4,
}

LOW_VALUE_TERMS = {
    "gamethread": -18,
    "game thread": -18,
    "open thread": -16,
    "odds": -10,
    "podcast": -8,
    "trivia": -12,
    "watch": -4,
}


def team_terms(team: Team, include_minor_leagues: bool = True) -> list[str]:
    terms = [team.name, team.abbreviation, *team.aliases]
    if include_minor_leagues:
        terms.extend(team.affiliates)
    return [term.lower() for term in terms if term]


def score_candidate(
    candidate: ArticleCandidate,
    team: Team,
    include_minor_leagues: bool = True,
    mlb_general: bool = False,
) -> float:
    haystack = f"{candidate.title} {candidate.summary}".lower()
    score = float(candidate.source.weight)
    if mlb_general:
        score += 5
    terms = team_terms(team, include_minor_leagues)
    if team.id == "mlb":
        terms = ["mlb", "major league baseball", "baseball"]
    for term in terms:
        if not term:
            continue
        if term in haystack:
            score += 18 if len(term) > 3 else 7
    for term, value in POSITIVE_TERMS.items():
        if term in haystack:
            score += value
    for term, value in LOW_VALUE_TERMS.items():
        if term in haystack:
            score += value
    if candidate.published is None:
        score -= 2
    candidate.score = score
    return score


def in_window(article_time: datetime | None, start: datetime, end: datetime) -> bool:
    if article_time is None:
        return True
    article_time = article_time.astimezone(start.tzinfo)
    return start <= article_time < end


def candidate_is_relevant(candidate: ArticleCandidate, team: Team, include_minor_leagues: bool) -> bool:
    if team.id == "mlb":
        return candidate.score >= 2
    terms = team_terms(team, include_minor_leagues)
    haystack = f"{candidate.title} {candidate.summary} {candidate.url}".lower()
    return candidate.score >= 8 or any(term in haystack for term in terms if len(term) > 3)


def dedupe_candidates(candidates: list[ArticleCandidate]) -> list[ArticleCandidate]:
    by_url: dict[str, ArticleCandidate] = {}
    by_title: dict[str, ArticleCandidate] = {}
    for candidate in sorted(candidates, key=lambda item: item.score, reverse=True):
        url_key = canonical_url(candidate.url)
        title = title_key(candidate.title)
        existing = by_url.get(url_key) or by_title.get(title)
        if existing:
            continue
        by_url[url_key] = candidate
        if title:
            by_title[title] = candidate
    return list(by_url.values())


def too_similar(title: str, selected: list[Article]) -> bool:
    tokens = token_set(title)
    if not tokens:
        return False
    for article in selected:
        other = token_set(article.title)
        if not other:
            continue
        overlap = len(tokens & other) / max(len(tokens | other), 1)
        if overlap >= 0.62:
            return True
    return False


def select_articles(articles: list[Article], max_articles: int) -> list[Article]:
    selected: list[Article] = []
    per_host: dict[str, int] = defaultdict(int)
    for article in sorted(articles, key=lambda item: item.score, reverse=True):
        host = urlsplit(article.url).netloc.lower()
        if too_similar(article.title, selected):
            continue
        if per_host[host] >= 3 and len(selected) >= 3:
            continue
        selected.append(article)
        per_host[host] += 1
        if len(selected) >= max_articles:
            break
    return selected
