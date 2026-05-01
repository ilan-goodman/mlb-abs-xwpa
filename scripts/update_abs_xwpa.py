#!/usr/bin/env python3
"""Build ABS challenge xWPA leaderboards from Savant and MLB Stats API data."""

from __future__ import annotations

import argparse
import csv
import html
import io
import json
import math
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
FEEDS = RAW / "mlb_game_feeds"
PROCESSED = ROOT / "data" / "processed"
SITE = ROOT / "site"

SAVANT_LEADERBOARD = "https://baseballsavant.mlb.com/leaderboard/abs-challenges"
SAVANT_SERVICE = "https://baseballsavant.mlb.com/leaderboard/services/abs/{team_id}"
STATS_TEAMS = "https://statsapi.mlb.com/api/v1/teams"
STATS_SCHEDULE = "https://statsapi.mlb.com/api/v1/schedule"
STATS_FEED = "https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"

BASE_INDEX = {"1B": 0, "2B": 1, "3B": 2}
DEFAULT_FAILED_CHALLENGE_RUN_COST = 0.226
RUNS_PER_WIN = 10.0
BALL_RADIUS_FT = 1.45 / 12.0
MIN_MISSED_EXPECTED_XWPA = 0.0005


def mkdirs() -> None:
    for path in (RAW, FEEDS, PROCESSED, SITE):
        path.mkdir(parents=True, exist_ok=True)


def fetch_bytes(url: str, cache_path: Path | None = None, force: bool = False) -> bytes:
    if cache_path and cache_path.exists() and not force:
        return cache_path.read_bytes()

    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "abs-xwpa/0.1 (+https://baseballsavant.mlb.com)",
            "Accept": "application/json,text/csv,text/html;q=0.9,*/*;q=0.8",
        },
    )
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=45) as response:
                payload = response.read()
            if cache_path:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                cache_path.write_bytes(payload)
            return payload
        except Exception as exc:  # pragma: no cover - network retry guard.
            last_exc = exc
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"Failed to fetch {url}: {last_exc}")


def fetch_json(url: str, cache_path: Path | None = None, force: bool = False) -> Any:
    return json.loads(fetch_bytes(url, cache_path, force).decode("utf-8"))


def qs(params: dict[str, Any]) -> str:
    return urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})


def savant_params(year: int) -> dict[str, Any]:
    return {
        "year": year,
        "challengeType": "team-summary",
        "gameType": "regular",
        "level": "mlb",
        "minChal": 1,
        "minOppChal": 0,
        "dataCount": "runs",
        "groupBy": "",
    }


def get_teams(year: int, force: bool = False) -> list[dict[str, Any]]:
    url = f"{STATS_TEAMS}?{qs({'sportId': 1, 'season': year, 'activeStatus': 'Y'})}"
    payload = fetch_json(url, RAW / f"mlb_teams_{year}.json", force)
    teams = []
    for team in payload["teams"]:
        if team.get("sport", {}).get("id") != 1:
            continue
        teams.append(
            {
                "id": int(team["id"]),
                "name": team["name"],
                "teamName": team.get("teamName", team["name"]),
                "abbr": team["abbreviation"],
                "fileCode": team.get("fileCode", "").upper(),
            }
        )
    return sorted(teams, key=lambda t: t["name"])


def fetch_savant_summary_csv(year: int, force: bool = False) -> list[dict[str, str]]:
    params = {
        **savant_params(year),
        "page": 0,
        "pageSize": 50,
        "sort": "net_chal_gained_runs_total",
        "sortDir": "desc",
        "csv": "true",
    }
    url = f"{SAVANT_LEADERBOARD}?{qs(params)}"
    text = fetch_bytes(url, RAW / f"savant_team_summary_{year}.csv", force).decode("utf-8-sig")
    return list(csv.DictReader(io.StringIO(text)))


def fetch_team_abs_rows(team_id: int, year: int, force: bool = False) -> list[dict[str, Any]]:
    url = f"{SAVANT_SERVICE.format(team_id=team_id)}?{qs(savant_params(year))}"
    payload = fetch_json(url, RAW / "savant_team_challenges" / f"{year}_{team_id}.json", force)
    return payload.get("data", [])


def collect_challenges(teams: list[dict[str, Any]], year: int, force: bool = False) -> list[dict[str, Any]]:
    rows_by_key: dict[tuple[int, str, int], dict[str, Any]] = {}
    team_names = {team["id"]: team["name"] for team in teams}

    for team in teams:
        for row in fetch_team_abs_rows(team["id"], year, force):
            if row.get("against") is not False:
                continue
            mode = row.get("team_summary_mode")
            if mode not in {"batter-for", "catcher-for"}:
                continue
            play_id = row.get("play_id")
            if not play_id:
                continue
            challenge_team_id = int(row.get("player_team") or team["id"])
            key = (int(row["game_pk"]), str(play_id), challenge_team_id)
            row = dict(row)
            row["challenge_team_id"] = challenge_team_id
            row["challenge_team_abbr"] = row.get("player_team_abbr") or team["abbr"]
            row["challenge_team_name"] = team_names.get(challenge_team_id, row["challenge_team_abbr"])
            row["challenge_side"] = "batting" if mode == "batter-for" else "fielding"
            row["challenger_name"] = infer_challenger_name(row)
            rows_by_key[key] = row

    rows = list(rows_by_key.values())
    rows.sort(key=lambda r: (r["game_date"], int(r["game_pk"]), int(r["event_inning"]), r["play_id"]))
    return rows


def infer_challenger_name(row: dict[str, Any]) -> str:
    challenger = int_or_none(row.get("challenging_player_id"))
    if challenger is None:
        return ""
    if challenger == int_or_none(row.get("player_at_bat")):
        return flip_name(row.get("batter_name_flipped") or row.get("batter_name") or "")
    if challenger == int_or_none(row.get("pitcher")):
        return flip_name(row.get("pitcher_name_flipped") or row.get("pitcher_name") or "")
    if challenger == int_or_none(row.get("fielder_2")):
        return flip_name(row.get("catcher_name_flipped") or row.get("catcher_name") or "")
    return str(challenger)


def flip_name(name: str) -> str:
    if "," in name:
        last, first = [part.strip() for part in name.split(",", 1)]
        return f"{first} {last}".strip()
    return name.strip()


def int_or_none(value: Any) -> int | None:
    try:
        if value in (None, ""):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def float_or_zero(value: Any) -> float:
    try:
        if value in (None, ""):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def get_completed_game_pks(year: int, end_date: str, force: bool = False) -> list[int]:
    start_date = f"{year}-03-01"
    params = {
        "sportId": 1,
        "gameType": "R",
        "startDate": start_date,
        "endDate": end_date,
    }
    url = f"{STATS_SCHEDULE}?{qs(params)}"
    payload = fetch_json(url, RAW / f"schedule_{year}_{end_date}.json", force)
    game_pks: list[int] = []
    for day in payload.get("dates", []):
        for game in day.get("games", []):
            status = game.get("status", {})
            if status.get("abstractGameState") == "Final" or status.get("codedGameState") == "F":
                game_pks.append(int(game["gamePk"]))
    return sorted(set(game_pks))


def fetch_game_feed(game_pk: int, force: bool = False) -> dict[str, Any]:
    url = STATS_FEED.format(game_pk=game_pk)
    return fetch_json(url, FEEDS / f"{game_pk}.json", force)


@dataclass(frozen=True)
class PitchState:
    game_pk: int
    play_id: str
    inning: int
    is_top: bool
    batting_team_id: int
    fielding_team_id: int
    home_team_id: int
    away_team_id: int
    home_score: int
    away_score: int
    balls: int
    strikes: int
    outs: int
    bases: int
    start_time: str


class RunModel:
    def __init__(self) -> None:
        self.state_counts: dict[tuple[int, int, int, int], Counter[int]] = defaultdict(Counter)
        self.base_out_counts: dict[tuple[int, int], Counter[int]] = defaultdict(Counter)
        self.half_counts: Counter[int] = Counter()
        self._state_dist_cache: dict[tuple[int, int, int, int], dict[int, float]] = {}
        self._continue_cache: dict[tuple[float, int, str, int, int, int], float] = {}
        self.home_team_id = 0
        self.away_team_id = 0
        self.challenge_team_id = 0

    def add_state(self, key: tuple[int, int, int, int], remaining_runs: int) -> None:
        self.state_counts[key][remaining_runs] += 1
        self.base_out_counts[(key[0], key[1])][remaining_runs] += 1

    def add_half(self, runs: int) -> None:
        self.half_counts[runs] += 1

    def dist_for_state(self, outs: int, bases: int, balls: int, strikes: int) -> dict[int, float]:
        key = (outs, bases, balls, strikes)
        if key in self._state_dist_cache:
            return self._state_dist_cache[key]

        exact = self.state_counts.get(key, Counter())
        fallback = self.base_out_counts.get((outs, bases), Counter()) or self.half_counts
        dist = blend_counters(exact, fallback, prior_weight=25)
        self._state_dist_cache[key] = dist
        return dist

    def half_dist(self) -> dict[int, float]:
        return normalize_counter(self.half_counts or Counter({0: 1}))

    def win_prob(
        self,
        score_diff: float,
        inning: int,
        is_top: bool,
        batting_team_id: int,
        challenge_team_id: int,
        home_team_id: int,
        away_team_id: int,
        outs: int,
        bases: int,
        balls: int,
        strikes: int,
        half_over: bool = False,
    ) -> float:
        self.home_team_id = home_team_id
        self.away_team_id = away_team_id
        self.challenge_team_id = challenge_team_id
        half = "top" if is_top else "bottom"
        self._continue_cache.clear()
        if half_over:
            return self._after_half(score_diff, inning, half)
        current_dist = self.dist_for_state(outs, bases, balls, strikes)
        sign = 1 if batting_team_id == challenge_team_id else -1
        return sum(
            prob * self._after_half(score_diff + sign * runs, inning, half)
            for runs, prob in current_dist.items()
        )

    def _continue_from_half_start(self, score_diff: float, inning: int, half: str) -> float:
        if inning > 9:
            return terminal_wp(score_diff)

        key = (round(score_diff, 6), inning, half, self.challenge_team_id, self.home_team_id, self.away_team_id)
        if key in self._continue_cache:
            return self._continue_cache[key]

        if inning == 9 and half == "bottom" and home_leads(score_diff, self.challenge_team_id, self.home_team_id):
            return terminal_wp(score_diff)

        batting_team = self.away_team_id if half == "top" else self.home_team_id
        sign = 1 if batting_team == self.challenge_team_id else -1
        wp = sum(
            prob * self._after_half(score_diff + sign * runs, inning, half)
            for runs, prob in self.half_dist().items()
        )
        self._continue_cache[key] = wp
        return wp

    def _after_half(self, score_diff: float, inning: int, half: str) -> float:
        if inning >= 9:
            if half == "top" and home_leads(score_diff, self.challenge_team_id, self.home_team_id):
                return terminal_wp(score_diff)
            if half == "bottom":
                return terminal_wp(score_diff)

        if half == "top":
            return self._continue_from_half_start(score_diff, inning, "bottom")
        return self._continue_from_half_start(score_diff, inning + 1, "top")


def terminal_wp(score_diff: float) -> float:
    if score_diff > 0:
        return 1.0
    if score_diff < 0:
        return 0.0
    return 0.5


def home_leads(score_diff: float, challenge_team_id: int, home_team_id: int) -> bool:
    home_diff = score_diff if challenge_team_id == home_team_id else -score_diff
    return home_diff > 0


def normalize_counter(counter: Counter[int]) -> dict[int, float]:
    total = sum(counter.values())
    if total <= 0:
        return {0: 1.0}
    return {runs: count / total for runs, count in sorted(counter.items())}


def blend_counters(exact: Counter[int], fallback: Counter[int], prior_weight: int) -> dict[int, float]:
    exact_n = sum(exact.values())
    fallback_dist = normalize_counter(fallback)
    if exact_n <= 0:
        return fallback_dist
    weight = exact_n / (exact_n + prior_weight)
    exact_dist = normalize_counter(exact)
    keys = set(exact_dist) | set(fallback_dist)
    return {
        key: weight * exact_dist.get(key, 0.0) + (1 - weight) * fallback_dist.get(key, 0.0)
        for key in sorted(keys)
    }


def replay_game(feed: dict[str, Any], model: RunModel | None = None) -> dict[str, PitchState]:
    game_pk = int(feed["gamePk"])
    home_team_id = int(feed["gameData"]["teams"]["home"]["id"])
    away_team_id = int(feed["gameData"]["teams"]["away"]["id"])

    challenge_states: dict[str, PitchState] = {}
    half_key: tuple[int, bool] | None = None
    half_states: list[tuple[tuple[int, int, int, int], int]] = []
    half_runs = 0
    bases = 0
    outs = 0
    balls = 0
    strikes = 0
    home_score = 0
    away_score = 0

    def finalize_half() -> None:
        if model is None or half_key is None:
            return
        model.add_half(half_runs)
        for key, runs_so_far in half_states:
            model.add_state(key, max(0, half_runs - runs_so_far))

    for play in feed.get("liveData", {}).get("plays", {}).get("allPlays", []):
        about = play.get("about", {})
        inning = int(about.get("inning", 0))
        is_top = bool(about.get("isTopInning"))
        current_half = (inning, is_top)
        if current_half != half_key:
            finalize_half()
            half_key = current_half
            half_states = []
            half_runs = 0
            bases = 0
            outs = 0

        balls = 0
        strikes = 0
        matchup = play.get("matchup", {})
        batting_team_id = int_or_none(matchup.get("batSideTeamId")) or (away_team_id if is_top else home_team_id)
        fielding_team_id = home_team_id if batting_team_id == away_team_id else away_team_id
        runner_moves = group_runner_moves(play.get("runners", []))
        processed_move_indexes: set[int] = set()

        for event in sorted(play.get("playEvents", []), key=lambda e: int(e.get("index", -1))):
            idx = int(event.get("index", -1))
            if event.get("isPitch") and event.get("playId"):
                play_id = str(event["playId"])
                key = (outs, bases, balls, strikes)
                half_states.append((key, half_runs))
                challenge_states[play_id] = PitchState(
                    game_pk=game_pk,
                    play_id=play_id,
                    inning=inning,
                    is_top=is_top,
                    batting_team_id=batting_team_id,
                    fielding_team_id=fielding_team_id,
                    home_team_id=home_team_id,
                    away_team_id=away_team_id,
                    home_score=home_score,
                    away_score=away_score,
                    balls=balls,
                    strikes=strikes,
                    outs=outs,
                    bases=bases,
                    start_time=event.get("startTime", ""),
                )
                count = event.get("count") or {}
                balls = int(count.get("balls", balls))
                strikes = int(count.get("strikes", strikes))

            if idx in runner_moves:
                bases, outs, runs_scored = apply_runner_moves(bases, outs, runner_moves[idx])
                half_runs += runs_scored
                if is_top:
                    away_score += runs_scored
                else:
                    home_score += runs_scored
                processed_move_indexes.add(idx)

        for idx, moves in runner_moves.items():
            if idx not in processed_move_indexes:
                bases, outs, runs_scored = apply_runner_moves(bases, outs, moves)
                half_runs += runs_scored
                if is_top:
                    away_score += runs_scored
                else:
                    home_score += runs_scored

    finalize_half()
    return challenge_states


def group_runner_moves(runners: list[dict[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for runner in runners:
        details = runner.get("details", {})
        idx = int(details.get("playIndex", 10_000))
        grouped[idx].append(runner)
    return grouped


def apply_runner_moves(bases: int, outs: int, moves: list[dict[str, Any]]) -> tuple[int, int, int]:
    remove_mask = 0
    add_mask = 0
    runs = 0

    for move in moves:
        movement = move.get("movement", {})
        origin = movement.get("originBase") or movement.get("start")
        if origin in BASE_INDEX:
            remove_mask |= 1 << BASE_INDEX[origin]

    bases &= ~remove_mask

    for move in moves:
        movement = move.get("movement", {})
        if movement.get("isOut"):
            outs += 1
            continue
        end = movement.get("end")
        if end == "score":
            runs += 1
        elif end in BASE_INDEX:
            add_mask |= 1 << BASE_INDEX[end]

    bases |= add_mask
    return bases, min(outs, 3), runs


@dataclass(frozen=True)
class CallTransition:
    balls: int
    strikes: int
    outs: int
    bases: int
    runs: int
    half_over: bool


def apply_called_pitch(bases: int, outs: int, balls: int, strikes: int, is_strike: bool) -> CallTransition:
    if is_strike:
        if strikes >= 2:
            outs += 1
            return CallTransition(0, 0, outs, bases, 0, outs >= 3)
        return CallTransition(balls, strikes + 1, outs, bases, 0, False)

    if balls >= 3:
        new_bases, runs = apply_walk(bases)
        return CallTransition(0, 0, outs, new_bases, runs, False)
    return CallTransition(balls + 1, strikes, outs, bases, 0, False)


def apply_walk(bases: int) -> tuple[int, int]:
    first = bool(bases & 0b001)
    second = bool(bases & 0b010)
    third = bool(bases & 0b100)
    runs = 0

    if first:
        if second:
            if third:
                runs += 1
            third = True
            second = True
        else:
            second = True
    first = True

    new_bases = (1 if first else 0) | (2 if second else 0) | (4 if third else 0)
    return new_bases, runs


def score_diff_for_team(row: dict[str, Any], challenge_team_id: int) -> int:
    bat_score = int(float_or_zero(row.get("bat_score")))
    fld_score = int(float_or_zero(row.get("fld_score")))
    if challenge_team_id == int(row["bat_team_id"]):
        return bat_score - fld_score
    return fld_score - bat_score


def score_diff_for_state(state: PitchState, challenge_team_id: int) -> int:
    if challenge_team_id == state.home_team_id:
        return state.home_score - state.away_score
    return state.away_score - state.home_score


def wp_after_transition(
    model: RunModel,
    row: dict[str, Any],
    state: PitchState,
    transition: CallTransition,
    base_score_diff: float,
) -> float:
    challenge_team_id = int(row["challenge_team_id"])
    run_sign = 1 if int(row["bat_team_id"]) == challenge_team_id else -1
    score_diff = base_score_diff + run_sign * transition.runs
    return model.win_prob(
        score_diff=score_diff,
        inning=state.inning,
        is_top=state.is_top,
        batting_team_id=state.batting_team_id,
        challenge_team_id=challenge_team_id,
        home_team_id=state.home_team_id,
        away_team_id=state.away_team_id,
        outs=transition.outs,
        bases=transition.bases,
        balls=transition.balls,
        strikes=transition.strikes,
        half_over=transition.half_over,
    )


def evaluate_challenge(row: dict[str, Any], state: PitchState, model: RunModel) -> dict[str, Any]:
    challenge_team_id = int(row["challenge_team_id"])
    original_is_strike = bool(int(row.get("original_isStrike_ump", 0)))
    overturned_is_strike = not original_is_strike
    was_overturned = bool(int(row.get("is_challengeABS_overturned", 0)))
    actual_is_strike = overturned_is_strike if was_overturned else original_is_strike
    score_diff = score_diff_for_team(row, challenge_team_id)

    original_transition = apply_called_pitch(state.bases, state.outs, state.balls, state.strikes, original_is_strike)
    overturned_transition = apply_called_pitch(state.bases, state.outs, state.balls, state.strikes, overturned_is_strike)
    actual_transition = apply_called_pitch(state.bases, state.outs, state.balls, state.strikes, actual_is_strike)

    wp_original = wp_after_transition(model, row, state, original_transition, score_diff)
    wp_overturned = wp_after_transition(model, row, state, overturned_transition, score_diff)
    wp_actual = wp_after_transition(model, row, state, actual_transition, score_diff)
    wpa_if_overturned = wp_overturned - wp_original
    direct_wpa = wp_actual - wp_original

    lost_run_cost = float_or_zero(row.get("sz_challenge_lost_runs"))
    if lost_run_cost <= 0:
        lost_run_cost = DEFAULT_FAILED_CHALLENGE_RUN_COST

    option_wpa_if_failed = -lost_run_cost / RUNS_PER_WIN
    option_wpa_proxy = 0.0 if was_overturned else option_wpa_if_failed
    denom = wpa_if_overturned - option_wpa_if_failed
    breakeven = (-option_wpa_if_failed / denom) if denom > 0 else None

    return {
        "bases": state.bases,
        "base_state": base_state_label(state.bases),
        "outs": state.outs,
        "balls": state.balls,
        "strikes": state.strikes,
        "inning": state.inning,
        "half": "Top" if state.is_top else "Bot",
        "pitch_start_time": state.start_time,
        "original_call": "Strike" if original_is_strike else "Ball",
        "corrected_call": "Strike" if overturned_is_strike else "Ball",
        "actual_call": "Strike" if actual_is_strike else "Ball",
        "wp_original_call": wp_original,
        "wp_overturned_call": wp_overturned,
        "wp_actual_call": wp_actual,
        "wpa_if_overturned": wpa_if_overturned,
        "direct_wpa": direct_wpa,
        "option_wpa_if_failed": option_wpa_if_failed,
        "option_wpa_proxy": option_wpa_proxy,
        "total_xwpa": direct_wpa,
        "risk_adjusted_xwpa": direct_wpa + option_wpa_proxy,
        "breakeven_overturn_prob": breakeven,
    }


def base_state_label(mask: int) -> str:
    if mask == 0:
        return "---"
    return "".join(base if mask & (1 << idx) else "-" for base, idx in BASE_INDEX.items())


def add_inventory_columns(rows: list[dict[str, Any]]) -> None:
    rows.sort(key=lambda r: (int(r["game_pk"]), int(r["challenge_team_id"]), int(r["inning"]), r["pitch_start_time"]))
    fail_counts: dict[tuple[int, int, str], int] = defaultdict(int)
    for row in rows:
        inning = int(row["inning"])
        bucket, limit = challenge_inventory_bucket(inning)
        key = (int(row["game_pk"]), int(row["challenge_team_id"]), bucket)
        before = fail_counts[key]
        failed = int(row.get("is_challengeABS_overturned", 0)) == 0
        after = min(limit, before + (1 if failed else 0))
        row["challenge_losses_before"] = before
        row["challenge_losses_after"] = after
        row["challenge_loss_limit"] = limit
        row["remaining_challenge_losses_before"] = max(0, limit - before)
        row["exhausted_after"] = failed and after >= limit
        fail_counts[key] = after


def challenge_inventory_bucket(inning: int) -> tuple[str, int]:
    bucket = "reg" if inning <= 9 else f"extra-{inning}"
    return bucket, 2 if bucket == "reg" else 1


def team_meta_from_feed(feed: dict[str, Any]) -> dict[int, dict[str, Any]]:
    teams = feed.get("gameData", {}).get("teams", {})
    output = {}
    for side in ("home", "away"):
        team = teams.get(side, {})
        if "id" not in team:
            continue
        output[int(team["id"])] = {
            "id": int(team["id"]),
            "abbr": team.get("abbreviation") or team.get("fileCode", "").upper(),
            "name": team.get("name") or team.get("teamName") or team.get("abbreviation") or str(team["id"]),
        }
    return output


def player_name(person: dict[str, Any] | None) -> str:
    if not person:
        return ""
    return str(person.get("fullName") or person.get("nameFirstLast") or person.get("boxscoreName") or "")


def strike_zone_miss(call_is_strike: bool, pitch_data: dict[str, Any]) -> tuple[bool, float, float, float, float, float]:
    coordinates = pitch_data.get("coordinates") or {}
    if coordinates.get("pX") in (None, "") or coordinates.get("pZ") in (None, ""):
        return False, 0.0, 0.0, 0.0, 0.0, 0.0
    px = float_or_zero(coordinates.get("pX"))
    pz = float_or_zero(coordinates.get("pZ"))
    top = float_or_zero(pitch_data.get("strikeZoneTop"))
    bottom = float_or_zero(pitch_data.get("strikeZoneBottom"))
    width_inches = float_or_zero(pitch_data.get("strikeZoneWidth")) or 17.0
    if not top or not bottom:
        return False, 0.0, px, pz, bottom, top

    half_width = (width_inches / 2.0) / 12.0 + BALL_RADIUS_FT
    left = -half_width
    right = half_width
    zone_bottom = bottom - BALL_RADIUS_FT
    zone_top = top + BALL_RADIUS_FT
    outside = max(left - px, px - right, zone_bottom - pz, pz - zone_top, 0.0)
    inside = min(px - left, right - px, pz - zone_bottom, zone_top - pz)
    is_inside = inside >= 0
    if call_is_strike:
        return outside > 0, outside, px, pz, bottom, top
    return is_inside, max(inside, 0.0), px, pz, bottom, top


def overturn_probability_from_distance(distance_ft: float) -> float:
    inches = max(0.0, distance_ft * 12.0)
    return 1.0 / (1.0 + math.exp(-((inches - 0.75) / 0.75)))


def build_missed_opportunities(
    game_pks: list[int],
    evaluated_challenges: list[dict[str, Any]],
    model: RunModel,
    force: bool = False,
) -> list[dict[str, Any]]:
    challenge_by_pitch: dict[tuple[int, str], dict[str, Any]] = {}
    for row in evaluated_challenges:
        challenge_by_pitch[(int(row["game_pk"]), str(row["play_id"]))] = row

    fail_counts: dict[tuple[int, int, str], int] = defaultdict(int)
    updated_challenges: set[tuple[int, str]] = set()
    opportunities: list[dict[str, Any]] = []

    for game_pk in game_pks:
        feed = fetch_game_feed(game_pk, force)
        states = replay_game(feed, None)
        teams = team_meta_from_feed(feed)
        official_date = feed.get("gameData", {}).get("datetime", {}).get("officialDate", "")

        for play in feed.get("liveData", {}).get("plays", {}).get("allPlays", []):
            matchup = play.get("matchup", {})
            batter = matchup.get("batter") or {}
            pitcher = matchup.get("pitcher") or {}
            about = play.get("about", {})
            for event in sorted(play.get("playEvents", []), key=lambda e: int(e.get("index", -1))):
                if not event.get("isPitch") or not event.get("playId"):
                    continue
                play_id = str(event["playId"])
                state = states.get(play_id)
                if state is None:
                    continue

                challenge_key = (game_pk, play_id)
                actual_challenge = challenge_by_pitch.get(challenge_key)
                if actual_challenge is None:
                    maybe = missed_opportunity_for_pitch(
                        feed=feed,
                        event=event,
                        state=state,
                        matchup=matchup,
                        about=about,
                        teams=teams,
                        official_date=official_date,
                        fail_counts=fail_counts,
                        model=model,
                        batter=batter,
                        pitcher=pitcher,
                    )
                    if maybe is not None:
                        opportunities.append(maybe)

                if actual_challenge is not None and challenge_key not in updated_challenges:
                    bucket, limit = challenge_inventory_bucket(int(actual_challenge["inning"]))
                    key = (game_pk, int(actual_challenge["challenge_team_id"]), bucket)
                    if int(actual_challenge.get("is_challengeABS_overturned", 0)) == 0:
                        fail_counts[key] = min(limit, fail_counts[key] + 1)
                    updated_challenges.add(challenge_key)

    opportunities.sort(key=lambda row: row["missed_expected_xwpa"], reverse=True)
    return opportunities


def missed_opportunity_for_pitch(
    feed: dict[str, Any],
    event: dict[str, Any],
    state: PitchState,
    matchup: dict[str, Any],
    about: dict[str, Any],
    teams: dict[int, dict[str, Any]],
    official_date: str,
    fail_counts: dict[tuple[int, int, str], int],
    model: RunModel,
    batter: dict[str, Any],
    pitcher: dict[str, Any],
) -> dict[str, Any] | None:
    call = (event.get("details") or {}).get("call") or {}
    call_code = call.get("code")
    if call_code not in {"B", "C"}:
        return None
    call_is_strike = call_code == "C"
    pitch_data = event.get("pitchData") or {}
    is_miss, distance_ft, px, pz, sz_bot, sz_top = strike_zone_miss(call_is_strike, pitch_data)
    if not is_miss or distance_ft <= 0:
        return None

    challenge_team_id = state.batting_team_id if call_is_strike else state.fielding_team_id
    side = "batting" if call_is_strike else "fielding"
    bucket, limit = challenge_inventory_bucket(state.inning)
    inv_key = (state.game_pk, challenge_team_id, bucket)
    losses_before = fail_counts[inv_key]
    remaining = max(0, limit - losses_before)
    if remaining <= 0:
        return None

    row = {
        "challenge_team_id": challenge_team_id,
        "bat_team_id": state.batting_team_id,
        "fld_team_id": state.fielding_team_id,
    }
    score_diff = score_diff_for_state(state, challenge_team_id)
    original_transition = apply_called_pitch(state.bases, state.outs, state.balls, state.strikes, call_is_strike)
    corrected_transition = apply_called_pitch(state.bases, state.outs, state.balls, state.strikes, not call_is_strike)
    wp_original = wp_after_transition(model, row, state, original_transition, score_diff)
    wp_corrected = wp_after_transition(model, row, state, corrected_transition, score_diff)
    wpa_if_overturned = wp_corrected - wp_original
    if wpa_if_overturned <= 0:
        return None

    overturn_probability = overturn_probability_from_distance(distance_ft)
    option_cost = (DEFAULT_FAILED_CHALLENGE_RUN_COST / RUNS_PER_WIN) / remaining
    expected_net = overturn_probability * wpa_if_overturned - (1.0 - overturn_probability) * option_cost
    if expected_net <= MIN_MISSED_EXPECTED_XWPA:
        return None

    team = teams.get(challenge_team_id, {})
    bat_team = teams.get(state.batting_team_id, {})
    fld_team = teams.get(state.fielding_team_id, {})
    role = "hitter" if side == "batting" else "pitcher"
    opportunity_player = batter if role == "hitter" else pitcher

    return {
        "game_pk": state.game_pk,
        "play_id": state.play_id,
        "game_date": official_date,
        "challenge_team_id": challenge_team_id,
        "challenge_team_abbr": team.get("abbr", ""),
        "challenge_team_name": team.get("name", ""),
        "challenge_side": side,
        "role": role,
        "player_id": int_or_none(opportunity_player.get("id")) or "",
        "player_name": player_name(opportunity_player),
        "batter_id": int_or_none(batter.get("id")) or "",
        "batter_name": player_name(batter),
        "pitcher_id": int_or_none(pitcher.get("id")) or "",
        "pitcher_name": player_name(pitcher),
        "bat_team_abbr": bat_team.get("abbr", ""),
        "fld_team_abbr": fld_team.get("abbr", ""),
        "inning": state.inning,
        "half": "Top" if state.is_top else "Bot",
        "outs": state.outs,
        "bases": state.bases,
        "base_state": base_state_label(state.bases),
        "balls": state.balls,
        "strikes": state.strikes,
        "original_call": "Strike" if call_is_strike else "Ball",
        "corrected_call": "Ball" if call_is_strike else "Strike",
        "plate_x": px,
        "plate_z": pz,
        "strike_zone_bottom": sz_bot,
        "strike_zone_top": sz_top,
        "zone_distance_ft": distance_ft,
        "zone_distance_inches": distance_ft * 12.0,
        "overturn_probability": overturn_probability,
        "wp_original_call": wp_original,
        "wp_corrected_call": wp_corrected,
        "wpa_if_overturned": wpa_if_overturned,
        "option_cost_if_failed": option_cost,
        "missed_expected_xwpa": expected_net,
        "decision_penalty_xwpa": -expected_net,
        "challenge_losses_before": losses_before,
        "remaining_challenge_losses_before": remaining,
        "challenge_loss_limit": limit,
        "pitch_start_time": state.start_time or event.get("startTime", ""),
        "pitch_description": (event.get("details") or {}).get("description", ""),
        "pitch_type": ((event.get("details") or {}).get("type") or {}).get("description", ""),
    }


def aggregate_rows(rows: list[dict[str, Any]], group_fields: list[str]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = tuple(row.get(field, "") for field in group_fields)
        grouped[key].append(row)

    output = []
    for key, items in grouped.items():
        attempts = len(items)
        overturns = sum(int(item.get("is_challengeABS_overturned", 0)) for item in items)
        direct_wpa = sum(float(item["direct_wpa"]) for item in items)
        option_wpa = sum(float(item["option_wpa_proxy"]) for item in items)
        total_xwpa = sum(float(item["total_xwpa"]) for item in items)
        risk_adjusted_xwpa = sum(float(item["risk_adjusted_xwpa"]) for item in items)
        row = {field: key[idx] for idx, field in enumerate(group_fields)}
        row.update(
            {
                "attempts": attempts,
                "overturns": overturns,
                "confirms": attempts - overturns,
                "overturn_rate": overturns / attempts if attempts else 0,
                "direct_wpa": direct_wpa,
                "option_wpa_proxy": option_wpa,
                "total_xwpa": total_xwpa,
                "risk_adjusted_xwpa": risk_adjusted_xwpa,
                "xwpa_per_challenge": total_xwpa / attempts if attempts else 0,
                "risk_adjusted_per_challenge": risk_adjusted_xwpa / attempts if attempts else 0,
                "wpa_if_overturned": sum(float(item["wpa_if_overturned"]) for item in items),
                "savant_challenge_runs": sum(float_or_zero(item.get("sz_challenge_runs")) for item in items),
                "strikeout_flips": sum(int(item.get("is_strikeout_overturn", 0)) for item in items),
                "walk_flips": sum(int(item.get("is_walk_overturn", 0)) for item in items),
                "exhausting_fails": sum(1 for item in items if item.get("exhausted_after") is True),
            }
        )
        output.append(row)

    output.sort(key=lambda row: row["total_xwpa"], reverse=True)
    return output


def aggregate_missed_rows(rows: list[dict[str, Any]], group_fields: list[str]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = tuple(row.get(field, "") for field in group_fields)
        grouped[key].append(row)

    output = []
    for key, items in grouped.items():
        count = len(items)
        total = sum(float(item["missed_expected_xwpa"]) for item in items)
        row = {field: key[idx] for idx, field in enumerate(group_fields)}
        row.update(
            {
                "missed_opportunities": count,
                "missed_xwpa": total,
                "missed_xwpa_per_opportunity": total / count if count else 0.0,
                "missed_wpa_if_overturned": sum(float(item["wpa_if_overturned"]) for item in items),
                "avg_missed_overturn_probability": sum(float(item["overturn_probability"]) for item in items) / count if count else 0.0,
                "avg_zone_distance_inches": sum(float(item["zone_distance_inches"]) for item in items) / count if count else 0.0,
                "missed_batting_xwpa": sum(float(item["missed_expected_xwpa"]) for item in items if item.get("challenge_side") == "batting"),
                "missed_fielding_xwpa": sum(float(item["missed_expected_xwpa"]) for item in items if item.get("challenge_side") == "fielding"),
            }
        )
        output.append(row)

    output.sort(key=lambda row: row["missed_xwpa"], reverse=True)
    return output


def add_missed_defaults(row: dict[str, Any]) -> None:
    row.setdefault("missed_opportunities", 0)
    row.setdefault("missed_xwpa", 0.0)
    row.setdefault("missed_xwpa_per_opportunity", 0.0)
    row.setdefault("missed_wpa_if_overturned", 0.0)
    row.setdefault("avg_missed_overturn_probability", 0.0)
    row.setdefault("avg_zone_distance_inches", 0.0)
    row.setdefault("missed_batting_xwpa", 0.0)
    row.setdefault("missed_fielding_xwpa", 0.0)


def merge_team_missed_rows(team_rows: list[dict[str, Any]], missed_team_rows: list[dict[str, Any]]) -> None:
    lookup = {int(row["challenge_team_id"]): row for row in missed_team_rows}
    for row in team_rows:
        missed = lookup.get(int(row["challenge_team_id"]), {})
        row.update({k: v for k, v in missed.items() if k not in {"challenge_team_id", "challenge_team_abbr", "challenge_team_name"}})
        add_missed_defaults(row)
        row["decision_xwpa"] = float(row.get("total_xwpa", 0.0)) - float(row.get("missed_xwpa", 0.0))
        row["decision_risk_adjusted_xwpa"] = float(row.get("risk_adjusted_xwpa", 0.0)) - float(row.get("missed_xwpa", 0.0))
        row["decision_batting_xwpa"] = float(row.get("batting_xwpa", 0.0)) - float(row.get("missed_batting_xwpa", 0.0))
        row["decision_fielding_xwpa"] = float(row.get("fielding_xwpa", 0.0)) - float(row.get("missed_fielding_xwpa", 0.0))
        decision_denominator = int(row.get("attempts", 0)) + int(row.get("missed_opportunities", 0))
        row["decision_xwpa_per_opportunity"] = row["decision_xwpa"] / decision_denominator if decision_denominator else 0.0


def merge_player_missed_rows(player_rows: list[dict[str, Any]], missed_player_rows: list[dict[str, Any]]) -> None:
    lookup = {
        (str(row.get("role", "")), str(row.get("player_id", "")), str(row.get("challenge_team_abbr", ""))): row
        for row in missed_player_rows
    }
    for row in player_rows:
        key = (str(row.get("role", "")), str(row.get("player_id", "")), str(row.get("challenge_team_abbr", "")))
        missed = lookup.get(key, {})
        row.update({k: v for k, v in missed.items() if k not in {"role", "player_id", "player_name", "challenge_team_abbr"}})
        add_missed_defaults(row)
        row["decision_xwpa"] = float(row.get("total_xwpa", 0.0)) - float(row.get("missed_xwpa", 0.0))


def build_player_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    player_rows: list[dict[str, Any]] = []
    for row in rows:
        side = row["challenge_side"]
        if side == "batting":
            player_rows.append(player_role_row(row, "hitter", row.get("player_at_bat"), row.get("batter_name_flipped") or row.get("batter_name")))
        else:
            player_rows.append(player_role_row(row, "catcher", row.get("fielder_2"), row.get("catcher_name_flipped") or row.get("catcher_name")))
            player_rows.append(player_role_row(row, "pitcher", row.get("pitcher"), row.get("pitcher_name_flipped") or row.get("pitcher_name")))
            player_rows.append(player_role_row(row, "fielder_challenger", row.get("challenging_player_id"), row.get("challenger_name")))
    return player_rows


def player_role_row(row: dict[str, Any], role: str, player_id: Any, player_name: Any) -> dict[str, Any]:
    out = dict(row)
    out["role"] = role
    out["player_id"] = int_or_none(player_id) or ""
    out["player_name"] = flip_name(str(player_name or ""))
    return out


def build_failed_against_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Credit catchers/pitchers when an opposing hitter burns a challenge."""
    player_rows: list[dict[str, Any]] = []
    for row in rows:
        if row["challenge_side"] != "batting":
            continue
        player_rows.append(failed_against_role_row(row, "catcher_against", row.get("fielder_2"), row.get("catcher_name_flipped") or row.get("catcher_name")))
        player_rows.append(failed_against_role_row(row, "pitcher_against", row.get("pitcher"), row.get("pitcher_name_flipped") or row.get("pitcher_name")))
    return player_rows


def failed_against_role_row(row: dict[str, Any], role: str, player_id: Any, player_name: Any) -> dict[str, Any]:
    failed = int(row.get("is_challengeABS_overturned", 0)) == 0
    out = {
        "role": role,
        "player_id": int_or_none(player_id) or "",
        "player_name": flip_name(str(player_name or "")),
        "team_id": int_or_none(row.get("fld_team_id")) or "",
        "team_abbr": row.get("fld_team_abbr") or "",
        "game_pk": row.get("game_pk"),
        "play_id": row.get("play_id"),
        "game_date": row.get("game_date"),
        "opponent_team_abbr": row.get("bat_team_abbr") or "",
        "opponent_hitter_id": int_or_none(row.get("player_at_bat")) or "",
        "opponent_hitter_name": flip_name(str(row.get("batter_name_flipped") or row.get("batter_name") or "")),
        "is_failed_challenge_against": int(failed),
        "is_overturned_against": int(not failed),
        "fooled_xwpa": -float(row.get("option_wpa_proxy", 0.0)) if failed else 0.0,
        "failed_against_wpa_at_stake": float(row.get("wpa_if_overturned", 0.0)) if failed else 0.0,
        "opponent_success_xwpa": float(row.get("total_xwpa", 0.0)) if not failed else 0.0,
        "failed_strikeout_challenges_against": int(failed and int(row.get("strikes", 0)) == 2),
        "failed_reasonable_challenges_against": int(failed and int(row.get("is_challengeABS_reasonable_attempt", 0)) == 1),
    }
    return out


def aggregate_failed_against_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    group_fields = ["role", "player_id", "player_name", "team_id", "team_abbr"]
    for row in rows:
        if not row.get("player_id"):
            continue
        grouped[tuple(row.get(field, "") for field in group_fields)].append(row)

    output = []
    for key, items in grouped.items():
        challenges_against = len(items)
        failed = sum(int(item["is_failed_challenge_against"]) for item in items)
        row = {field: key[idx] for idx, field in enumerate(group_fields)}
        row.update(
            {
                "challenges_against": challenges_against,
                "failed_challenges_against": failed,
                "overturned_challenges_against": challenges_against - failed,
                "failed_challenges_against_rate": failed / challenges_against if challenges_against else 0.0,
                "fooled_xwpa": sum(float(item["fooled_xwpa"]) for item in items),
                "fooled_xwpa_per_challenge_against": sum(float(item["fooled_xwpa"]) for item in items) / challenges_against if challenges_against else 0.0,
                "failed_against_wpa_at_stake": sum(float(item["failed_against_wpa_at_stake"]) for item in items),
                "opponent_success_xwpa": sum(float(item["opponent_success_xwpa"]) for item in items),
                "failed_strikeout_challenges_against": sum(int(item["failed_strikeout_challenges_against"]) for item in items),
                "failed_reasonable_challenges_against": sum(int(item["failed_reasonable_challenges_against"]) for item in items),
            }
        )
        output.append(row)

    output.sort(key=lambda row: (row["fooled_xwpa"], row["failed_challenges_against"], row["failed_challenges_against_rate"]), reverse=True)
    return output


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def mirror_processed_data_to_site() -> None:
    out_dir = SITE / "data" / "processed"
    out_dir.mkdir(parents=True, exist_ok=True)
    for path in PROCESSED.iterdir():
        if path.is_file() and path.suffix in {".csv", ".json"}:
            (out_dir / path.name).write_bytes(path.read_bytes())


def round_for_json(row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, float):
            out[key] = round(value, 6)
        else:
            out[key] = value
    return out


def pct(value: float | None) -> str:
    if value is None:
        return ""
    return f"{100 * value:.1f}%"


def signed_pct(value: float) -> str:
    return f"{value * 100:+.2f}"


def signed_wins(value: float) -> str:
    return f"{value:+.3f}"


def signed_wpa_points(value: float) -> str:
    return f"{value * 100:+.1f}"


def article_team_phrase(row: dict[str, Any]) -> str:
    if not row:
        return "the current leader"
    name = html.escape(str(row.get("challenge_team_name") or row.get("challenge_team_abbr") or "the leader"))
    abbr = html.escape(str(row.get("challenge_team_abbr") or ""))
    return f"{name} ({abbr})" if abbr else name


def article_player_phrase(row: dict[str, Any]) -> str:
    if not row:
        return "the current leader"
    name = html.escape(str(row.get("player_name") or "the current leader"))
    team = html.escape(str(row.get("challenge_team_abbr") or row.get("team_abbr") or ""))
    return f"{name}, {team}" if team else name


def challenge_phrase(row: dict[str, Any]) -> str:
    if not row:
        return "No challenge swings are available yet."
    date_text = html.escape(str(row.get("game_date", ""))[:10])
    team = html.escape(str(row.get("challenge_team_abbr", "")))
    challenger = html.escape(str(row.get("challenger_name") or "the challenger"))
    half = html.escape(str(row.get("half", "")))
    inning = html.escape(str(row.get("inning", "")))
    base_state = html.escape(str(row.get("base_state", "")))
    count = f"{int(float_or_zero(row.get('balls')))}-{int(float_or_zero(row.get('strikes')))}"
    call = f"{html.escape(str(row.get('original_call', '')))} to {html.escape(str(row.get('actual_call', '')))}"
    return (
        f"{date_text}: {challenger} ({team}) turned a {half} {inning}, {count} count "
        f"with {base_state} into a {call} challenge worth {signed_wpa_points(float(row.get('total_xwpa', 0.0)))} WPA points."
    )


def missed_phrase(row: dict[str, Any]) -> str:
    if not row:
        return "No missed challenge opportunities cleared the model threshold."
    date_text = html.escape(str(row.get("game_date", ""))[:10])
    team = html.escape(str(row.get("challenge_team_abbr", "")))
    player = html.escape(str(row.get("player_name") or "the player"))
    half = html.escape(str(row.get("half", "")))
    inning = html.escape(str(row.get("inning", "")))
    base_state = html.escape(str(row.get("base_state", "")))
    count = f"{int(float_or_zero(row.get('balls')))}-{int(float_or_zero(row.get('strikes')))}"
    original = html.escape(str(row.get("original_call", "")))
    corrected = html.escape(str(row.get("corrected_call", "")))
    distance = float(row.get("zone_distance_inches", 0.0))
    return (
        f"{date_text}: {player} ({team}) passed on a {half} {inning}, {count} {original.lower()} "
        f"with {base_state}; the model had it {distance:.1f} inches over the inferred ABS edge "
        f"and worth {signed_wpa_points(float(row.get('missed_expected_xwpa', 0.0)))} expected points if challenged to {corrected.lower()}."
    )


def normalize_adsense_client(value: str | None) -> str:
    cleaned = (value or "").strip()
    if not cleaned:
        return ""
    if re.fullmatch(r"pub-\d{16}", cleaned):
        return f"ca-{cleaned}"
    if re.fullmatch(r"ca-pub-\d{16}", cleaned):
        return cleaned
    raise ValueError("AdSense client must look like ca-pub-0000000000000000 or pub-0000000000000000.")


def adsense_publisher_id(value: str | None) -> str:
    client = normalize_adsense_client(value)
    return client.removeprefix("ca-") if client else ""


def render_adsense_banner(client: str | None, slot: str | None) -> str:
    ad_client = normalize_adsense_client(client)
    ad_slot = (slot or "").strip()
    if not ad_client or not ad_slot:
        return ""
    safe_client = html.escape(ad_client, quote=True)
    safe_slot = html.escape(ad_slot, quote=True)
    return f"""
    <aside class="ad-banner" aria-label="Advertisement">
      <div class="ad-label">Advertisement</div>
      <script async src="https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js?client={safe_client}" crossorigin="anonymous"></script>
      <ins class="adsbygoogle"
           style="display:block"
           data-ad-client="{safe_client}"
           data-ad-slot="{safe_slot}"
           data-ad-format="horizontal"
           data-full-width-responsive="true"></ins>
      <script>(adsbygoogle = window.adsbygoogle || []).push({{}});</script>
    </aside>"""


def render_article_page(
    team_rows: list[dict[str, Any]],
    player_rows: list[dict[str, Any]],
    failed_against_rows: list[dict[str, Any]],
    challenge_rows: list[dict[str, Any]],
    missed_rows: list[dict[str, Any]],
    year: int,
    end_date: str,
    adsense_client: str | None = None,
    adsense_slot: str | None = None,
) -> str:
    teams_by_total = sorted(team_rows, key=lambda row: float(row.get("total_xwpa", 0.0)), reverse=True)
    teams_by_risk = sorted(team_rows, key=lambda row: float(row.get("risk_adjusted_xwpa", 0.0)), reverse=True)
    challenges_by_swing = sorted(challenge_rows, key=lambda row: abs(float(row.get("total_xwpa", 0.0))), reverse=True)
    missed_by_value = sorted(missed_rows, key=lambda row: float(row.get("missed_expected_xwpa", 0.0)), reverse=True)
    hitter_leaders = sorted(
        [row for row in player_rows if row.get("role") == "hitter"],
        key=lambda row: float(row.get("total_xwpa", 0.0)),
        reverse=True,
    )
    catcher_leaders = sorted(
        [row for row in player_rows if row.get("role") == "catcher"],
        key=lambda row: float(row.get("total_xwpa", 0.0)),
        reverse=True,
    )
    failed_catchers = sorted(
        [row for row in failed_against_rows if row.get("role") == "catcher_against"],
        key=lambda row: float(row.get("fooled_xwpa", 0.0)),
        reverse=True,
    )
    failed_pitchers = sorted(
        [row for row in failed_against_rows if row.get("role") == "pitcher_against"],
        key=lambda row: float(row.get("fooled_xwpa", 0.0)),
        reverse=True,
    )

    total_attempts = sum(int(row.get("attempts", 0)) for row in team_rows)
    total_overturns = sum(int(row.get("overturns", 0)) for row in team_rows)
    league_xwpa = sum(float(row.get("total_xwpa", 0.0)) for row in team_rows)
    league_risk = sum(float(row.get("risk_adjusted_xwpa", 0.0)) for row in team_rows)
    overturn_rate = total_overturns / total_attempts if total_attempts else 0.0
    leader = teams_by_total[0] if teams_by_total else {}
    risk_leader = teams_by_risk[0] if teams_by_risk else {}
    top_swing = challenges_by_swing[0] if challenges_by_swing else {}
    top_missed = missed_by_value[0] if missed_by_value else {}
    updated = date.today().isoformat()

    team_json = json.dumps([round_for_json(row) for row in team_rows], ensure_ascii=False)
    player_json = json.dumps([round_for_json(row) for row in player_rows], ensure_ascii=False)
    failed_against_json = json.dumps([round_for_json(row) for row in failed_against_rows], ensure_ascii=False)
    article_challenges = [
        {
            "game_date": row.get("game_date", ""),
            "challenge_team_abbr": row.get("challenge_team_abbr", ""),
            "challenger_name": row.get("challenger_name", ""),
            "challenge_side": row.get("challenge_side", ""),
            "half": row.get("half", ""),
            "inning": row.get("inning", ""),
            "base_state": row.get("base_state", ""),
            "balls": int(float_or_zero(row.get("balls"))),
            "strikes": int(float_or_zero(row.get("strikes"))),
            "original_call": row.get("original_call", ""),
            "actual_call": row.get("actual_call", ""),
            "total_xwpa": round(float(row.get("total_xwpa", 0.0)), 6),
        }
        for row in challenges_by_swing
    ]
    challenge_json = json.dumps(article_challenges, ensure_ascii=False)
    article_missed = [
        {
            "game_date": row.get("game_date", ""),
            "challenge_team_abbr": row.get("challenge_team_abbr", ""),
            "challenge_team_name": row.get("challenge_team_name", ""),
            "player_name": row.get("player_name", ""),
            "role": row.get("role", ""),
            "challenge_side": row.get("challenge_side", ""),
            "half": row.get("half", ""),
            "inning": row.get("inning", ""),
            "base_state": row.get("base_state", ""),
            "balls": int(float_or_zero(row.get("balls"))),
            "strikes": int(float_or_zero(row.get("strikes"))),
            "original_call": row.get("original_call", ""),
            "corrected_call": row.get("corrected_call", ""),
            "zone_distance_inches": round(float(row.get("zone_distance_inches", 0.0)), 2),
            "overturn_probability": round(float(row.get("overturn_probability", 0.0)), 4),
            "remaining_challenge_losses_before": int(float_or_zero(row.get("remaining_challenge_losses_before"))),
            "wpa_if_overturned": round(float(row.get("wpa_if_overturned", 0.0)), 6),
            "missed_expected_xwpa": round(float(row.get("missed_expected_xwpa", 0.0)), 6),
        }
        for row in missed_by_value
    ]
    missed_json = json.dumps(article_missed, ensure_ascii=False)
    adsense_banner = render_adsense_banner(adsense_client, adsense_slot)

    template = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>The ABS Challenge Leaderboard We Actually Wanted</title>
  <style>
    :root {
      --paper: #f4efe4;
      --paper-deep: #e5d8c4;
      --ink: #111827;
      --muted: #62707b;
      --line: #cbbda5;
      --panel: #fff9eb;
      --navy: #192f45;
      --green: #176a4d;
      --red: #a3362b;
      --gold: #b37b24;
      --blue: #2b667e;
      --shadow: 0 18px 50px rgba(17, 24, 39, .14);
    }
    * { box-sizing: border-box; }
    html { scroll-behavior: smooth; }
    body {
      margin: 0;
      color: var(--ink);
      background:
        radial-gradient(circle at 14% 12%, rgba(179, 123, 36, .18), transparent 28%),
        linear-gradient(90deg, rgba(25,47,69,.08) 1px, transparent 1px) 0 0 / 30px 30px,
        linear-gradient(0deg, rgba(25,47,69,.06) 1px, transparent 1px) 0 0 / 30px 30px,
        var(--paper);
      font-family: Charter, "Iowan Old Style", Georgia, "Times New Roman", serif;
    }
    a { color: var(--blue); text-decoration-thickness: 1px; text-underline-offset: 3px; }
    .shell { max-width: 1180px; margin: 0 auto; padding: 24px 20px 56px; }
    .masthead {
      min-height: 92vh;
      display: grid;
      grid-template-rows: auto 1fr auto;
      border-bottom: 3px solid var(--ink);
      position: relative;
      overflow: hidden;
    }
    .masthead::after {
      content: "";
      position: absolute;
      inset: 18% -8% auto auto;
      width: min(560px, 48vw);
      aspect-ratio: 1;
      border: 2px solid rgba(25,47,69,.18);
      border-radius: 999px;
      background:
        linear-gradient(90deg, transparent 47%, rgba(163,54,43,.58) 48% 52%, transparent 53%),
        repeating-radial-gradient(circle, rgba(17,24,39,.05) 0 2px, transparent 3px 20px);
      transform: rotate(-18deg);
      pointer-events: none;
    }
    .topbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 18px 0;
      font: 700 12px/1.2 ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      letter-spacing: .08em;
      text-transform: uppercase;
      position: relative;
      z-index: 1;
    }
    .topbar nav { display: flex; gap: 14px; flex-wrap: wrap; }
    .topbar a { color: var(--ink); text-decoration: none; border-bottom: 1px solid currentColor; }
    .hero-copy {
      align-self: center;
      max-width: 930px;
      padding: 42px 0 34px;
      position: relative;
      z-index: 1;
    }
    .rubric {
      display: inline-flex;
      align-items: center;
      gap: 10px;
      margin-bottom: 18px;
      font: 800 12px/1 ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      letter-spacing: .1em;
      text-transform: uppercase;
      color: var(--red);
    }
    .rubric::before { content: ""; width: 42px; height: 2px; background: var(--red); }
    h1 {
      max-width: 980px;
      margin: 0;
      font-size: clamp(48px, 9vw, 126px);
      line-height: .84;
      letter-spacing: 0;
    }
    .dek {
      max-width: 760px;
      margin: 28px 0 0;
      color: #394852;
      font-size: clamp(20px, 2.3vw, 30px);
      line-height: 1.18;
    }
    .byline {
      display: flex;
      align-items: center;
      gap: 12px;
      margin-top: 26px;
      color: var(--muted);
      font: 14px/1.35 ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    .stamp {
      border: 2px solid var(--ink);
      background: rgba(255,249,235,.9);
      padding: 8px 10px;
      color: var(--ink);
      font-weight: 800;
      transform: rotate(-1.5deg);
    }
    .hero-strip {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 1px;
      background: var(--ink);
      border: 2px solid var(--ink);
      margin-bottom: 22px;
      position: relative;
      z-index: 1;
    }
    .hero-stat { background: var(--panel); padding: 16px; min-height: 112px; }
    .hero-stat span {
      display: block;
      color: var(--muted);
      font: 800 11px/1.1 ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      letter-spacing: .08em;
      text-transform: uppercase;
    }
    .hero-stat strong { display: block; margin-top: 8px; font-size: clamp(28px, 4vw, 46px); line-height: .95; }
    .ad-banner {
      max-width: 970px;
      margin: 18px auto 0;
      padding: 8px 0 10px;
      border-top: 1px solid rgba(17,24,39,.18);
      border-bottom: 1px solid rgba(17,24,39,.18);
      text-align: center;
      color: var(--muted);
      background: rgba(255,249,235,.42);
      overflow: hidden;
    }
    .ad-label {
      margin-bottom: 6px;
      font: 800 10px/1 ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      letter-spacing: .08em;
      text-transform: uppercase;
    }
    .story-grid {
      display: grid;
      grid-template-columns: minmax(0, 720px) minmax(280px, 1fr);
      gap: 44px;
      align-items: start;
      margin-top: 42px;
    }
    .story-copy {
      font-size: 19px;
      line-height: 1.62;
    }
    .story-copy p { margin: 0 0 22px; }
    .story-copy h2 {
      margin: 44px 0 14px;
      font-size: clamp(30px, 4vw, 52px);
      line-height: .98;
      letter-spacing: 0;
    }
    .pull {
      border-left: 5px solid var(--red);
      padding: 6px 0 6px 18px;
      margin: 30px 0;
      color: var(--navy);
      font-size: 26px;
      line-height: 1.18;
      font-weight: 800;
    }
    .sidebar {
      position: sticky;
      top: 18px;
      display: grid;
      gap: 14px;
    }
    .scorecard, .viz, .note-box {
      background: rgba(255,249,235,.92);
      border: 1px solid var(--line);
      box-shadow: var(--shadow);
    }
    .scorecard { padding: 18px; border-top: 7px solid var(--navy); }
    .scorecard h2, .viz h2, .note-box h2 {
      margin: 0;
      font-size: 19px;
      line-height: 1.1;
    }
    .scorecard-list { margin-top: 14px; display: grid; gap: 12px; }
    .scorecard-row {
      display: grid;
      grid-template-columns: 34px minmax(0, 1fr) auto;
      align-items: center;
      gap: 10px;
      border-top: 1px solid rgba(203,189,165,.72);
      padding-top: 12px;
      font: 14px/1.25 ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    .team-logo { width: 34px; height: 34px; object-fit: contain; }
    .team-name { font-weight: 850; }
    .team-sub { color: var(--muted); font-size: 12px; }
    .value-pos { color: var(--green); font-weight: 850; }
    .value-neg { color: var(--red); font-weight: 850; }
    .viz {
      margin: 42px 0;
      padding: 20px;
      overflow: hidden;
    }
    .viz-head {
      display: flex;
      justify-content: space-between;
      gap: 18px;
      align-items: start;
      margin-bottom: 16px;
    }
    .viz-kicker {
      color: var(--red);
      font: 800 11px/1 ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      letter-spacing: .09em;
      text-transform: uppercase;
      margin-bottom: 8px;
    }
    .button-row { display: flex; flex-wrap: wrap; gap: 8px; justify-content: flex-end; }
    button, select, input {
      appearance: none;
      border: 1px solid var(--ink);
      background: var(--panel);
      color: var(--ink);
      min-height: 36px;
      padding: 8px 11px;
      font: 800 12px/1 ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      letter-spacing: .02em;
      cursor: pointer;
    }
    input {
      cursor: text;
      min-width: min(100%, 300px);
      text-transform: none;
      letter-spacing: 0;
      font-weight: 700;
    }
    input::placeholder { color: #78848d; }
    button.active { background: var(--ink); color: white; }
    button:disabled { cursor: not-allowed; opacity: .45; }
    .board-tools {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin: -4px 0 16px;
      flex-wrap: wrap;
    }
    .typeahead { position: relative; flex: 1 1 280px; max-width: 420px; }
    .typeahead input { width: 100%; }
    .suggestions {
      display: none;
      position: absolute;
      z-index: 12;
      top: calc(100% + 5px);
      left: 0;
      right: 0;
      max-height: 240px;
      overflow-y: auto;
      border: 1px solid var(--ink);
      background: var(--panel);
      box-shadow: 0 12px 30px rgba(17,24,39,.2);
      font: 13px/1.25 ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    .suggestions.open { display: block; }
    .suggestion {
      width: 100%;
      border: 0;
      border-bottom: 1px solid rgba(203,189,165,.7);
      background: transparent;
      text-align: left;
      min-height: 0;
      padding: 10px 11px;
      font-weight: 800;
      cursor: pointer;
    }
    .suggestion:hover, .suggestion:focus { background: rgba(43,102,126,.12); outline: none; }
    .suggestion small { display: block; margin-top: 3px; color: var(--muted); font-weight: 700; }
    .board-actions { display: flex; align-items: center; justify-content: flex-end; gap: 8px; flex-wrap: wrap; }
    .toggle-row {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      border: 1px solid rgba(17,24,39,.45);
      background: rgba(255,249,235,.72);
      padding: 9px 10px;
      min-height: 38px;
      font: 800 11px/1.1 ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      text-transform: uppercase;
    }
    .toggle-row input {
      min-width: 0;
      width: 16px;
      height: 16px;
      accent-color: var(--green);
    }
    .leaderboard-count {
      color: var(--muted);
      font: 800 11px/1 ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      letter-spacing: 0;
      text-transform: uppercase;
      white-space: nowrap;
    }
    .empty-state {
      border: 1px dashed rgba(17,24,39,.35);
      background: rgba(255,255,255,.36);
      color: var(--muted);
      padding: 18px;
      font: 13px/1.4 ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    .bar-chart { display: grid; gap: 8px; }
    .bar-row {
      display: grid;
      grid-template-columns: 126px minmax(0, 1fr) 82px;
      gap: 10px;
      align-items: center;
      min-height: 36px;
      font: 13px/1 ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    .bar-team { display: flex; align-items: center; gap: 8px; font-weight: 850; min-width: 0; }
    .mini-logo { width: 24px; height: 24px; object-fit: contain; flex: 0 0 24px; }
    .track { height: 16px; background: rgba(17,24,39,.09); border: 1px solid rgba(17,24,39,.16); position: relative; overflow: hidden; }
    .bar-fill { height: 100%; width: 0; background: var(--green); transition: width .35s ease; }
    .bar-fill.neg { background: var(--red); }
    .bar-value { text-align: right; font-weight: 850; }
    .split-layout {
      display: grid;
      grid-template-columns: minmax(0, 1.25fr) minmax(320px, .75fr);
      gap: 20px;
      align-items: stretch;
    }
    .scatter-wrap {
      min-height: 410px;
      position: relative;
      border: 1px solid rgba(203,189,165,.8);
      background:
        linear-gradient(90deg, rgba(25,47,69,.06) 1px, transparent 1px) 0 0 / 44px 44px,
        linear-gradient(0deg, rgba(25,47,69,.05) 1px, transparent 1px) 0 0 / 44px 44px,
        rgba(255,255,255,.34);
    }
    svg { width: 100%; height: 410px; display: block; }
    .tooltip {
      position: fixed;
      z-index: 9;
      max-width: 260px;
      pointer-events: none;
      background: var(--ink);
      color: white;
      padding: 10px 11px;
      font: 12px/1.35 ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      opacity: 0;
      transform: translate(-50%, calc(-100% - 12px));
      transition: opacity .12s ease;
    }
    .case-file {
      border: 1px solid var(--ink);
      background: var(--paper-deep);
      padding: 16px;
      min-height: 410px;
    }
    .case-top {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 16px;
    }
    .case-logo { width: 72px; height: 72px; object-fit: contain; }
    .case-title { font-size: 30px; line-height: .95; margin: 0; }
    .case-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; }
    .case-stat {
      background: rgba(255,249,235,.74);
      border: 1px solid rgba(17,24,39,.18);
      padding: 10px;
      min-height: 78px;
    }
    .case-stat span {
      display: block;
      color: var(--muted);
      font: 800 10px/1.1 ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      letter-spacing: .07em;
      text-transform: uppercase;
    }
    .case-stat strong { display: block; margin-top: 7px; font: 900 22px/1 ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
    .player-board {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }
    .player-row, .swing-card {
      background: rgba(255,255,255,.42);
      border: 1px solid rgba(203,189,165,.9);
      padding: 12px;
    }
    .player-row {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 12px;
      align-items: center;
      font: 14px/1.25 ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    .player-row strong { display: block; }
    .player-row span { color: var(--muted); font-size: 12px; }
    .swing-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 12px;
    }
    .swing-card {
      min-height: 190px;
      display: grid;
      grid-template-rows: auto 1fr auto;
    }
    .swing-meta {
      display: flex;
      justify-content: space-between;
      gap: 8px;
      color: var(--muted);
      font: 800 11px/1.1 ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      letter-spacing: .06em;
      text-transform: uppercase;
    }
    .swing-card h3 { margin: 14px 0; font-size: 24px; line-height: 1; }
    .swing-card p {
      margin: 0;
      color: #394852;
      font: 13px/1.35 ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    .methodology {
      display: grid;
      grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
      gap: 18px;
      margin-top: 46px;
      border-top: 3px solid var(--ink);
      padding-top: 24px;
      font: 14px/1.55 ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: #34434d;
    }
    .methodology h2 { margin: 0 0 10px; font: 900 22px/1 Charter, "Iowan Old Style", Georgia, serif; color: var(--ink); }
    .methodology ul { margin: 0; padding-left: 18px; }
    .methodology li { margin-bottom: 8px; }
    footer {
      margin-top: 40px;
      padding-top: 18px;
      border-top: 1px solid var(--line);
      color: var(--muted);
      font: 13px/1.5 ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    .codex-note { margin-top: 8px; font-size: 12px; }
    @media (max-width: 920px) {
      .hero-strip, .story-grid, .split-layout, .methodology { grid-template-columns: 1fr; }
      .sidebar { position: static; }
      .player-board, .swing-grid { grid-template-columns: 1fr; }
      .masthead { min-height: auto; }
    }
    @media (max-width: 620px) {
      .shell { padding-inline: 14px; }
      .topbar { align-items: flex-start; flex-direction: column; }
      h1 { font-size: 52px; }
      .bar-row { grid-template-columns: 92px minmax(0, 1fr) 62px; }
      .hero-stat { min-height: 92px; }
      .case-grid { grid-template-columns: 1fr; }
      .viz-head { flex-direction: column; }
      .button-row { justify-content: flex-start; }
      .board-tools { align-items: stretch; }
      .board-actions { justify-content: flex-start; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <header class="masthead">
      <div class="topbar">
        <div>ABS Challenge Lab / __YEAR__</div>
        <nav>
          <a href="dashboard.html">Data Dashboard</a>
          <a href="data/processed/team_abs_xwpa.csv">Team CSV</a>
          <a href="data/processed/missed_challenge_opportunities.csv">Missed CSV</a>
          <a href="data/processed/player_failed_challenges_against.csv">Failed Against CSV</a>
        </nav>
      </div>
      <div class="hero-copy">
        <div class="rubric">Win Probability, Not Just Accuracy</div>
        <h1>The ABS Challenge Leaderboard We Actually Wanted</h1>
        <p class="dek">Overturn rate tells you who was right. xWPA asks whether being right mattered.</p>
        <div class="byline">
          <span class="stamp">Updated __UPDATED__</span>
          <span>Regular season ABS challenges through __END_DATE__.</span>
        </div>
      </div>
      <div class="hero-strip">
        <div class="hero-stat"><span>Challenges Studied</span><strong>__TOTAL_ATTEMPTS__</strong></div>
        <div class="hero-stat"><span>Overturn Rate</span><strong>__OVERTURN_RATE__</strong></div>
        <div class="hero-stat"><span>League Direct xWPA</span><strong>__LEAGUE_XWPA_POINTS__</strong></div>
        <div class="hero-stat"><span>Top Club</span><strong>__LEADER_ABBR__</strong></div>
      </div>
      __ADSENSE_BANNER__
    </header>

    <div class="story-grid">
      <article class="story-copy">
        <p>There are two stories inside every ABS challenge. The first is the one the broadcast can resolve instantly: did the strike-zone graphic move the call from red to green? The second is slower and much more interesting: what did that correction do to the game?</p>

        <p>The public leaderboard that inspired this project is a useful start. Overturn rate is tidy, and overturn rate above expectation adds needed context. But a sixth-inning 0-0 miss and a ninth-inning full-count pitch with the bases loaded should not live in the same accounting bucket. A challenge is a tiny replay review attached to a huge amount of game state.</p>

        <p>So this version scores every challenge by expected win probability added. For each pitch, the script reconstructs the inning, half-inning, score, base/out state, count, batting team, and fielding team from MLB Stats API game feeds. It then asks a simple counterfactual question: what was the challenging team's win probability after the original umpire call, and what was it after the ABS-corrected call?</p>

        <div class="pull">Through __END_DATE__, the top team is __LEADER_PHRASE__ at __LEADER_WINS__ direct challenge wins, or __LEADER_POINTS__ WPA points.</div>

        <p>The early leaderboard has a pleasingly weird shape. The first-place club is __LEADER_PHRASE__, but the story is not just volume. The leader's challenges have produced __LEADER_XWPA_PER__ points per attempt, and the risk-adjusted leader is __RISK_LEADER_PHRASE__ once the failed-challenge inventory proxy is included. The league as a whole has banked __LEAGUE_XWPA_WINS__ direct wins of call-correction value before accounting for the cost of unsuccessful challenges.</p>

        <p>The single largest swing in the file is the kind of pitch that makes this exercise feel necessary. __TOP_SWING_PHRASE__ That one pitch carries more scoreboard weight than a dozen low-leverage confirms.</p>
      </article>

      <aside class="sidebar">
        <div class="scorecard">
          <h2>Quick Top Five</h2>
          <div class="scorecard-list" id="sideTopTeams"></div>
        </div>
        <div class="note-box scorecard">
          <h2>Player Notes</h2>
          <div class="scorecard-list">
            <div class="scorecard-row"><span></span><div><div class="team-name">__TOP_HITTER__</div><div class="team-sub">Hitter challenge xWPA leader</div></div><div class="value-pos">__TOP_HITTER_WPA__</div></div>
            <div class="scorecard-row"><span></span><div><div class="team-name">__TOP_CATCHER__</div><div class="team-sub">Catcher challenge xWPA leader</div></div><div class="value-pos">__TOP_CATCHER_WPA__</div></div>
            <div class="scorecard-row"><span></span><div><div class="team-name">__TOP_FAILED_CATCHER__</div><div class="team-sub">Failed hitter challenges against</div></div><div class="value-pos">__TOP_FAILED_CATCHER_WPA__</div></div>
          </div>
        </div>
      </aside>
    </div>

    <section class="viz" aria-labelledby="teamVizTitle">
      <div class="viz-head">
        <div>
          <div class="viz-kicker">Leaderboard</div>
          <h2 id="teamVizTitle">The Team Board Changes When the Stakes Change</h2>
        </div>
        <div class="button-row" id="teamMetricButtons">
          <button class="active" data-team-metric="total_xwpa">Direct xWPA</button>
          <button data-team-metric="risk_adjusted_xwpa">Risk Adj.</button>
          <button data-team-metric="fielding_xwpa">Fielding</button>
          <button data-team-metric="batting_xwpa">Batting</button>
          <button data-team-metric="xwpa_per_challenge">Per Challenge</button>
        </div>
      </div>
      <div class="board-tools">
        <div class="typeahead">
          <input id="teamBoardSearch" placeholder="Search teams" autocomplete="off">
          <div class="suggestions" id="teamBoardSuggestions"></div>
        </div>
        <div class="board-actions">
          <span class="leaderboard-count" id="teamBoardCount"></span>
          <label class="toggle-row"><input id="teamIncludeMissed" type="checkbox">Include Missed</label>
          <button id="teamShowMore">Show 15 More</button>
          <button id="teamShowAll">Show All</button>
          <button id="teamReset">Reset</button>
        </div>
      </div>
      <div class="bar-chart" id="teamBars"></div>
    </section>

    <article class="story-copy">
      <h2>Separating the Call From the Decision</h2>
      <p>The cleanest number here is direct xWPA. It gives full credit when ABS flips the call and zero direct credit when the call stands. That keeps the metric tied to the thing that actually changed on the field.</p>

      <p>The risk-adjusted column is a second lens, not a replacement. Failed challenges matter because teams can run out of them, so the script converts Savant's lost-challenge run value into wins using a 10-runs-per-win rule of thumb. That is a practical proxy for the hidden option value of keeping a challenge available. It is intentionally labeled as a proxy because MLB has not published an official challenge-inventory win model.</p>

      <h2>The Challenge That Never Came</h2>
      <p>There is now a third bucket: missed opportunities. These are not observed decisions, so they should be handled with more humility. The model scans every called ball and called strike in the MLB game feed, compares the pitch location to an inferred ABS rectangle, estimates the chance that a challenge would have flipped the call from the pitch's distance beyond the edge, then weighs that against the game's win-probability leverage and the team's remaining challenge inventory.</p>

      <p>Through __END_DATE__, the model finds __MISSED_COUNT__ positive expected-value pitches that were not challenged, worth __MISSED_TOTAL_POINTS__ expected WPA points left on the table. The biggest is this one: __TOP_MISSED_PHRASE__</p>
    </article>

    <section class="viz">
      <div class="viz-head">
        <div>
          <div class="viz-kicker">Shape of the Season</div>
          <h2>Volume Helps, But Leverage Is the Cheat Code</h2>
        </div>
      </div>
      <div class="split-layout">
        <div class="scatter-wrap">
          <svg id="teamScatter" role="img" aria-label="Team attempts versus direct xWPA scatterplot"></svg>
          <div class="tooltip" id="tooltip"></div>
        </div>
        <div class="case-file">
          <div class="case-top">
            <div>
              <select id="teamSelect" aria-label="Team profile"></select>
              <h2 class="case-title" id="caseTitle"></h2>
            </div>
            <img class="case-logo" id="caseLogo" alt="">
          </div>
          <div class="case-grid" id="caseStats"></div>
        </div>
      </div>
    </section>

    <article class="story-copy">
      <h2>The Reverse Leaderboard</h2>
      <p>There is another skill hiding in the data. When hitters challenge a called strike and lose, the catcher and pitcher have helped turn conviction into a burned challenge. The stat here is called failed challenges against. It is credited only on hitter-initiated challenges that were confirmed, and its value column, fooled xWPA, gives positive credit for the opponent's lost-challenge proxy.</p>

      <p>On that board, __TOP_FAILED_CATCHER_PHRASE__ has been the early standout among catchers with __TOP_FAILED_CATCHER_COUNT__ failed hitter challenges against. Among pitchers, __TOP_FAILED_PITCHER_PHRASE__ leads at __TOP_FAILED_PITCHER_COUNT__. This is not a pure framing stat, and it is not meant to be one. It is closer to a game-theory stat: who is living in the hitter's head long enough to make the hitter spend?</p>
    </article>

    <section class="viz">
      <div class="viz-head">
        <div>
          <div class="viz-kicker">Estimated Misses</div>
          <h2>Positive-EV Challenges That Never Got Thrown</h2>
        </div>
        <div class="button-row" id="missedSideButtons">
          <button class="active" data-missed-side="all">All</button>
          <button data-missed-side="batting">Batting</button>
          <button data-missed-side="fielding">Fielding</button>
        </div>
      </div>
      <div class="board-tools">
        <div class="typeahead">
          <input id="missedSearch" placeholder="Search players or teams" autocomplete="off">
          <div class="suggestions" id="missedSuggestions"></div>
        </div>
        <div class="board-actions">
          <span class="leaderboard-count" id="missedCount"></span>
          <button id="missedShowMore">Show 6 More</button>
          <button id="missedShowAll">Show All</button>
          <button id="missedReset">Reset</button>
        </div>
      </div>
      <div class="swing-grid" id="missedGrid"></div>
    </section>

    <section class="viz">
      <div class="viz-head">
        <div>
          <div class="viz-kicker">Individual Boards</div>
          <h2>Who Creates, and Who Baits the Miss?</h2>
        </div>
        <div class="button-row" id="playerModeButtons">
          <button class="active" data-player-mode="hitter">Hitters</button>
          <button data-player-mode="catcher">Catcher Challenges</button>
          <button data-player-mode="pitcher">Pitcher Challenges</button>
          <button data-player-mode="catcher_against">Catchers: Failed Against</button>
          <button data-player-mode="pitcher_against">Pitchers: Failed Against</button>
        </div>
      </div>
      <div class="board-tools">
        <div class="typeahead">
          <input id="playerBoardSearch" placeholder="Search players" autocomplete="off">
          <div class="suggestions" id="playerBoardSuggestions"></div>
        </div>
        <div class="board-actions">
          <span class="leaderboard-count" id="playerBoardCount"></span>
          <label class="toggle-row"><input id="playerIncludeMissed" type="checkbox">Include Missed</label>
          <button id="playerShowMore">Show 12 More</button>
          <button id="playerShowAll">Show All</button>
          <button id="playerReset">Reset</button>
        </div>
      </div>
      <div class="player-board" id="playerBoard"></div>
    </section>

    <section class="viz">
      <div class="viz-head">
        <div>
          <div class="viz-kicker">The Plays</div>
          <h2>The Highest-Leverage Challenges So Far</h2>
        </div>
        <div class="button-row" id="swingButtons">
          <button class="active" data-swing-side="all">All</button>
          <button data-swing-side="batting">Batting</button>
          <button data-swing-side="fielding">Fielding</button>
        </div>
      </div>
      <div class="board-tools">
        <div class="typeahead">
          <input id="swingSearch" placeholder="Search players or teams" autocomplete="off">
          <div class="suggestions" id="swingSuggestions"></div>
        </div>
        <div class="board-actions">
          <span class="leaderboard-count" id="swingCount"></span>
          <button id="swingShowMore">Show 6 More</button>
          <button id="swingShowAll">Show All</button>
          <button id="swingReset">Reset</button>
        </div>
      </div>
      <div class="swing-grid" id="swingGrid"></div>
    </section>

    <section class="methodology">
      <div>
        <h2>How It Works</h2>
        <ul>
          <li>Challenge attempts come from Baseball Savant's ABS leaderboard and challenge drawer service.</li>
          <li>Each pitch is joined to MLB Stats API live game feeds by game and pitch id.</li>
          <li>The win-probability model is trained from completed __YEAR__ regular-season game feeds through __END_DATE__.</li>
          <li>For each challenge, the model evaluates the original call and the ABS-corrected call from the challenging team's perspective.</li>
          <li>Risk-adjusted xWPA adds a failed-challenge inventory proxy based on Savant's lost-challenge run value, converted at 10 runs per win.</li>
          <li>Missed opportunities are inferred from MLB game-feed pitch coordinates, a strike-zone rectangle expanded by a baseball radius, game leverage, modeled overturn probability from distance beyond the edge, and remaining challenge-loss inventory.</li>
        </ul>
      </div>
      <div>
        <h2>Caveats</h2>
        <ul>
          <li>This is an independent model, not an official MLB or Statcast metric.</li>
          <li>Early-season run distributions are noisy, especially in rare count/base/out states.</li>
          <li>The inventory penalty is a proxy. A full version would model challenge availability, inning rules, and future challenge quality inside each game.</li>
          <li>Player credit follows the challenge side: hitter for batting challenges; catcher, pitcher, and fielding challenger for fielding challenges.</li>
          <li>Missed-opportunity player credit is estimated only for hitters on missed batting challenges and pitchers on missed fielding challenges because the public game feed does not reliably expose the catcher on every historical pitch.</li>
        </ul>
        <p>Sources: <a href="https://www.mlb.com/news/how-to-know-who-is-good-at-using-abs-2026-mlb">MLB.com on ABS challenge skill</a>, <a href="https://baseballsavant.mlb.com/leaderboard/abs-challenges">Baseball Savant ABS leaderboard</a>, and MLB Stats API game feeds.</p>
      </div>
    </section>

    <footer>
      Built as a static page from the same daily pipeline that writes the CSVs. The interactive graphics use embedded data, so the page can be published as ordinary static HTML.
      <div class="codex-note">Generated with Codex; because of this, it may contain errors.</div>
    </footer>
  </div>

  <script>
    const teams = __TEAM_JSON__;
    const players = __PLAYER_JSON__;
    const failedAgainst = __FAILED_AGAINST_JSON__;
    const challenges = __CHALLENGE_JSON__;
    const missed = __MISSED_JSON__;

    const logo = id => `https://www.mlbstatic.com/team-logos/${id}.svg`;
    const wpaPts = v => `${v >= 0 ? '+' : ''}${(v * 100).toFixed(1)}`;
    const wpaWins = v => `${v >= 0 ? '+' : ''}${v.toFixed(3)}`;
    const pct = v => `${(v * 100).toFixed(1)}%`;
    const cls = v => v >= 0 ? 'value-pos' : 'value-neg';
    const fmtSide = side => side === 'fielding' ? 'Fielding' : side === 'batting' ? 'Batting' : side;
    const teamByAbbr = new Map(teams.map(row => [row.challenge_team_abbr, row]));
    const boardState = {
      team: { metric: 'total_xwpa', limit: 15, query: '' },
      player: { mode: 'hitter', limit: 12, query: '' },
      swing: { side: 'all', limit: 6, query: '' },
      missed: { side: 'all', limit: 6, query: '' },
      includeMissed: false
    };

    const normalize = value => String(value || '')
      .toLowerCase()
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '');
    const escapeHTML = value => String(value || '').replace(/[&<>"']/g, char => ({
      '&': '&amp;',
      '<': '&lt;',
      '>': '&gt;',
      '"': '&quot;',
      "'": '&#39;'
    }[char]));
    const matchesQuery = (text, query) => !query || normalize(text).includes(normalize(query));
    const limitedRows = (rows, limit) => limit === Infinity ? rows : rows.slice(0, limit);
    const displayLimit = value => value === Infinity ? 'all' : value;

    function uniqueBy(items, keyFn) {
      const seen = new Set();
      return items.filter(item => {
        const key = keyFn(item);
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });
    }

    function renderSuggestions(panel, options, query, onSelect) {
      const q = normalize(query).trim();
      if (!q) {
        panel.classList.remove('open');
        panel.innerHTML = '';
        return;
      }
      const rows = uniqueBy(options, option => `${option.value}|${option.detail || ''}`)
        .filter(option => normalize(`${option.label} ${option.value} ${option.detail || ''} ${option.search || ''}`).includes(q))
        .slice(0, 8);
      if (!rows.length) {
        panel.classList.remove('open');
        panel.innerHTML = '';
        return;
      }
      panel.innerHTML = rows.map((option, idx) => `
        <button class="suggestion" type="button" data-suggestion-index="${idx}">
          ${escapeHTML(option.label)}
          ${option.detail ? `<small>${escapeHTML(option.detail)}</small>` : ''}
        </button>
      `).join('');
      panel.classList.add('open');
      panel.querySelectorAll('[data-suggestion-index]').forEach(button => {
        button.addEventListener('click', () => {
          const option = rows[Number(button.dataset.suggestionIndex)];
          onSelect(option.value);
          panel.classList.remove('open');
          panel.innerHTML = '';
        });
      });
    }

    function setupTypeahead(inputId, panelId, getOptions, onQuery) {
      const input = document.querySelector(inputId);
      const panel = document.querySelector(panelId);
      input.addEventListener('input', () => {
        onQuery(input.value);
        renderSuggestions(panel, getOptions(), input.value, value => {
          input.value = value;
          onQuery(value);
        });
      });
      input.addEventListener('focus', () => renderSuggestions(panel, getOptions(), input.value, value => {
        input.value = value;
        onQuery(value);
      }));
      input.addEventListener('keydown', event => {
        if (event.key === 'Escape') {
          panel.classList.remove('open');
          panel.innerHTML = '';
        }
        if (event.key === 'Enter') {
          const first = panel.querySelector('.suggestion');
          if (first) {
            event.preventDefault();
            first.click();
          }
        }
      });
    }

    document.addEventListener('click', event => {
      document.querySelectorAll('.typeahead').forEach(box => {
        if (!box.contains(event.target)) {
          const panel = box.querySelector('.suggestions');
          panel.classList.remove('open');
          panel.innerHTML = '';
        }
      });
    });

    function teamSearchText(row) {
      return `${row.challenge_team_name} ${row.challenge_team_abbr}`;
    }

    function playerTeam(row, isAgainst = row.role && row.role.includes('_against')) {
      return isAgainst ? row.team_abbr : row.challenge_team_abbr;
    }

    function playerSearchText(row) {
      return `${row.player_name} ${playerTeam(row)} ${row.role}`;
    }

    function swingSearchText(row) {
      const team = teamByAbbr.get(row.challenge_team_abbr);
      return [
        row.challenger_name,
        row.challenge_team_abbr,
        team ? team.challenge_team_name : '',
        row.challenge_side,
        row.original_call,
        row.actual_call,
        row.base_state
      ].join(' ');
    }

    function missedSearchText(row) {
      return [
        row.player_name,
        row.challenge_team_abbr,
        row.challenge_team_name,
        row.challenge_side,
        row.role,
        row.original_call,
        row.corrected_call,
        row.base_state
      ].join(' ');
    }

    function setBoardCount(id, visible, total, limit) {
      document.querySelector(id).textContent = `${visible} of ${total} shown (${displayLimit(limit)})`;
    }

    function updateLimitButtons(prefix, visible, total, query, limit, defaultLimit) {
      document.querySelector(`#${prefix}ShowMore`).disabled = visible >= total;
      document.querySelector(`#${prefix}ShowAll`).disabled = visible >= total;
      document.querySelector(`#${prefix}Reset`).disabled = !query && limit === defaultLimit;
    }

    function renderSideTopTeams() {
      const top = [...teams].sort((a, b) => b.total_xwpa - a.total_xwpa).slice(0, 5);
      document.querySelector('#sideTopTeams').innerHTML = top.map((row, idx) => `
        <div class="scorecard-row">
          <img class="team-logo" src="${logo(row.challenge_team_id)}" alt="">
          <div><div class="team-name">${idx + 1}. ${row.challenge_team_abbr}</div><div class="team-sub">${row.attempts} chal, ${pct(row.overturn_rate)} won</div></div>
          <div class="${cls(row.total_xwpa)}">${wpaPts(row.total_xwpa)}</div>
        </div>
      `).join('');
    }

    function teamMetricValue(row, metric) {
      if (!boardState.includeMissed) return row[metric] || 0;
      if (metric === 'total_xwpa') return row.decision_xwpa || 0;
      if (metric === 'risk_adjusted_xwpa') return row.decision_risk_adjusted_xwpa || 0;
      if (metric === 'batting_xwpa') return row.decision_batting_xwpa || 0;
      if (metric === 'fielding_xwpa') return row.decision_fielding_xwpa || 0;
      if (metric === 'xwpa_per_challenge') return row.decision_xwpa_per_opportunity || 0;
      return row[metric] || 0;
    }

    function metricLabel(label) {
      return boardState.includeMissed ? `${label} + Missed` : label;
    }

    function renderTeamBars(metric = boardState.team.metric) {
      boardState.team.metric = metric;
      const labels = {
        total_xwpa: metricLabel('Direct xWPA'),
        risk_adjusted_xwpa: metricLabel('Risk Adj.'),
        fielding_xwpa: metricLabel('Fielding'),
        batting_xwpa: metricLabel('Batting'),
        xwpa_per_challenge: metricLabel('Per Opportunity')
      };
      const ranked = [...teams]
        .sort((a, b) => teamMetricValue(b, metric) - teamMetricValue(a, metric))
        .map((row, idx) => ({ ...row, board_rank: idx + 1 }));
      const filtered = ranked
        .filter(row => matchesQuery(teamSearchText(row), boardState.team.query));
      const rows = limitedRows(filtered, boardState.team.limit);
      setBoardCount('#teamBoardCount', rows.length, filtered.length, boardState.team.limit);
      updateLimitButtons('team', rows.length, filtered.length, boardState.team.query, boardState.team.limit, 15);
      if (!rows.length) {
        document.querySelector('#teamBars').innerHTML = '<div class="empty-state">No teams match that search.</div>';
        return;
      }
      const max = Math.max(...rows.map(row => Math.abs(teamMetricValue(row, metric))), .001);
      document.querySelector('#teamBars').innerHTML = rows.map((row, idx) => {
        const value = teamMetricValue(row, metric);
        const missedNote = boardState.includeMissed && row.missed_opportunities
          ? `, ${row.missed_opportunities} missed opp, ${wpaPts(row.missed_xwpa)} left`
          : '';
        return `
          <div class="bar-row" title="${row.challenge_team_name}: ${labels[metric]} ${wpaPts(value)}${missedNote}">
            <div class="bar-team"><img class="mini-logo" src="${logo(row.challenge_team_id)}" alt="">${row.board_rank}. ${row.challenge_team_abbr}</div>
            <div class="track"><div class="bar-fill ${value < 0 ? 'neg' : ''}" style="width:${Math.max(3, Math.abs(value) / max * 100)}%"></div></div>
            <div class="bar-value ${cls(value)}">${metric === 'xwpa_per_challenge' ? wpaPts(value) : wpaPts(value)}</div>
          </div>
        `;
      }).join('');
    }

    function renderScatter() {
      const svg = document.querySelector('#teamScatter');
      const tip = document.querySelector('#tooltip');
      const width = 760;
      const height = 410;
      const pad = { left: 56, right: 24, top: 24, bottom: 48 };
      const maxAttempts = Math.max(...teams.map(row => row.attempts));
      const minWpa = Math.min(...teams.map(row => row.total_xwpa), 0);
      const maxWpa = Math.max(...teams.map(row => row.total_xwpa), .1);
      const x = value => pad.left + (value / maxAttempts) * (width - pad.left - pad.right);
      const y = value => height - pad.bottom - ((value - minWpa) / (maxWpa - minWpa || 1)) * (height - pad.top - pad.bottom);
      svg.setAttribute('viewBox', `0 0 ${width} ${height}`);
      svg.innerHTML = `
        <line x1="${pad.left}" y1="${y(0)}" x2="${width - pad.right}" y2="${y(0)}" stroke="rgba(17,24,39,.32)" stroke-dasharray="4 4"></line>
        <line x1="${pad.left}" y1="${pad.top}" x2="${pad.left}" y2="${height - pad.bottom}" stroke="rgba(17,24,39,.45)"></line>
        <line x1="${pad.left}" y1="${height - pad.bottom}" x2="${width - pad.right}" y2="${height - pad.bottom}" stroke="rgba(17,24,39,.45)"></line>
        <text x="${pad.left}" y="16" font-size="12" font-family="ui-sans-serif" fill="#62707b">Direct xWPA</text>
        <text x="${width - 118}" y="${height - 14}" font-size="12" font-family="ui-sans-serif" fill="#62707b">Attempts</text>
      `;
      [...teams].sort((a, b) => a.total_xwpa - b.total_xwpa).forEach(row => {
        const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        const radius = 7 + row.overturn_rate * 9;
        circle.setAttribute('cx', x(row.attempts));
        circle.setAttribute('cy', y(row.total_xwpa));
        circle.setAttribute('r', radius);
        circle.setAttribute('fill', row.total_xwpa >= 0 ? 'rgba(23,106,77,.74)' : 'rgba(163,54,43,.74)');
        circle.setAttribute('stroke', '#111827');
        circle.setAttribute('stroke-width', '1');
        circle.addEventListener('mousemove', event => {
          tip.style.opacity = 1;
          tip.style.left = `${event.clientX}px`;
          tip.style.top = `${event.clientY}px`;
          tip.innerHTML = `<strong>${row.challenge_team_name}</strong><br>${row.attempts} attempts, ${pct(row.overturn_rate)} overturned<br>${wpaWins(row.total_xwpa)} direct wins`;
        });
        circle.addEventListener('mouseleave', () => { tip.style.opacity = 0; });
        svg.appendChild(circle);
      });
      [...teams].sort((a, b) => b.total_xwpa - a.total_xwpa).slice(0, 5).forEach(row => {
        const text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        text.setAttribute('x', x(row.attempts) + 12);
        text.setAttribute('y', y(row.total_xwpa) + 4);
        text.setAttribute('font-size', '12');
        text.setAttribute('font-family', 'ui-sans-serif');
        text.setAttribute('font-weight', '800');
        text.setAttribute('fill', '#111827');
        text.textContent = row.challenge_team_abbr;
        svg.appendChild(text);
      });
    }

    function renderCase(teamId) {
      const row = teams.find(team => String(team.challenge_team_id) === String(teamId)) || teams[0];
      document.querySelector('#caseTitle').textContent = row.challenge_team_abbr;
      document.querySelector('#caseLogo').src = logo(row.challenge_team_id);
      document.querySelector('#caseStats').innerHTML = [
        ['Attempts', row.attempts.toLocaleString()],
        ['Overturn Rate', pct(row.overturn_rate)],
        ['Direct xWPA', wpaWins(row.total_xwpa)],
        ['Risk Adj.', wpaWins(row.risk_adjusted_xwpa)],
        ['Batting', wpaWins(row.batting_xwpa)],
        ['Fielding', wpaWins(row.fielding_xwpa)]
      ].map(([label, value]) => `<div class="case-stat"><span>${label}</span><strong>${value}</strong></div>`).join('');
    }

    function setupTeamSelect() {
      const select = document.querySelector('#teamSelect');
      select.innerHTML = [...teams].sort((a, b) => a.challenge_team_abbr.localeCompare(b.challenge_team_abbr)).map(row => `<option value="${row.challenge_team_id}">${row.challenge_team_abbr} - ${row.challenge_team_name}</option>`).join('');
      select.value = [...teams].sort((a, b) => b.total_xwpa - a.total_xwpa)[0].challenge_team_id;
      select.addEventListener('change', () => renderCase(select.value));
      renderCase(select.value);
    }

    function playerRowsForMode(mode) {
      const isAgainst = mode.includes('_against');
      return (isAgainst ? failedAgainst : players)
        .filter(row => row.role === mode)
        .sort((a, b) => playerMetricValue(b, isAgainst) - playerMetricValue(a, isAgainst))
        .map((row, idx) => ({ ...row, board_rank: idx + 1 }))
        .filter(row => matchesQuery(playerSearchText(row), boardState.player.query));
    }

    function playerMetricValue(row, isAgainst = false) {
      if (isAgainst) return row.fooled_xwpa || 0;
      return boardState.includeMissed ? (row.decision_xwpa || 0) : (row.total_xwpa || 0);
    }

    function renderPlayerBoard(mode = boardState.player.mode) {
      boardState.player.mode = mode;
      const isAgainst = mode.includes('_against');
      const filtered = playerRowsForMode(mode);
      const rows = limitedRows(filtered, boardState.player.limit);
      setBoardCount('#playerBoardCount', rows.length, filtered.length, boardState.player.limit);
      updateLimitButtons('player', rows.length, filtered.length, boardState.player.query, boardState.player.limit, 12);
      if (!rows.length) {
        document.querySelector('#playerBoard').innerHTML = '<div class="empty-state">No players match that search.</div>';
        return;
      }
      document.querySelector('#playerBoard').innerHTML = rows.map((row, idx) => {
        const value = playerMetricValue(row, isAgainst);
        const team = playerTeam(row, isAgainst);
        const detail = isAgainst
          ? `${row.failed_challenges_against}/${row.challenges_against} failed against, ${pct(row.failed_challenges_against_rate)}`
          : `${row.attempts} challenges, ${pct(row.overturn_rate)} won${boardState.includeMissed && row.missed_opportunities ? `, ${row.missed_opportunities} missed` : ''}`;
        return `<div class="player-row"><div><strong>${row.board_rank}. ${row.player_name}</strong><span>${team} / ${detail}</span></div><div class="${cls(value)}">${wpaPts(value)}</div></div>`;
      }).join('');
    }

    function renderSwingCards(side = boardState.swing.side) {
      boardState.swing.side = side;
      const ranked = challenges
        .filter(row => side === 'all' || row.challenge_side === side)
        .sort((a, b) => Math.abs(b.total_xwpa) - Math.abs(a.total_xwpa))
        .map((row, idx) => ({ ...row, board_rank: idx + 1 }));
      const filtered = ranked
        .filter(row => matchesQuery(swingSearchText(row), boardState.swing.query));
      const rows = limitedRows(filtered, boardState.swing.limit);
      setBoardCount('#swingCount', rows.length, filtered.length, boardState.swing.limit);
      updateLimitButtons('swing', rows.length, filtered.length, boardState.swing.query, boardState.swing.limit, 6);
      if (!rows.length) {
        document.querySelector('#swingGrid').innerHTML = '<div class="empty-state">No challenges match that search.</div>';
        return;
      }
      document.querySelector('#swingGrid').innerHTML = rows.map((row, idx) => `
        <article class="swing-card">
          <div class="swing-meta"><span>${String(row.game_date).slice(0, 10)}</span><span>${fmtSide(row.challenge_side)}</span></div>
          <h3>${row.board_rank}. ${row.challenge_team_abbr} ${wpaPts(row.total_xwpa)}</h3>
          <p><strong>${row.challenger_name || 'Challenge'}</strong>, ${row.half} ${row.inning}, ${row.balls}-${row.strikes}, bases ${row.base_state}. ${row.original_call} became ${row.actual_call}.</p>
        </article>
      `).join('');
    }

    function renderMissedCards(side = boardState.missed.side) {
      boardState.missed.side = side;
      const ranked = missed
        .filter(row => side === 'all' || row.challenge_side === side)
        .sort((a, b) => b.missed_expected_xwpa - a.missed_expected_xwpa)
        .map((row, idx) => ({ ...row, board_rank: idx + 1 }));
      const filtered = ranked
        .filter(row => matchesQuery(missedSearchText(row), boardState.missed.query));
      const rows = limitedRows(filtered, boardState.missed.limit);
      setBoardCount('#missedCount', rows.length, filtered.length, boardState.missed.limit);
      updateLimitButtons('missed', rows.length, filtered.length, boardState.missed.query, boardState.missed.limit, 6);
      if (!rows.length) {
        document.querySelector('#missedGrid').innerHTML = '<div class="empty-state">No missed opportunities match that search.</div>';
        return;
      }
      document.querySelector('#missedGrid').innerHTML = rows.map(row => `
        <article class="swing-card">
          <div class="swing-meta"><span>${String(row.game_date).slice(0, 10)}</span><span>${fmtSide(row.challenge_side)}</span></div>
          <h3>${row.board_rank}. ${row.challenge_team_abbr} ${wpaPts(row.missed_expected_xwpa)}</h3>
          <p><strong>${row.player_name || 'Opportunity'}</strong>, ${row.half} ${row.inning}, ${row.balls}-${row.strikes}, bases ${row.base_state}. ${row.original_call} projected to ${row.corrected_call}, ${Number(row.zone_distance_inches).toFixed(1)} inches beyond the edge, ${pct(row.overturn_probability)} overturn probability, ${row.remaining_challenge_losses_before} losses left.</p>
        </article>
      `).join('');
    }

    const playerModeLabels = {
      hitter: 'Hitters',
      catcher: 'Catcher Challenges',
      pitcher: 'Pitcher Challenges',
      catcher_against: 'Catchers: Failed Against',
      pitcher_against: 'Pitchers: Failed Against'
    };

    function teamSuggestionOptions() {
      return teams.map(row => ({
        label: row.challenge_team_name,
        value: row.challenge_team_name,
        detail: row.challenge_team_abbr,
        search: teamSearchText(row)
      }));
    }

    function playerSuggestionOptions() {
      const mode = boardState.player.mode;
      const rows = (mode.includes('_against') ? failedAgainst : players).filter(row => row.role === mode);
      return rows.map(row => ({
        label: row.player_name,
        value: row.player_name,
        detail: `${playerTeam(row)} / ${playerModeLabels[mode]}`,
        search: playerSearchText(row)
      }));
    }

    function swingSuggestionOptions() {
      const teamOptions = uniqueBy(challenges.map(row => {
        const team = teamByAbbr.get(row.challenge_team_abbr);
        return {
          label: team ? team.challenge_team_name : row.challenge_team_abbr,
          value: team ? team.challenge_team_name : row.challenge_team_abbr,
          detail: row.challenge_team_abbr,
          search: `${team ? team.challenge_team_name : ''} ${row.challenge_team_abbr}`
        };
      }), option => option.detail);
      const playerOptions = uniqueBy(challenges
        .filter(row => row.challenger_name)
        .map(row => ({
          label: row.challenger_name,
          value: row.challenger_name,
          detail: `${row.challenge_team_abbr} / ${fmtSide(row.challenge_side)}`,
          search: swingSearchText(row)
        })), option => option.value);
      return [...teamOptions, ...playerOptions];
    }

    function missedSuggestionOptions() {
      const teamOptions = uniqueBy(missed.map(row => ({
        label: row.challenge_team_name || row.challenge_team_abbr,
        value: row.challenge_team_name || row.challenge_team_abbr,
        detail: row.challenge_team_abbr,
        search: missedSearchText(row)
      })), option => option.detail);
      const playerOptions = uniqueBy(missed
        .filter(row => row.player_name)
        .map(row => ({
          label: row.player_name,
          value: row.player_name,
          detail: `${row.challenge_team_abbr} / ${fmtSide(row.challenge_side)}`,
          search: missedSearchText(row)
        })), option => option.value);
      return [...teamOptions, ...playerOptions];
    }

    setupTypeahead('#teamBoardSearch', '#teamBoardSuggestions', teamSuggestionOptions, value => {
      boardState.team.query = value;
      boardState.team.limit = 15;
      renderTeamBars();
    });
    setupTypeahead('#playerBoardSearch', '#playerBoardSuggestions', playerSuggestionOptions, value => {
      boardState.player.query = value;
      boardState.player.limit = 12;
      renderPlayerBoard();
    });
    setupTypeahead('#swingSearch', '#swingSuggestions', swingSuggestionOptions, value => {
      boardState.swing.query = value;
      boardState.swing.limit = 6;
      renderSwingCards();
    });
    setupTypeahead('#missedSearch', '#missedSuggestions', missedSuggestionOptions, value => {
      boardState.missed.query = value;
      boardState.missed.limit = 6;
      renderMissedCards();
    });

    document.querySelector('#teamShowMore').addEventListener('click', () => {
      boardState.team.limit = boardState.team.limit === Infinity ? Infinity : boardState.team.limit + 15;
      renderTeamBars();
    });
    document.querySelector('#teamShowAll').addEventListener('click', () => {
      boardState.team.limit = Infinity;
      renderTeamBars();
    });
    document.querySelector('#teamReset').addEventListener('click', () => {
      boardState.team.query = '';
      boardState.team.limit = 15;
      document.querySelector('#teamBoardSearch').value = '';
      renderTeamBars();
    });

    document.querySelector('#playerShowMore').addEventListener('click', () => {
      boardState.player.limit = boardState.player.limit === Infinity ? Infinity : boardState.player.limit + 12;
      renderPlayerBoard();
    });
    document.querySelector('#playerShowAll').addEventListener('click', () => {
      boardState.player.limit = Infinity;
      renderPlayerBoard();
    });
    document.querySelector('#playerReset').addEventListener('click', () => {
      boardState.player.query = '';
      boardState.player.limit = 12;
      document.querySelector('#playerBoardSearch').value = '';
      renderPlayerBoard();
    });

    document.querySelector('#swingShowMore').addEventListener('click', () => {
      boardState.swing.limit = boardState.swing.limit === Infinity ? Infinity : boardState.swing.limit + 6;
      renderSwingCards();
    });
    document.querySelector('#swingShowAll').addEventListener('click', () => {
      boardState.swing.limit = Infinity;
      renderSwingCards();
    });
    document.querySelector('#swingReset').addEventListener('click', () => {
      boardState.swing.query = '';
      boardState.swing.limit = 6;
      document.querySelector('#swingSearch').value = '';
      renderSwingCards();
    });

    document.querySelector('#missedShowMore').addEventListener('click', () => {
      boardState.missed.limit = boardState.missed.limit === Infinity ? Infinity : boardState.missed.limit + 6;
      renderMissedCards();
    });
    document.querySelector('#missedShowAll').addEventListener('click', () => {
      boardState.missed.limit = Infinity;
      renderMissedCards();
    });
    document.querySelector('#missedReset').addEventListener('click', () => {
      boardState.missed.query = '';
      boardState.missed.limit = 6;
      document.querySelector('#missedSearch').value = '';
      renderMissedCards();
    });

    function setIncludeMissed(value) {
      boardState.includeMissed = value;
      document.querySelector('#teamIncludeMissed').checked = value;
      document.querySelector('#playerIncludeMissed').checked = value;
      renderTeamBars();
      renderPlayerBoard();
    }
    document.querySelector('#teamIncludeMissed').addEventListener('change', event => setIncludeMissed(event.target.checked));
    document.querySelector('#playerIncludeMissed').addEventListener('change', event => setIncludeMissed(event.target.checked));

    document.querySelectorAll('[data-team-metric]').forEach(button => {
      button.addEventListener('click', () => {
        document.querySelectorAll('[data-team-metric]').forEach(item => item.classList.remove('active'));
        button.classList.add('active');
        renderTeamBars(button.dataset.teamMetric);
      });
    });
    document.querySelectorAll('[data-player-mode]').forEach(button => {
      button.addEventListener('click', () => {
        document.querySelectorAll('[data-player-mode]').forEach(item => item.classList.remove('active'));
        button.classList.add('active');
        boardState.player.query = '';
        boardState.player.limit = 12;
        document.querySelector('#playerBoardSearch').value = '';
        renderPlayerBoard(button.dataset.playerMode);
      });
    });
    document.querySelectorAll('[data-swing-side]').forEach(button => {
      button.addEventListener('click', () => {
        document.querySelectorAll('[data-swing-side]').forEach(item => item.classList.remove('active'));
        button.classList.add('active');
        boardState.swing.limit = 6;
        renderSwingCards(button.dataset.swingSide);
      });
    });
    document.querySelectorAll('[data-missed-side]').forEach(button => {
      button.addEventListener('click', () => {
        document.querySelectorAll('[data-missed-side]').forEach(item => item.classList.remove('active'));
        button.classList.add('active');
        boardState.missed.limit = 6;
        renderMissedCards(button.dataset.missedSide);
      });
    });

    renderSideTopTeams();
    renderTeamBars();
    renderScatter();
    setupTeamSelect();
    renderPlayerBoard();
    renderSwingCards();
    renderMissedCards();
  </script>
</body>
</html>"""

    replacements = {
        "__YEAR__": str(year),
        "__UPDATED__": updated,
        "__END_DATE__": html.escape(end_date),
        "__TOTAL_ATTEMPTS__": f"{total_attempts:,}",
        "__OVERTURN_RATE__": pct(overturn_rate),
        "__LEAGUE_XWPA_POINTS__": signed_wpa_points(league_xwpa),
        "__LEAGUE_XWPA_WINS__": signed_wins(league_xwpa),
        "__LEAGUE_RISK_WINS__": signed_wins(league_risk),
        "__MISSED_COUNT__": f"{len(missed_by_value):,}",
        "__MISSED_TOTAL_POINTS__": signed_wpa_points(sum(float(row.get("missed_expected_xwpa", 0.0)) for row in missed_by_value)),
        "__TOP_MISSED_PHRASE__": missed_phrase(top_missed),
        "__LEADER_ABBR__": html.escape(str(leader.get("challenge_team_abbr", ""))),
        "__LEADER_PHRASE__": article_team_phrase(leader),
        "__LEADER_PHRASE_SHORT__": html.escape(str(leader.get("challenge_team_name") or leader.get("challenge_team_abbr") or "The leader")),
        "__LEADER_WINS__": signed_wins(float(leader.get("total_xwpa", 0.0))),
        "__LEADER_POINTS__": signed_wpa_points(float(leader.get("total_xwpa", 0.0))),
        "__LEADER_XWPA_PER__": signed_wpa_points(float(leader.get("xwpa_per_challenge", 0.0))),
        "__RISK_LEADER_PHRASE__": article_team_phrase(risk_leader),
        "__TOP_SWING_PHRASE__": challenge_phrase(top_swing),
        "__TOP_HITTER__": article_player_phrase(hitter_leaders[0] if hitter_leaders else {}),
        "__TOP_HITTER_WPA__": signed_wpa_points(float(hitter_leaders[0].get("total_xwpa", 0.0))) if hitter_leaders else "+0.0",
        "__TOP_CATCHER__": article_player_phrase(catcher_leaders[0] if catcher_leaders else {}),
        "__TOP_CATCHER_WPA__": signed_wpa_points(float(catcher_leaders[0].get("total_xwpa", 0.0))) if catcher_leaders else "+0.0",
        "__TOP_FAILED_CATCHER__": article_player_phrase(failed_catchers[0] if failed_catchers else {}),
        "__TOP_FAILED_CATCHER_PHRASE__": article_player_phrase(failed_catchers[0] if failed_catchers else {}),
        "__TOP_FAILED_CATCHER_WPA__": signed_wpa_points(float(failed_catchers[0].get("fooled_xwpa", 0.0))) if failed_catchers else "+0.0",
        "__TOP_FAILED_CATCHER_COUNT__": f"{int(failed_catchers[0].get('failed_challenges_against', 0))}" if failed_catchers else "0",
        "__TOP_FAILED_PITCHER_PHRASE__": article_player_phrase(failed_pitchers[0] if failed_pitchers else {}),
        "__TOP_FAILED_PITCHER_COUNT__": f"{int(failed_pitchers[0].get('failed_challenges_against', 0))}" if failed_pitchers else "0",
        "__TEAM_JSON__": team_json,
        "__PLAYER_JSON__": player_json,
        "__FAILED_AGAINST_JSON__": failed_against_json,
        "__CHALLENGE_JSON__": challenge_json,
        "__MISSED_JSON__": missed_json,
        "__ADSENSE_BANNER__": adsense_banner,
    }
    for token, value in replacements.items():
        template = template.replace(token, value)
    return template


def render_dashboard(
    team_rows: list[dict[str, Any]],
    player_rows: list[dict[str, Any]],
    failed_against_rows: list[dict[str, Any]],
    challenge_rows: list[dict[str, Any]],
    year: int,
) -> str:
    team_json = json.dumps([round_for_json(row) for row in team_rows], ensure_ascii=False)
    player_json = json.dumps([round_for_json(row) for row in player_rows], ensure_ascii=False)
    failed_against_json = json.dumps([round_for_json(row) for row in failed_against_rows], ensure_ascii=False)
    challenge_json = json.dumps([round_for_json(row) for row in challenge_rows[:250]], ensure_ascii=False)
    updated = date.today().isoformat()
    leader = team_rows[0] if team_rows else {}

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>ABS Challenge xWPA</title>
  <style>
    :root {{
      --ink: #17212b;
      --muted: #607080;
      --paper: #f6f2e9;
      --panel: #fffaf0;
      --line: #d7ccb9;
      --teal: #007c89;
      --red: #b13d2f;
      --gold: #b7832f;
      --navy: #23395b;
      --green: #2f7d4f;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--ink);
      background:
        linear-gradient(90deg, rgba(35,57,91,.06) 1px, transparent 1px) 0 0 / 28px 28px,
        linear-gradient(0deg, rgba(35,57,91,.05) 1px, transparent 1px) 0 0 / 28px 28px,
        var(--paper);
      font-family: Georgia, "Times New Roman", serif;
    }}
    .wrap {{ max-width: 1260px; margin: 0 auto; padding: 28px 20px 48px; }}
    header {{ display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 20px; align-items: end; border-bottom: 2px solid var(--ink); padding-bottom: 18px; }}
    h1 {{ margin: 0; font-size: clamp(34px, 5vw, 76px); line-height: .9; letter-spacing: 0; }}
    .deck {{ max-width: 780px; margin: 14px 0 0; font: 16px/1.45 ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color: var(--muted); }}
    .stamp {{ border: 2px solid var(--ink); padding: 12px 14px; min-width: 210px; background: var(--panel); }}
    .stamp strong {{ display: block; font-size: 26px; }}
    .kpis {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin: 22px 0; }}
    .kpi {{ background: var(--panel); border: 1px solid var(--line); border-top: 5px solid var(--teal); padding: 14px; min-height: 104px; }}
    .kpi:nth-child(2) {{ border-top-color: var(--gold); }}
    .kpi:nth-child(3) {{ border-top-color: var(--red); }}
    .kpi:nth-child(4) {{ border-top-color: var(--green); }}
    .kpi .label {{ color: var(--muted); font: 12px/1.2 ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; text-transform: uppercase; letter-spacing: .08em; }}
    .kpi .value {{ margin-top: 8px; font-size: 30px; font-weight: 700; }}
    section {{ margin-top: 28px; }}
    .section-head {{ display: flex; align-items: center; justify-content: space-between; gap: 16px; margin-bottom: 10px; }}
    h2 {{ margin: 0; font-size: 24px; }}
    .controls {{ display: flex; gap: 8px; flex-wrap: wrap; }}
    button, input {{
      font: 13px ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      border: 1px solid var(--ink);
      background: var(--panel);
      color: var(--ink);
      padding: 8px 10px;
    }}
    button.active {{ background: var(--ink); color: white; }}
    input {{ min-width: 220px; }}
    .table-shell {{ overflow-x: auto; border: 1px solid var(--line); background: rgba(255,250,240,.86); }}
    table {{ width: 100%; border-collapse: collapse; font: 13px/1.35 ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }}
    th {{ position: sticky; top: 0; background: #eadfcc; border-bottom: 1px solid var(--ink); text-align: right; padding: 9px 10px; white-space: nowrap; cursor: pointer; }}
    th:first-child, td:first-child {{ text-align: left; }}
    td {{ border-bottom: 1px solid rgba(215,204,185,.8); padding: 8px 10px; text-align: right; white-space: nowrap; }}
    tr:hover td {{ background: rgba(0,124,137,.08); }}
    .team-cell {{ display: inline-flex; align-items: center; gap: 8px; font-weight: 700; }}
    .logo {{ width: 24px; height: 24px; object-fit: contain; }}
    .pos {{ color: var(--green); font-weight: 700; }}
    .neg {{ color: var(--red); font-weight: 700; }}
    .note {{ margin-top: 8px; color: var(--muted); font: 12px/1.4 ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }}
    @media (max-width: 760px) {{
      header, .kpis {{ grid-template-columns: 1fr; }}
      h1 {{ font-size: 42px; }}
      .section-head {{ align-items: stretch; flex-direction: column; }}
      input {{ width: 100%; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <header>
      <div>
        <h1>ABS Challenge xWPA</h1>
        <p class="deck">A win-probability view of {year} MLB ABS challenges. Total xWPA compares the corrected ABS call to the original umpire call; the inventory-risk proxy is kept separate so the headline stays tied to the call itself.</p>
      </div>
      <div class="stamp">
        <span>Leader</span>
        <strong>{html.escape(str(leader.get("challenge_team_abbr", "")))}</strong>
        <span>{signed_pct(float(leader.get("total_xwpa", 0)))} WPA pts</span>
      </div>
    </header>

    <div class="kpis">
      <div class="kpi"><div class="label">Challenges</div><div class="value" id="kpi-attempts"></div></div>
      <div class="kpi"><div class="label">League Direct xWPA</div><div class="value" id="kpi-xwpa"></div></div>
      <div class="kpi"><div class="label">Overturn Rate</div><div class="value" id="kpi-rate"></div></div>
      <div class="kpi"><div class="label">Updated</div><div class="value">{updated}</div></div>
    </div>

    <section>
      <div class="section-head">
        <h2>Team Leaderboard</h2>
      </div>
      <div class="table-shell"><table id="team-table"></table></div>
      <p class="note">xWPA columns are shown as win-probability percentage points. Risk-adjusted xWPA subtracts the failed-challenge inventory proxy.</p>
    </section>

    <section>
      <div class="section-head">
        <h2>Player Breakdowns</h2>
        <div class="controls">
          <button data-role="hitter" class="active">Hitters</button>
          <button data-role="catcher">Catchers</button>
          <button data-role="pitcher">Pitchers</button>
          <button data-role="fielder_challenger">Fielding Challengers</button>
          <input id="player-search" placeholder="Search players">
        </div>
      </div>
      <div class="table-shell"><table id="player-table"></table></div>
    </section>

    <section>
      <div class="section-head">
        <h2>Hitter Challenges Against</h2>
        <div class="controls">
          <button data-against-role="catcher_against" class="active">Catchers</button>
          <button data-against-role="pitcher_against">Pitchers</button>
        </div>
      </div>
      <div class="table-shell"><table id="failed-against-table"></table></div>
      <p class="note">Failed challenges against are opponent hitter challenges that were confirmed. Fooled xWPA credits the catcher or pitcher for the opponent's burned-challenge proxy.</p>
    </section>

    <section>
      <div class="section-head"><h2>Largest Challenge Swings</h2></div>
      <div class="table-shell"><table id="challenge-table"></table></div>
    </section>
  </div>

  <script>
    const teams = {team_json};
    const players = {player_json};
    const failedAgainst = {failed_against_json};
    const challenges = {challenge_json};

    const fmtPct = v => `${{(v * 100).toFixed(2)}}`;
    const fmtRate = v => `${{(v * 100).toFixed(1)}}%`;
    const signed = v => `<span class="${{v >= 0 ? 'pos' : 'neg'}}">${{fmtPct(v)}}</span>`;
    const logo = id => `<img class="logo" src="https://www.mlbstatic.com/team-logos/${{id}}.svg" alt="">`;

    function renderTable(el, rows, columns) {{
      el.innerHTML = `<thead><tr>${{columns.map(c => `<th data-key="${{c.key}}">${{c.label}}</th>`).join('')}}</tr></thead><tbody>${{
        rows.map(row => `<tr>${{columns.map(c => `<td>${{c.render ? c.render(row[c.key], row) : row[c.key] ?? ''}}</td>`).join('')}}</tr>`).join('')
      }}</tbody>`;
      el.querySelectorAll('th').forEach(th => th.addEventListener('click', () => {{
        const key = th.dataset.key;
        const sorted = [...rows].sort((a, b) => (Number(b[key]) || String(b[key]).localeCompare(String(a[key]))) - (Number(a[key]) || 0));
        renderTable(el, sorted, columns);
      }}));
    }}

    const teamCols = [
      {{key:'challenge_team_abbr', label:'Team', render:(v,r)=>`<span class="team-cell">${{logo(r.challenge_team_id)}}${{v}}</span>`}},
      {{key:'attempts', label:'Chal'}},
      {{key:'overturn_rate', label:'Won%', render:fmtRate}},
      {{key:'total_xwpa', label:'Total xWPA', render:signed}},
      {{key:'direct_wpa', label:'Direct WPA', render:signed}},
      {{key:'option_wpa_proxy', label:'Option Proxy', render:signed}},
      {{key:'risk_adjusted_xwpa', label:'Risk Adj', render:signed}},
      {{key:'xwpa_per_challenge', label:'xWPA / Chal', render:signed}},
      {{key:'strikeout_flips', label:'K Flip'}},
      {{key:'walk_flips', label:'BB Flip'}},
      {{key:'exhausting_fails', label:'Exhaust'}}
    ];

    const playerCols = [
      {{key:'player_name', label:'Player'}},
      {{key:'challenge_team_abbr', label:'Team'}},
      {{key:'attempts', label:'Chal'}},
      {{key:'overturn_rate', label:'Won%', render:fmtRate}},
      {{key:'total_xwpa', label:'Total xWPA', render:signed}},
      {{key:'direct_wpa', label:'Direct WPA', render:signed}},
      {{key:'option_wpa_proxy', label:'Option Proxy', render:signed}},
      {{key:'risk_adjusted_xwpa', label:'Risk Adj', render:signed}},
      {{key:'xwpa_per_challenge', label:'xWPA / Chal', render:signed}}
    ];

    const failedAgainstCols = [
      {{key:'player_name', label:'Player'}},
      {{key:'team_abbr', label:'Team'}},
      {{key:'challenges_against', label:'Hitter Chal'}},
      {{key:'failed_challenges_against', label:'Failed Against'}},
      {{key:'failed_challenges_against_rate', label:'Fail%', render:fmtRate}},
      {{key:'fooled_xwpa', label:'Fooled xWPA', render:signed}},
      {{key:'fooled_xwpa_per_challenge_against', label:'Fooled / Chal', render:signed}},
      {{key:'failed_against_wpa_at_stake', label:'WPA At Stake', render:signed}},
      {{key:'opponent_success_xwpa', label:'Opp Success xWPA', render:signed}},
      {{key:'failed_strikeout_challenges_against', label:'K Confirms'}}
    ];

    const challengeCols = [
      {{key:'game_date', label:'Date', render:v=>String(v).slice(0,10)}},
      {{key:'challenge_team_abbr', label:'Team'}},
      {{key:'challenger_name', label:'Challenger'}},
      {{key:'challenge_side', label:'Side'}},
      {{key:'inning', label:'Inn', render:(v,r)=>`${{r.half}} ${{v}}`}},
      {{key:'base_state', label:'Bases'}},
      {{key:'balls', label:'Count', render:(v,r)=>`${{v}}-${{r.strikes}}`}},
      {{key:'original_call', label:'Orig'}},
      {{key:'actual_call', label:'ABS'}},
      {{key:'total_xwpa', label:'xWPA', render:signed}},
      {{key:'wpa_if_overturned', label:'If Ovr', render:signed}}
    ];

    function currentPlayerRows(role='hitter') {{
      const q = document.querySelector('#player-search').value.trim().toLowerCase();
      return players
        .filter(r => r.role === role)
        .filter(r => !q || String(r.player_name).toLowerCase().includes(q))
        .sort((a,b) => b.total_xwpa - a.total_xwpa)
        .slice(0, 100);
    }}

    let activeRole = 'hitter';
    document.querySelectorAll('[data-role]').forEach(btn => btn.addEventListener('click', () => {{
      document.querySelectorAll('[data-role]').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      activeRole = btn.dataset.role;
      renderTable(document.querySelector('#player-table'), currentPlayerRows(activeRole), playerCols);
    }}));
    document.querySelector('#player-search').addEventListener('input', () => {{
      renderTable(document.querySelector('#player-table'), currentPlayerRows(activeRole), playerCols);
    }});

    function againstRows(role='catcher_against') {{
      return failedAgainst
        .filter(r => r.role === role)
        .sort((a,b) => b.fooled_xwpa - a.fooled_xwpa)
        .slice(0, 100);
    }}

    let activeAgainstRole = 'catcher_against';
    document.querySelectorAll('[data-against-role]').forEach(btn => btn.addEventListener('click', () => {{
      document.querySelectorAll('[data-against-role]').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      activeAgainstRole = btn.dataset.againstRole;
      renderTable(document.querySelector('#failed-against-table'), againstRows(activeAgainstRole), failedAgainstCols);
    }}));

    const totalAttempts = teams.reduce((sum, r) => sum + r.attempts, 0);
    const totalOverturns = teams.reduce((sum, r) => sum + r.overturns, 0);
    const totalXwpa = teams.reduce((sum, r) => sum + r.total_xwpa, 0);
    document.querySelector('#kpi-attempts').textContent = totalAttempts.toLocaleString();
    document.querySelector('#kpi-rate').textContent = fmtRate(totalOverturns / totalAttempts);
    document.querySelector('#kpi-xwpa').textContent = fmtPct(totalXwpa);

    renderTable(document.querySelector('#team-table'), teams, teamCols);
    renderTable(document.querySelector('#player-table'), currentPlayerRows(), playerCols);
    renderTable(document.querySelector('#failed-against-table'), againstRows(), failedAgainstCols);
    renderTable(document.querySelector('#challenge-table'), challenges.sort((a,b)=>Math.abs(b.total_xwpa)-Math.abs(a.total_xwpa)).slice(0,80), challengeCols);
  </script>
</body>
</html>"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, default=date.today().year)
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument("--force", action="store_true", help="refresh cached raw API responses")
    parser.add_argument(
        "--model-scope",
        choices=["season", "challenge-games"],
        default="season",
        help="use all completed season games or only games with ABS challenges to train run distributions",
    )
    parser.add_argument(
        "--adsense-client",
        default=os.environ.get("ADSENSE_CLIENT", ""),
        help="optional AdSense client id, e.g. ca-pub-0000000000000000",
    )
    parser.add_argument(
        "--adsense-slot",
        default=os.environ.get("ADSENSE_SLOT", ""),
        help="optional AdSense display ad slot id for the article banner",
    )
    args = parser.parse_args()

    mkdirs()
    print(f"Fetching MLB teams and Savant ABS challenges for {args.year}...", file=sys.stderr)
    teams = get_teams(args.year, args.force)
    _ = fetch_savant_summary_csv(args.year, args.force)
    challenges = collect_challenges(teams, args.year, args.force)
    if not challenges:
        raise SystemExit("No ABS challenge rows found.")

    challenge_game_pks = sorted({int(row["game_pk"]) for row in challenges})
    if args.model_scope == "season":
        model_game_pks = get_completed_game_pks(args.year, args.end_date, args.force)
    else:
        model_game_pks = challenge_game_pks

    print(f"Training run model from {len(model_game_pks)} game feeds...", file=sys.stderr)
    model = RunModel()
    states_by_game: dict[int, dict[str, PitchState]] = {}
    for idx, game_pk in enumerate(model_game_pks, 1):
        if idx % 50 == 0:
            print(f"  processed {idx}/{len(model_game_pks)} games", file=sys.stderr)
        feed = fetch_game_feed(game_pk, args.force)
        states = replay_game(feed, model)
        if game_pk in challenge_game_pks:
            states_by_game[game_pk] = states

    missing_games = [pk for pk in challenge_game_pks if pk not in states_by_game]
    for game_pk in missing_games:
        feed = fetch_game_feed(game_pk, args.force)
        states_by_game[game_pk] = replay_game(feed, None)

    print(f"Evaluating {len(challenges)} ABS challenge attempts...", file=sys.stderr)
    evaluated: list[dict[str, Any]] = []
    missing_states = 0
    for row in challenges:
        state = states_by_game.get(int(row["game_pk"]), {}).get(str(row["play_id"]))
        if state is None:
            missing_states += 1
            continue
        enriched = dict(row)
        enriched.update(evaluate_challenge(enriched, state, model))
        evaluated.append(enriched)

    if missing_states:
        print(f"Warning: skipped {missing_states} challenges without matching pitch state.", file=sys.stderr)

    add_inventory_columns(evaluated)

    team_rows = aggregate_rows(
        evaluated,
        ["challenge_team_id", "challenge_team_abbr", "challenge_team_name"],
    )
    side_rows = aggregate_rows(
        evaluated,
        ["challenge_team_id", "challenge_team_abbr", "challenge_side"],
    )
    side_lookup = {
        (row["challenge_team_id"], row["challenge_side"]): row["total_xwpa"]
        for row in side_rows
    }
    for row in team_rows:
        row["batting_xwpa"] = side_lookup.get((row["challenge_team_id"], "batting"), 0.0)
        row["fielding_xwpa"] = side_lookup.get((row["challenge_team_id"], "fielding"), 0.0)

    player_attempts = build_player_rows(evaluated)
    player_rows = aggregate_rows(
        player_attempts,
        ["role", "player_id", "player_name", "challenge_team_abbr"],
    )
    player_rows = [row for row in player_rows if row.get("player_id")]
    failed_against_attempts = build_failed_against_rows(evaluated)
    failed_against_rows = aggregate_failed_against_rows(failed_against_attempts)
    missed_game_pks = model_game_pks if args.model_scope == "season" else challenge_game_pks
    print(f"Estimating missed challenge opportunities from {len(missed_game_pks)} game feeds...", file=sys.stderr)
    missed_rows = build_missed_opportunities(missed_game_pks, evaluated, model, args.force)
    missed_team_rows = aggregate_missed_rows(
        missed_rows,
        ["challenge_team_id", "challenge_team_abbr", "challenge_team_name"],
    )
    missed_player_rows = aggregate_missed_rows(
        [row for row in missed_rows if row.get("player_id")],
        ["role", "player_id", "player_name", "challenge_team_abbr"],
    )
    merge_team_missed_rows(team_rows, missed_team_rows)
    merge_player_missed_rows(player_rows, missed_player_rows)

    write_csv(PROCESSED / "team_abs_xwpa.csv", team_rows)
    write_csv(PROCESSED / "team_side_abs_xwpa.csv", side_rows)
    write_csv(PROCESSED / "player_abs_xwpa.csv", player_rows)
    write_csv(PROCESSED / "player_failed_challenges_against.csv", failed_against_rows)
    write_csv(PROCESSED / "challenges_abs_xwpa.csv", evaluated)
    write_csv(PROCESSED / "missed_challenge_opportunities.csv", missed_rows)
    write_csv(PROCESSED / "team_missed_challenge_opportunities.csv", missed_team_rows)
    write_csv(PROCESSED / "player_missed_challenge_opportunities.csv", missed_player_rows)

    summary = {
        "year": args.year,
        "end_date": args.end_date,
        "model_scope": args.model_scope,
        "model_games": len(model_game_pks),
        "challenge_attempts": len(evaluated),
        "missed_challenge_opportunities": len(missed_rows),
        "team_rows": len(team_rows),
        "player_rows": len(player_rows),
        "failed_against_rows": len(failed_against_rows),
    }
    (PROCESSED / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    dashboard_html = render_dashboard(team_rows, player_rows, failed_against_rows, evaluated, args.year)
    article_html = render_article_page(
        team_rows,
        player_rows,
        failed_against_rows,
        evaluated,
        missed_rows,
        args.year,
        args.end_date,
        args.adsense_client,
        args.adsense_slot,
    )
    (SITE / "dashboard.html").write_text(dashboard_html, encoding="utf-8")
    (SITE / "article.html").write_text(article_html, encoding="utf-8")
    (SITE / "index.html").write_text(article_html, encoding="utf-8")
    (SITE / ".nojekyll").write_text("", encoding="utf-8")
    publisher_id = adsense_publisher_id(args.adsense_client)
    ads_txt = SITE / "ads.txt"
    if publisher_id:
        ads_txt.write_text(f"google.com, {publisher_id}, DIRECT, f08c47fec0942fa0\n", encoding="utf-8")
    elif ads_txt.exists():
        ads_txt.unlink()
    mirror_processed_data_to_site()

    print(json.dumps(summary, indent=2), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
