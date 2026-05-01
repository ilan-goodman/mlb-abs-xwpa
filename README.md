# MLB ABS Challenge xWPA

This project calculates win-probability value for MLB Automated Ball-Strike
challenge attempts.

It pulls 2026 ABS challenge data from Baseball Savant, joins each challenge
pitch to MLB Stats API game feeds for base/out/count/score context, builds a
season-to-date run distribution model, and writes:

- `data/processed/team_abs_xwpa.csv`
- `data/processed/player_abs_xwpa.csv`
- `data/processed/player_failed_challenges_against.csv`
- `data/processed/challenges_abs_xwpa.csv`
- `site/index.html`
- `site/article.html`
- `site/dashboard.html`

## Metric Definitions

The headline `total_xwpa` is the direct win-probability swing from ABS call
corrections:

```text
win probability after the corrected ABS call
- win probability after the original umpire call
```

It is shown from the challenging team's perspective. Confirmed challenges have
zero direct WPA because the call did not change.

`option_wpa_proxy` is a separate penalty for failed challenges. It converts
Savant's lost-challenge run penalty into wins using a 10 runs-per-win rule of
thumb. This is a proxy for burning challenge inventory, not an official
MLB/Savant metric.

`risk_adjusted_xwpa` is:

```text
direct_wpa + option_wpa_proxy
```

The output also includes `wpa_if_overturned`, which is the value the challenge
would have produced if the call changed. This is useful for understanding the
stakes of confirmed challenges.

## Failed Challenges Against

For catchers and pitchers, `player_failed_challenges_against.csv` measures how
often opposing hitters challenged a called strike and lost.

- `challenges_against`: hitter ABS challenges seen by that catcher/pitcher.
- `failed_challenges_against`: hitter challenges that were confirmed.
- `failed_challenges_against_rate`: confirmed hitter challenges divided by all
  hitter challenges against.
- `fooled_xwpa`: positive credit for the opposing hitter's burned-challenge
  inventory proxy.
- `failed_against_wpa_at_stake`: the hitter-side WPA that would have been
  gained if those failed challenges had overturned.

## Data Sources

- Baseball Savant ABS leaderboard:
  https://baseballsavant.mlb.com/leaderboard/abs-challenges
- Baseball Savant ABS challenge drawer service:
  `https://baseballsavant.mlb.com/leaderboard/services/abs/{team_id}`
- MLB Stats API live game feeds:
  `https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live`

## Run

```bash
python3 scripts/update_abs_xwpa.py --year 2026
```

Open `site/index.html` for the publishable story page or
`site/dashboard.html` for the data dashboard after the run finishes.

## Publish Online

This is a static site, so the simplest deployment is to upload the `site/`
folder to a static host. The article is `site/index.html`, which most hosts
serve at the root URL. The dashboard is `site/dashboard.html`.

Good low-friction options:

- Netlify Drop: drag the `site/` folder into Netlify's drop publisher for a
  quick public URL.
- GitHub Pages: commit the project to GitHub. The included
  `.github/workflows/pages.yml` workflow regenerates the metrics, publishes the
  `site/` folder as a Pages artifact, and refreshes the generated outputs.
- Cloudflare Pages: connect the repository and use `site/` as the build output
  directory.

For a faster refresh while developing, train the win-probability model only on
games that contain ABS challenges:

```bash
python3 scripts/update_abs_xwpa.py --year 2026 --model-scope challenge-games
```

## Schedule Daily

The GitHub Pages workflow runs daily at 6:00 AM America/Chicago using
GitHub Actions' timezone-aware schedule:

```yaml
schedule:
  - cron: "0 6 * * *"
    timezone: "America/Chicago"
```

Local cron example, if you also want this machine to refresh every morning at
8:15 local time:

```cron
15 8 * * * /Users/ilang/Documents/Codex/baseball/scripts/run_daily.sh >> /Users/ilang/Documents/Codex/baseball/data/update.log 2>&1
```

The script caches final MLB game feeds in `data/raw/mlb_game_feeds/`, so local
daily runs only download new completed games. It regenerates the article,
dashboard, and mirrored CSV data in `site/`, so the Pages site is refreshed by
the same scheduled job.
