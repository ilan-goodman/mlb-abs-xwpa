# Morning Lineup

Morning Lineup sends a readable baseball packet every morning: 1 or more
fresh, freely available articles, each as a large-type PDF attachment, plus a
short thank-you note for the person printing and sharing them.

This first version is intentionally small and family-friendly. It does not need
a database or paid backend. Subscribers live in JSON, sources are curated in
JSON, and the daily job can run from a laptop or GitHub Actions.

## What It Does

- Supports MLB overall and all 30 MLB teams.
- Starts with richer curated sources for the Giants, Dodgers, Padres, and their
  listed affiliates.
- Pulls RSS feeds when available and falls back to public article index pages.
- Skips likely paywalled articles instead of redistributing them.
- Scores, dedupes, and selects a useful non-redundant set.
- Generates one large-type PDF per article.
- Sends through Resend or SMTP, or writes `.eml` previews in dry-run mode.

## Quick Start

Create a subscriber file:

```bash
cd morning-lineup
cp config/subscribers.example.json config/subscribers.json
```

Edit `config/subscribers.json`, then preview a packet:

```bash
python3 -m morning_lineup.daily \
  --subscribers config/subscribers.json \
  --output-dir out
```

For a one-off Giants preview without editing JSON:

```bash
cd morning-lineup
python3 -m morning_lineup.daily \
  --team sf-giants \
  --email reader@example.com \
  --output-dir out
```

The preview writes PDFs and an `.eml` file under `out/`.

## Local Setup Form

Run a tiny local form for adding family readers:

```bash
cd morning-lineup
python3 -m morning_lineup.server
```

Open `http://127.0.0.1:8765`, fill out the form, and it will write
`morning-lineup/config/subscribers.json`.

## Shareable GitHub Page

The friendly setup page for non-technical family members lives in the published
GitHub Pages site:

```text
https://ilan-goodman.github.io/mlb-abs-xwpa/morning-lineup/
```

See `WEB_SETUP.md` for the family setup flow and notes on why the public page
does not store emails or contain email-provider API keys.

## Sending Email

Resend is the easiest hosted option:

```bash
export MORNING_LINEUP_FROM_EMAIL="Morning Lineup <onboarding@resend.dev>"
export RESEND_API_KEY="re_..."
python3 -m morning_lineup.daily --send
```

SMTP also works:

```bash
export MORNING_LINEUP_FROM_EMAIL="Morning Lineup <you@example.com>"
export SMTP_HOST="smtp.example.com"
export SMTP_PORT="587"
export SMTP_USERNAME="you@example.com"
export SMTP_PASSWORD="..."
python3 -m morning_lineup.daily --send
```

## GitHub Actions Schedule

Copy `morning-lineup/github-workflows/morning-lineup.yml` to
`.github/workflows/morning-lineup.yml` when this app lives in its own repo.

The workflow uses two UTC schedules, `12:00` and `13:00`, plus a Pacific-time
gate so the job only actually sends at 5 AM Pacific across daylight saving
time changes.

Add these repository secrets:

- `MORNING_LINEUP_SUBSCRIBERS_JSON`: the full JSON array of subscribers.
- `MORNING_LINEUP_FROM_EMAIL`: sender address, for example
  `Morning Lineup <news@yourdomain.com>`.
- `RESEND_API_KEY`: Resend API key.
- `MORNING_LINEUP_OWNER_EMAIL`: optional address for preference or unsubscribe
  requests.

The daily email includes links to manage preferences or stop emails. With the
current static setup page, those links generate a request for you to apply. A
true one-click unsubscribe needs a private endpoint that can update subscriber
storage.

## Source Policy

This app is designed for freely available public articles. For sources that are
paywalled or library-access-only, the safe default is to include links or skip
them rather than create redistributable PDFs. The extractor contains paywall
markers, and the source list should stay limited to pages you are comfortable
printing for personal use.

## Adding Sources

Edit `morning-lineup/config/sources.json`. Sources can be:

- `rss`: RSS or Atom feed.
- `html_index`: a public page whose article links should be scanned.
- `json`: a simple JSON feed with `title`, `url`, `published`, and `summary`.

Then add the source id to a team's `curated_sources` in
`morning-lineup/config/teams.json`, or set `targets` on the source.
