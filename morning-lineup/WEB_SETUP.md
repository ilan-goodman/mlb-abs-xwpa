# Shareable Setup Page

The public setup page is published inside the existing GitHub Pages site at:

```text
https://ilan-goodman.github.io/mlb-abs-xwpa/morning-lineup/
```

It is intentionally static and safe:

- It does not store recipient emails.
- It does not include any Resend, Brevo, SMTP, or GitHub API keys.
- It lets a family member choose teams, enter one or more email recipients,
  preview the morning note, print a sample, download the subscriber JSON, or
  email the setup request to you.
- It can also receive manage/unsubscribe links from the daily email, but the
  final update still has to be handled by you or a private endpoint.

## Recommended Family Flow

1. Share the page with your mom.
2. Ask her to fill in the reader name, email recipients, and teams.
3. Ask her to click `Preview` and `Print Sample`.
4. Ask her to click `Send Setup Request`.
5. You paste the generated subscriber JSON into the private sender config or
   GitHub secret used by the daily job.

You can prefill your email in the setup page URL:

```text
https://ilan-goodman.github.io/mlb-abs-xwpa/morning-lineup/?owner=you@example.com
```

## Optional Endpoint

The page also supports an optional private setup endpoint:

```text
https://ilan-goodman.github.io/mlb-abs-xwpa/morning-lineup/?endpoint=https%3A%2F%2Fexample.com%2Fsetup&owner=you@example.com
```

That endpoint should accept a JSON POST body and store or forward the setup
request. Do not put API keys in the browser page. Keep keys in the endpoint,
GitHub Actions secrets, or a private server.

## Daily Sending Secrets

The active GitHub Actions workflow reads these values:

- `RESEND_API_KEY`: Resend API key.
- `MORNING_LINEUP_EMAIL_PROVIDER`: optional. Set to `smtp` to use Gmail or
  another SMTP mailbox instead of Resend.
- `MORNING_LINEUP_FROM_EMAIL`: verified sender, such as
  `Morning Lineup <news@example.com>`.
- `MORNING_LINEUP_SUBSCRIBERS_JSON`: full subscriber JSON array from the setup
  page.
- `MORNING_LINEUP_OWNER_EMAIL`: optional email address for preference-change
  requests.
- `SMTP_HOST`, `SMTP_PORT`, `SMTP_USERNAME`, `SMTP_PASSWORD`: required only for
  SMTP/Gmail sending.

The workflow runs at 5 AM Pacific using two UTC cron entries plus a Pacific-time
gate for daylight saving time.

## Why This Is Not Fully Automatic Yet

GitHub Pages is static hosting. It can display a form, generate JSON, and open
an email draft, but it cannot securely save subscribers or send email by itself.
Those tasks require a private backend or a scheduled job with secrets.
