from __future__ import annotations

import html
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from .config import CONFIG_DIR, load_teams, save_subscriber
from .models import Subscriber


SUBSCRIBERS_PATH = CONFIG_DIR / "subscribers.json"


def options_html() -> str:
    teams = sorted(load_teams().values(), key=lambda team: team.name)
    return "\n".join(
        f'<option value="{html.escape(team.id)}">{html.escape(team.name)}</option>' for team in teams
    )


def page(message: str = "") -> str:
    notice = f'<p class="notice">{html.escape(message)}</p>' if message else ""
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Morning Lineup</title>
  <style>
    :root {{
      color-scheme: light;
      --ink: #211b16;
      --paper: #fbf6ec;
      --line: #d8cbb9;
      --accent: #b23b24;
      --field: #f2eadc;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      font-family: Georgia, "Times New Roman", serif;
      color: var(--ink);
      background:
        linear-gradient(90deg, rgba(33,27,22,.035) 1px, transparent 1px) 0 0/28px 28px,
        var(--paper);
    }}
    main {{
      width: min(920px, calc(100% - 32px));
      margin: 0 auto;
      padding: 56px 0;
    }}
    header {{
      border-bottom: 2px solid var(--ink);
      padding-bottom: 22px;
      margin-bottom: 28px;
    }}
    h1 {{
      font-size: clamp(44px, 8vw, 92px);
      line-height: .9;
      margin: 0;
      letter-spacing: 0;
    }}
    .lede {{
      max-width: 690px;
      font-size: 22px;
      line-height: 1.35;
      margin: 18px 0 0;
    }}
    form {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 18px;
      background: rgba(255,255,255,.42);
      border: 1px solid var(--line);
      padding: 24px;
    }}
    label {{
      display: grid;
      gap: 8px;
      font-size: 15px;
      text-transform: uppercase;
      letter-spacing: .05em;
    }}
    input, select {{
      width: 100%;
      border: 1px solid var(--line);
      background: var(--field);
      color: var(--ink);
      padding: 14px 13px;
      font: 20px Georgia, "Times New Roman", serif;
      border-radius: 0;
    }}
    .full {{ grid-column: 1 / -1; }}
    .row {{
      display: flex;
      align-items: center;
      gap: 10px;
      text-transform: none;
      letter-spacing: 0;
      font-size: 19px;
    }}
    .row input {{ width: auto; }}
    button {{
      justify-self: start;
      border: 2px solid var(--ink);
      background: var(--accent);
      color: #fffaf3;
      padding: 13px 22px;
      font: 700 19px Georgia, "Times New Roman", serif;
      cursor: pointer;
    }}
    .notice {{
      border-left: 5px solid var(--accent);
      padding: 12px 16px;
      background: #fff8eb;
      font-size: 18px;
    }}
    @media (max-width: 700px) {{
      form {{ grid-template-columns: 1fr; }}
      main {{ padding-top: 30px; }}
    }}
  </style>
</head>
<body>
  <main>
    <header>
      <h1>Morning Lineup</h1>
      <p class="lede">Set up a printable morning baseball packet: fresh public articles, large type PDFs, and a note that makes the handoff feel human.</p>
    </header>
    {notice}
    <form method="post" action="/subscribe">
      <label>Name
        <input name="name" placeholder="Grandpa" autocomplete="name">
      </label>
      <label>Email
        <input name="email" type="email" placeholder="reader@example.com" required autocomplete="email">
      </label>
      <label class="full">Team
        <select name="team_id">{options_html()}</select>
      </label>
      <label>Timezone
        <input name="timezone" value="America/Los_Angeles">
      </label>
      <label>Send Hour
        <input name="send_hour" type="number" min="0" max="23" value="5">
      </label>
      <label>Max Articles
        <input name="max_articles" type="number" min="1" max="10" value="6">
      </label>
      <label>PDF Type Size
        <input name="font_size" type="number" min="14" max="24" value="18">
      </label>
      <label class="row full">
        <input name="include_minor_leagues" type="checkbox" value="1" checked>
        Include minor league affiliate news when available
      </label>
      <button type="submit">Save Reader</button>
    </form>
  </main>
</body>
</html>"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        self.respond(page())

    def do_POST(self) -> None:
        if self.path != "/subscribe":
            self.send_error(404)
            return
        length = int(self.headers.get("content-length", "0"))
        data = urllib.parse.parse_qs(self.rfile.read(length).decode("utf-8"))
        subscriber = Subscriber(
            email=data.get("email", [""])[0].strip(),
            name=data.get("name", [""])[0].strip(),
            team_id=data.get("team_id", ["mlb"])[0],
            team_ids=(data.get("team_id", ["mlb"])[0],),
            timezone=data.get("timezone", ["America/Los_Angeles"])[0],
            send_hour=int(data.get("send_hour", ["5"])[0] or 5),
            max_articles=int(data.get("max_articles", ["6"])[0] or 6),
            font_size=int(data.get("font_size", ["18"])[0] or 18),
            include_minor_leagues=bool(data.get("include_minor_leagues")),
        )
        if not subscriber.email:
            self.respond(page("Please enter an email address."), status=400)
            return
        save_subscriber(SUBSCRIBERS_PATH, subscriber)
        self.respond(page(f"Saved {subscriber.email}. The daily job can now include this reader."))

    def respond(self, body: str, status: int = 200) -> None:
        payload = body.encode("utf-8")
        self.send_response(status)
        self.send_header("content-type", "text/html; charset=utf-8")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def main() -> None:
    address = ("127.0.0.1", 8765)
    print(f"Morning Lineup setup form: http://{address[0]}:{address[1]}")
    ThreadingHTTPServer(address, Handler).serve_forever()


if __name__ == "__main__":
    main()
