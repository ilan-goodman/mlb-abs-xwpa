from __future__ import annotations

import base64
import json
import os
import smtplib
import ssl
import urllib.request
from email.message import EmailMessage
from pathlib import Path

from .models import Article, Subscriber, Team


def build_message(
    sender: str,
    subscriber: Subscriber,
    team: Team,
    subject: str,
    text_body: str,
    html_body: str,
    articles: list[Article],
) -> EmailMessage:
    message = EmailMessage()
    message["From"] = sender
    message["To"] = subscriber.email
    message["Subject"] = subject
    message.set_content(text_body)
    message.add_alternative(html_body, subtype="html")
    for article in articles:
        if not article.pdf_path:
            continue
        payload = Path(article.pdf_path).read_bytes()
        filename = Path(article.pdf_path).name
        message.add_attachment(payload, maintype="application", subtype="pdf", filename=filename)
    return message


def send_or_save(
    subscriber: Subscriber,
    team: Team,
    subject: str,
    text_body: str,
    html_body: str,
    articles: list[Article],
    output_dir: Path,
    send: bool = False,
) -> Path | None:
    sender = os.environ.get("MORNING_LINEUP_FROM_EMAIL", "Morning Lineup <baseball@example.com>")
    if os.environ.get("RESEND_API_KEY") and send:
        send_resend(sender, subscriber, subject, text_body, html_body, articles)
        return None
    message = build_message(sender, subscriber, team, subject, text_body, html_body, articles)
    if send and os.environ.get("SMTP_HOST"):
        send_smtp(message)
        return None
    output_dir.mkdir(parents=True, exist_ok=True)
    eml_path = output_dir / f"{subscriber.email.replace('@', '_at_')}.eml"
    eml_path.write_bytes(bytes(message))
    return eml_path


def send_smtp(message: EmailMessage) -> None:
    host = os.environ["SMTP_HOST"]
    port = int(os.environ.get("SMTP_PORT", "587"))
    username = os.environ.get("SMTP_USERNAME", "")
    password = os.environ.get("SMTP_PASSWORD", "")
    context = ssl.create_default_context()
    with smtplib.SMTP(host, port) as smtp:
        smtp.starttls(context=context)
        if username:
            smtp.login(username, password)
        smtp.send_message(message)


def send_resend(
    sender: str,
    subscriber: Subscriber,
    subject: str,
    text_body: str,
    html_body: str,
    articles: list[Article],
) -> None:
    attachments = []
    for article in articles:
        if not article.pdf_path:
            continue
        path = Path(article.pdf_path)
        attachments.append(
            {
                "filename": path.name,
                "content": base64.b64encode(path.read_bytes()).decode("ascii"),
            }
        )
    payload = {
        "from": sender,
        "to": [subscriber.email],
        "subject": subject,
        "text": text_body,
        "html": html_body,
        "attachments": attachments,
    }
    request = urllib.request.Request(
        "https://api.resend.com/emails",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {os.environ['RESEND_API_KEY']}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        if response.status >= 300:
            raise RuntimeError(f"Resend returned HTTP {response.status}")
