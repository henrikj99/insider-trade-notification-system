import os
import json
import logging
import smtplib
from datetime import datetime, time
from email.message import EmailMessage
from typing import Any, Dict, List

from zoneinfo import ZoneInfo

import requests
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError

# Starting the function app
app = func.FunctionApp()

LIST_URL = (
    "https://api3.oslo.oslobors.no/v1/newsreader/list"
    "?category=1102&issuer=&fromDate=&toDate=&market=&messageTitle="
)
MESSAGE_URL = (
    "https://api3.oslo.oslobors.no/v1/newsreader/message?messageId={message_id}"
)

# Headers for the API call to Oslo Børs
DEFAULT_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Origin": "https://newsweb.oslobors.no",
    "Referer": "https://newsweb.oslobors.no/",
    "User-Agent": "Mozilla/5.0",
}

# State blob location (set via env vars, with sane defaults)
STATE_CONTAINER = os.environ.get("STATE_CONTAINER", "state")
STATE_BLOB_NAME = os.environ.get("STATE_BLOB_NAME", "insider-alerts.json")

# Quiet hours (Oslo time): 00:00–05:00
OSLO_TZ = ZoneInfo("Europe/Oslo")
QUIET_START = time(0, 0)
QUIET_END = time(5, 0)


def in_quiet_hours() -> bool:
    now_local = datetime.now(OSLO_TZ).time()
    return QUIET_START <= now_local < QUIET_END


def _blob_client():
    conn_str = os.environ["BLOB_CONN_STR"]
    svc = BlobServiceClient.from_connection_string(conn_str)
    container = svc.get_container_client(STATE_CONTAINER)

    # Create container if it doesn't exist
    try:
        container.create_container()
    except ResourceExistsError:
        pass

    return container.get_blob_client(STATE_BLOB_NAME)


def load_state() -> Dict[str, Any]:
    """
    State format:
      {
        "last_processed_message_id": 663759
      }
    """
    bc = _blob_client()
    try:
        raw = bc.download_blob().readall()
        return json.loads(raw)
    except Exception:
        return {"last_processed_message_id": 0}


def save_state(state: Dict[str, Any]) -> None:
    bc = _blob_client()
    bc.upload_blob(json.dumps(state).encode("utf-8"), overwrite=True)


def fetch_list() -> List[Dict[str, Any]]:
    r = requests.post(LIST_URL, headers=DEFAULT_HEADERS, data=b"", timeout=20)
    r.raise_for_status()
    payload = r.json()
    return payload.get("data", {}).get("messages", []) or []


def fetch_message(message_id: int) -> Dict[str, Any]:
    url = MESSAGE_URL.format(message_id=message_id)
    r = requests.post(url, headers=DEFAULT_HEADERS, data=b"", timeout=20)
    r.raise_for_status()
    payload = r.json()
    return payload.get("data", {}).get("message", {}) or {}


def send_email(subject: str, body: str) -> None:
    smtp_host = os.environ["SMTP_HOST"]
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ["SMTP_USER"]
    smtp_pass = os.environ["SMTP_PASS"]
    mail_from = os.environ["MAIL_FROM"]

    mail_to = os.environ["MAIL_TO"]
    recipients = [addr.strip() for addr in mail_to.split(",") if addr.strip()]

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = mail_from
    msg["To"] = recipients
    msg.set_content(body)

    with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as s:
        s.starttls()
        s.login(smtp_user, smtp_pass)
        s.send_message(msg)


@app.timer_trigger(schedule="0 */2 * * * *", arg_name="mytimer", run_on_startup=False)
def poll_insider_trades(mytimer: func.TimerRequest) -> None:
    """
    Runs every 2 minutes by default (UTC-based trigger schedule).
    Quiet hours are enforced in code using Europe/Oslo local time.

    Cron format here is NCRONTAB:
      {sec} {min} {hour} {day} {month} {day-of-week}
    """
    if in_quiet_hours():
        logging.info("Quiet hours (00:00–05:00 Oslo). Skipping poll.")
        return

    logging.info("Poll started")

    state = load_state()
    last_id = int(state.get("last_processed_message_id", 0))
    logging.info("Last processed messageId: %s", last_id)

    messages = fetch_list()

    # Collect new messageIds
    new_items = []
    for m in messages:
        mid = int(m.get("messageId") or m.get("id") or 0)
        if mid > last_id:
            new_items.append(m)

    if not new_items:
        logging.info("No new disclosures")
        return

    # Process in ascending order so state advances correctly
    new_items.sort(key=lambda x: int(x.get("messageId") or x.get("id") or 0))

    newest_processed = last_id

    for item in new_items:
        mid = int(item.get("messageId") or item.get("id") or 0)
        title = item.get("title", "(no title)")
        issuer = item.get("issuerName", "")
        issuer_sign = item.get("issuerSign", "")

        # Fetch full message body
        full = fetch_message(mid)
        body_text = (full.get("body") or "").strip()

        email_subject = f"Insider trade alert: {issuer_sign} - {title}".strip(" -")
        email_body = (
            f"{title}\n"
            f"Issuer: {issuer} ({issuer_sign})\n"
            f"Published: {full.get('publishedTime','')}\n"
            f"MessageId: {mid}\n\n"
            f"{body_text}\n"
        )

        # Send email; only advance state if send succeeds
        send_email(email_subject, email_body)
        logging.info("Sent email for messageId %s", mid)

        newest_processed = max(newest_processed, mid)

    # Persist the newest messageId we successfully handled
    state["last_processed_message_id"] = newest_processed
    save_state(state)

    logging.info(
        "Poll finished. Updated last_processed_message_id=%s", newest_processed
    )
