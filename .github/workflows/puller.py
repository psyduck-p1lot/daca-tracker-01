"""
USCIS DACA I-821D Case Puller
------------------------------
Fetches case statuses from the USCIS internal API for a configurable
receipt number range, stores results in Supabase, and sends Telegram
alerts when any case status changes.

Environment variables required (set as GitHub Actions secrets):
  SUPABASE_URL        - your Supabase project URL
  SUPABASE_KEY        - your Supabase service_role key
  TELEGRAM_BOT_TOKEN  - your Telegram bot token (optional)
  TELEGRAM_CHAT_ID    - your Telegram chat ID (optional)
  RECEIPT_START       - e.g. IOE0934800001
  RECEIPT_END         - e.g. IOE0934899999
  REQUEST_DELAY_MS    - delay between requests in ms (default: 1500)
"""

import os
import re
import sys
import time
import json
import logging
import requests
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────────────────

SUPABASE_URL       = os.environ["SUPABASE_URL"]
SUPABASE_KEY       = os.environ["SUPABASE_KEY"]
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")
RECEIPT_START      = os.environ.get("RECEIPT_START", "IOE0934847100")
RECEIPT_END        = os.environ.get("RECEIPT_END",   "IOE0934847300")
REQUEST_DELAY_MS   = int(os.environ.get("REQUEST_DELAY_MS", "1500"))

USCIS_AUTH_URL     = "https://egov.uscis.gov/csol-api/ui-auth"
USCIS_CASE_URL     = "https://egov.uscis.gov/csol-api/case-statuses/{receipt}"

SUPABASE_HEADERS = {
    "apikey":        SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type":  "application/json",
    "Prefer":        "return=minimal",
}

# ── Receipt number helpers ───────────────────────────────────────────────────

def parse_receipt(receipt: str) -> dict:
    """Decode IOE receipt number into components."""
    m = re.match(r"^IOE(\d{2})(\d{3})(\d{5})$", receipt.upper())
    if not m:
        return {}
    fy, workday, seq = m.group(1), m.group(2), m.group(3)
    return {
        "prefix":  "IOE",
        "fy":      int(fy),
        "workday": int(workday),
        "seq_num": int(seq),
    }

def receipt_range(start: str, end: str):
    """Yield all receipt numbers between start and end inclusive."""
    prefix = start[:3]
    s = int(start[3:])
    e = int(end[3:])
    if s > e:
        raise ValueError(f"RECEIPT_START {start} is after RECEIPT_END {end}")
    for n in range(s, e + 1):
        yield f"{prefix}{str(n).zfill(10)}"

def infer_form_type(desc: str) -> str:
    """Infer form type from status description text."""
    desc_lower = (desc or "").lower()
    if "i-821" in desc_lower or "deferred action" in desc_lower or "daca" in desc_lower:
        return "I-821D"
    if "i-765" in desc_lower or "employment authorization" in desc_lower:
        return "I-765"
    if "i-131" in desc_lower or "travel document" in desc_lower:
        return "I-131"
    return "Unknown"

# ── USCIS API ────────────────────────────────────────────────────────────────

def get_uscis_token(session: requests.Session) -> str:
    """Fetch a short-lived JWT from the USCIS auth endpoint."""
    resp = session.get(USCIS_AUTH_URL, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    token = (
        data.get("JwtResponse", {}).get("accessToken")
        or data.get("accessToken")
    )
    if not token:
        raise RuntimeError(f"Could not parse token from: {data}")
    log.info("USCIS token acquired")
    return token

def fetch_case(session: requests.Session, receipt: str, token: str) -> dict | None:
    """
    Fetch a single case status. Returns normalized dict or None on error.
    Refreshes token automatically on 401.
    """
    url = USCIS_CASE_URL.format(receipt=receipt)
    headers = {
        "accept":        "*/*",
        "authorization": f"Bearer {token}",
        "content-type":  "application/json",
    }
    try:
        resp = session.get(url, headers=headers, timeout=15)
        if resp.status_code == 404:
            log.debug(f"{receipt} — 404 not found, skipping")
            return None
        resp.raise_for_status()
        data = resp.json()
        csr = data.get("CaseStatusResponse", {})
        details = csr.get("detailsEng", {})
        if not details:
            return None

        raw_form = csr.get("formType", "")
        desc     = details.get("actionCodeDesc", "")
        form     = raw_form or infer_form_type(desc)
        parsed   = parse_receipt(receipt)

        return {
            "receipt":     receipt,
            "form_type":   form,
            "status_text": details.get("actionCodeText", ""),
            "status_desc": desc,
            "status_date": details.get("enterDate", ""),
            "fy":          parsed.get("fy"),
            "workday":     parsed.get("workday"),
            "seq_num":     parsed.get("seq_num"),
            "pulled_at":   datetime.now(timezone.utc).isoformat(),
            "source_repo": os.environ.get("SOURCE_REPO", "unknown"),
        }
    except requests.HTTPError as e:
        log.warning(f"{receipt} — HTTP {e.response.status_code}")
        return None
    except Exception as e:
        log.warning(f"{receipt} — error: {e}")
        return None

# ── Supabase helpers ─────────────────────────────────────────────────────────

def get_last_status(receipt: str) -> str | None:
    """Fetch the most recent status_text for a receipt from Supabase."""
    url = (
        f"{SUPABASE_URL}/rest/v1/case_snapshots"
        f"?receipt=eq.{receipt}&order=pulled_at.desc&limit=1&select=status_text"
    )
    resp = requests.get(url, headers=SUPABASE_HEADERS, timeout=10)
    resp.raise_for_status()
    rows = resp.json()
    return rows[0]["status_text"] if rows else None

def upsert_snapshot(row: dict) -> None:
    """Insert a new snapshot row into Supabase."""
    url = f"{SUPABASE_URL}/rest/v1/case_snapshots"
    resp = requests.post(url, headers=SUPABASE_HEADERS, json=row, timeout=10)
    resp.raise_for_status()

# ── Telegram alerts ──────────────────────────────────────────────────────────

def send_telegram(message: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id":    TELEGRAM_CHAT_ID,
        "text":       message,
        "parse_mode": "Markdown",
    }
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        log.warning(f"Telegram send failed: {e}")

# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    log.info(f"Starting pull: {RECEIPT_START} → {RECEIPT_END}")
    log.info(f"Delay between requests: {REQUEST_DELAY_MS}ms")

    receipts    = list(receipt_range(RECEIPT_START, RECEIPT_END))
    total       = len(receipts)
    checked     = 0
    changed     = 0
    errors      = 0
    changes     = []

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    token = get_uscis_token(session)

    for i, receipt in enumerate(receipts):
        log.info(f"[{i+1}/{total}] Checking {receipt}")

        result = fetch_case(session, receipt, token)

        # Token expired — refresh and retry once
        if result is None and i > 0 and i % 200 == 0:
            log.info("Refreshing USCIS token...")
            try:
                token = get_uscis_token(session)
            except Exception as e:
                log.error(f"Token refresh failed: {e}")

        if result is None:
            errors += 1
        else:
            # Check for status change
            prev_status = get_last_status(receipt)
            status_changed = (
                prev_status is not None and
                prev_status != result["status_text"]
            )
            result["changed"] = status_changed

            if status_changed:
                changed += 1
                msg = (
                    f"🔔 *USCIS Status Change*\n"
                    f"Receipt: `{receipt}`\n"
                    f"Form: {result['form_type']}\n"
                    f"Was: _{prev_status}_\n"
                    f"Now: *{result['status_text']}*"
                )
                changes.append(msg)
                send_telegram(msg)
                log.info(f"STATUS CHANGE on {receipt}: {prev_status} → {result['status_text']}")

            upsert_snapshot(result)
            checked += 1

        time.sleep(REQUEST_DELAY_MS / 1000)

    # Summary
    summary = (
        f"✅ *Pull Complete*\n"
        f"Range: `{RECEIPT_START}` → `{RECEIPT_END}`\n"
        f"Checked: {checked} | Changed: {changed} | Errors: {errors}"
    )
    send_telegram(summary)
    log.info(f"Done. Checked={checked}, Changed={changed}, Errors={errors}")

    # Exit non-zero if too many errors (>50%) so GitHub Actions flags it
    if errors > total * 0.5:
        sys.exit(1)

if __name__ == "__main__":
    main()
