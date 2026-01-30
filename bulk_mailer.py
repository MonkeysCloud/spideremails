#!/usr/bin/env python3
"""
bulk_mailer.py – low-volume, low-noise outreach via Monkeysmail

Setup
=====
  export MONKEYSMAIL_API_KEY="your-api-key"
  python bulk_mailer.py emails.csv
"""

import csv
import os
import random
import sys
import time
import ssl
from collections import defaultdict
from datetime import datetime
from typing import List

import requests
import certifi
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context

# ───────────────────────────────────
# TLS adapter: TLS-1.2 only + no socket reuse
# ───────────────────────────────────
class TLS12CloseAdapter(HTTPAdapter):
    """Force TLS 1.2 and close the connection after each request."""

    def init_poolmanager(self, connections, maxsize, block=False, **kwargs):
        ctx = create_urllib3_context()
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.maximum_version = ssl.TLSVersion.TLSv1_2
        kwargs["ssl_context"] = ctx
        return super().init_poolmanager(connections, 1, block, **kwargs)

    def add_headers(self, request, **kwargs):
        super().add_headers(request, **kwargs)
        request.headers["Connection"] = "close"


# shared session with pinned CA bundle
session = requests.Session()
session.verify = certifi.where()
session.mount("https://", TLS12CloseAdapter(pool_connections=1, pool_maxsize=1))

# ───────────────────────────────────
# Tunables
# ───────────────────────────────────
BATCH_SIZE      = 1          # one recipient until domain warms up
SLEEP_BETWEEN   = 10         # seconds between calls
MAX_PER_WINDOW      = 100         # stop-after count
WINDOW_PAUSE        = 65 * 60     # 65 minutes in seconds

# Monkeysmail configuration
MONKEYSMAIL_API_BASE = "https://smtp.monkeysmail.com"
FROM_EMAIL = "invest@colibriv.com"
FROM_NAME = "ColibriV"
REPLY_TO = "jorge@colibriv.com"
SUBJECT_POOL = ["ColibriV — hydrogen turbofan propulsion (pre-seed / seed discussion)"]

# CAN-SPAM Compliance
COMPANY_ADDRESS = "6312 South Fiddlers Green Circle, Greenwood Village, CO 80111"
UNSUBSCRIBE_EMAIL = "unsubscribe@colibriv.com"

CSV_FAILURES_FILE = "monkeysmail_failed.csv"

# ───────────────────────────────────
# Helpers
# ───────────────────────────────────

def load_addresses(csv_path: str) -> list[str]:
    seen, keep = set(), []
    with open(csv_path, newline="") as fh:
        for row in csv.DictReader(fh):
            em = row.get("email", "").strip().lower()
            if em and "@" in em and em not in seen:
                seen.add(em)
                keep.append(em)
    return keep


def chunk(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def render_html(to_addr: str) -> str:
    name = to_addr.split("@")[0].replace(".", " ").replace("_", " ").title()
    return f"""
    <!DOCTYPE html>
    <html>
      <body style="font-family:Arial,Helvetica,sans-serif;line-height:1.45;color:#222">
        <p style="margin:0 0 1em 0;">Hi {name},</p>

        <p style="margin:0 0 1em 0;">
          I'm Jorge Peraza, founder of ColibriV. We're developing hydrogen-combustion turbofan propulsion as a practical, certification-first path to zero-carbon commercial aviation.
        </p>

        <p style="margin:0 0 1em 0;">
          Our approach avoids cryogenic LH₂ and fuel-cell complexity by using compressed gaseous hydrogen, with a disciplined roadmap of <strong>engines first → aircraft next</strong>, aligned with FAA pathways (Denver-based leadership; testing in Guanacaste).
        </p>

        <p style="margin:0 0 0.5em 0;">We're currently exploring:</p>
        <ul style="margin:0 0 1em 1.2em; padding:0;">
          <li>Pre-Seed / Seed funding to execute core propulsion milestones, or</li>
          <li>A $100k–$300k bridge round ahead of our U.S. Regulation Crowdfunding raise; we've already been accepted by StartEngine.</li>
        </ul>

        <p style="margin:0 0 0.5em 0;">Near-term deliverables include:</p>
        <ul style="margin:0 0 1em 1.2em; padding:0;">
          <li>Pressurized single-sector hot-fire tests</li>
          <li>Initial NOx &amp; combustion stability maps</li>
          <li>HAZOP/FMEA v1, PRD &amp; ventilation strategy</li>
          <li>Complete safety &amp; certification plan (ARP/DO)</li>
          <li>Supplier MoUs (tanks, valves, hydrogen partner)</li>
          <li>Data Pack v1 (plots + test logs, under NDA)</li>
        </ul>

        <p style="margin:0 0 0.5em 0;">If helpful, you're welcome to review:</p>
        <ul style="margin:0 0 1em 1.2em; padding:0;">
          <li><strong>Investment deck:</strong> <a href="https://drive.google.com/file/d/17Y8MmCJ38wxAJn_ZgrsFKCiq7rrx07xz/view?usp=sharing" style="color:#0066cc;">View on Google Drive</a></li>
          <li><strong>Business Plan:</strong> <a href="https://drive.google.com/file/d/15F371Up_ncpXoWTwGPAH8IU_E5pF4R8K/view" style="color:#0066cc;">View on Google Drive</a></li>
          <li><strong>Project overview:</strong> <a href="https://colibriv.com" style="color:#0066cc;">colibriv.com</a></li>
        </ul>

        <p style="margin:0 0 1.5em 0;">
          If this aligns with your focus, I'd appreciate the opportunity for a short introductory call.
        </p>

        <p style="margin:0 0 0.2em 0;">Best regards,</p>
        <p style="margin:0;">
          Jorge Peraza<br>
          Founder, ColibriV<br>
          <a href="mailto:jorge@colibriv.com" style="color:#0066cc;">jorge@colibriv.com</a> | <a href="https://colibriv.com" style="color:#0066cc;">colibriv.com</a>
        </p>

        <hr style="margin:2em 0 1em 0;border:none;border-top:1px solid #e0e0e0;">
        <p style="margin:0;font-size:11px;color:#666;">
          ColibriV · {COMPANY_ADDRESS}<br>
          You received this because we believe our mission aligns with your investment focus.<br>
          <a href="mailto:{UNSUBSCRIBE_EMAIL}?subject=Unsubscribe&body=Please%20remove%20me%20from%20your%20mailing%20list." style="color:#666;">Unsubscribe</a>
        </p>
      </body>
    </html>
    """


def render_text(to_addr: str) -> str:
    """Plain text version of the email."""
    name = to_addr.split("@")[0].replace(".", " ").replace("_", " ").title()
    return f"""Hi {name},

I'm Jorge Peraza, founder of ColibriV. We're developing hydrogen-combustion turbofan propulsion as a practical, certification-first path to zero-carbon commercial aviation.

Our approach avoids cryogenic LH₂ and fuel-cell complexity by using compressed gaseous hydrogen, with a disciplined roadmap of engines first → aircraft next, aligned with FAA pathways (Denver-based leadership; testing in Guanacaste).

We're currently exploring:
• Pre-Seed / Seed funding to execute core propulsion milestones, or
• a $100k–$300k bridge to close supplier MoUs and finalize safety & test readiness

Near-term deliverables include:
• Pressurized single-sector hot-fire tests
• Initial NOx & combustion stability maps
• HAZOP/FMEA v1, PRD & ventilation strategy
• Complete safety & certification plan (ARP/DO)
• Supplier MoUs (tanks, valves, hydrogen partner)
• Data Pack v1 (plots + test logs, under NDA)

If helpful, you're welcome to review:
• Investment deck: https://drive.google.com/file/d/17Y8MmCJ38wxAJn_ZgrsFKCiq7rrx07xz/view?usp=sharing
• Business Plan: https://drive.google.com/file/d/15F371Up_ncpXoWTwGPAH8IU_E5pF4R8K/view
• Project overview: https://colibriv.com

If this aligns with your focus, I'd appreciate the opportunity for a short introductory call.

Best regards,
Jorge Peraza
Founder, ColibriV
jorge@colibriv.com | colibriv.com

---
ColibriV · {COMPANY_ADDRESS}
You received this because we believe our mission aligns with your investment focus.
To unsubscribe, reply to {UNSUBSCRIBE_EMAIL} with "Unsubscribe" in the subject.
"""


def send_via_monkeysmail(api_key: str, subject: str, recipient: str, html_body: str, text_body: str) -> tuple[int, list[str]]:
    """Send email via Monkeysmail API."""
    url = f"{MONKEYSMAIL_API_BASE}/messages/send"
    
    payload = {
        "from": {"email": FROM_EMAIL, "name": FROM_NAME},
        "to": [recipient],
        "subject": subject,
        "html": html_body,
        "text": text_body,
        "reply_to": REPLY_TO,
        "tags": ["outreach", "colibriv", "investor"],
        # CAN-SPAM compliance headers
        "headers": {
            "List-Unsubscribe": f"<mailto:{UNSUBSCRIBE_EMAIL}?subject=Unsubscribe>",
            "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
        },
    }
    
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": api_key,
    }
    
    try:
        resp = session.post(url, json=payload, headers=headers, timeout=30)
        
        # Accept 200, 201, 202 as success (different APIs return different codes)
        if resp.status_code in (200, 201, 202):
            print(f"✓ Sent to {recipient} (status: {resp.status_code})")
            return 1, []
        else:
            print(f"[WARN] Monkeysmail {resp.status_code}: {resp.text}", file=sys.stderr)
            return 0, [recipient]
    except Exception as e:
        print(f"[ERROR] Failed to send to {recipient}: {e}", file=sys.stderr)
        return 0, [recipient]


# Track sent emails to prevent duplicates
SENT_LOG_FILE = "sent_emails.log"

def load_sent_emails() -> set[str]:
    """Load already sent emails to prevent duplicates."""
    if not os.path.exists(SENT_LOG_FILE):
        return set()
    with open(SENT_LOG_FILE, "r") as f:
        return set(line.strip().lower() for line in f if line.strip())

def log_sent_email(email: str):
    """Log an email as sent."""
    with open(SENT_LOG_FILE, "a") as f:
        f.write(f"{email.lower()}\n")


# ───────────────────────────────────
# Main
# ───────────────────────────────────

def main(csv_in: str):
    api_key = os.getenv("MONKEYSMAIL_API_KEY")
    if not api_key:
        sys.exit("Set MONKEYSMAIL_API_KEY env var first.")
    
    print("Using Monkeysmail API")
    print(f"From: {FROM_NAME} <{FROM_EMAIL}>")
    
    # Load already sent emails to prevent duplicates
    already_sent = load_sent_emails()
    if already_sent:
        print(f"⚠️  Found {len(already_sent)} already sent emails in {SENT_LOG_FILE}")
    
    recipients = load_addresses(csv_in)
    # Filter out already sent
    recipients = [r for r in recipients if r.lower() not in already_sent]
    print(f"Loaded {len(recipients)} new addresses to send")
    
    if not recipients:
        print("No new recipients to send to. Exiting.")
        return

    all_failures, sent_total = [], 0
    sent_in_window, window_start = 0, time.time()

    for i, batch in enumerate(chunk(recipients, BATCH_SIZE), 1):
        subject = random.choice(SUBJECT_POOL)
        recipient = batch[0]
        html = render_html(recipient)
        text = render_text(recipient)
        
        ok, bad = send_via_monkeysmail(api_key, subject, recipient, html, text)
        
        # Log successful send to prevent duplicates on restart
        if ok > 0:
            log_sent_email(recipient)
        
        sent_total += ok
        sent_in_window += ok
        all_failures.extend(bad)

        more_left = i * BATCH_SIZE < len(recipients)
        if not more_left:
            break

        # ——— rate limiting ———
        if sent_in_window >= MAX_PER_WINDOW:
            elapsed = time.time() - window_start
            sleep_for = max(0, WINDOW_PAUSE - elapsed)
            print(f"💤 Sent {sent_in_window} in "
                  f"{elapsed/60:.1f} min → sleeping {sleep_for/60:.1f} min")
            time.sleep(sleep_for)
            window_start = time.time()
            sent_in_window = 0
        else:
            time.sleep(SLEEP_BETWEEN)
    
    if all_failures:
        with open(CSV_FAILURES_FILE, "w", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(["email"])
            for email in all_failures:
                writer.writerow([email])
        print(f"❗  {len(all_failures)} failures written to {CSV_FAILURES_FILE}")
    
    print(f"✔  Finished — total delivered {sent_total}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Usage: python bulk_mailer.py emails.csv")
    main(sys.argv[1])