#!/usr/bin/env python3
"""
bulk_mailer.py – low-volume, low-noise outreach via Mailgun

Setup
=====
  export MAILGUN_DOMAIN="mg.example.com"
  export MAILGUN_API_KEY="key-xxxxxxxxxxxxxxxx"
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
FROM_POOL       = [("Jorge Peraza", "jorge@monkeys.cloud")]
SUBJECT_POOL    = ["MonkeysCloud | Pre-Series A opportunity"]
TRACK_OPENS     = True
ATTACH_PATHS    = [          # <— attachments enabled again
    "docs/MonkeysCloud.pdf",
    "docs/MonkeysCloud-share.pdf",
]
CSV_FAILURES_FILE = "mailgun_failed.csv"

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
    name = to_addr.split("@")[0]
    return f"""
    <!DOCTYPE html>
    <html>
      <body style="font-family:Arial,Helvetica,sans-serif;line-height:1.45;color:#222">
        <p style="margin:0 0 1em 0;">Hello {name},</p>

        <p style="margin:0 0 1em 0;">
          I hope all is well. I’m writing with exciting news about the
          <strong>Monkeys ecosystem</strong> and the three phases that bring it to life:
        </p>

        <h3 style="margin:1.2em 0 0.4em 0; font-size:1.1em;">Phase 1 — MonkeysCloud (Specialised Hosting Platform) 🚀 <span style="color:#28a745;">launch-ready</span></h3>
        <p style="margin:0 0 1em 0;">
          <a href="https://monkeys.cloud/" style="color:#0066cc;">MonkeysCloud</a> is no ordinary “all-in-one” tool; it’s a purpose-built hosting and
          DevOps platform for modern web agencies and product teams. Think managed cloud
          infrastructure with smart defaults: container orchestration, automatic SSL,
          one-click rollbacks, and granular usage analytics—everything you need to ship
          and scale projects without the usual ops overhead.
        </p>

        <h3 style="margin:1.2em 0 0.4em 0; font-size:1.1em;">Phase 2 — MonkeysLegion (Open Framework &amp; Community) ⚙️ <span style="color:#f0ad4e;">open-source rollout</span></h3>
        <p style="margin:0 0 1em 0;">
          Under the hood, MonkeysCloud runs on <a href="https://monkeyslegion.com/" style="color:#0066cc;">MonkeysLegion</a>, our lightning-fast PHP framework.
          By open-sourcing it we’ll grow a vibrant developer community, spark third-party
          innovation, and ensure every future Monkeys product shares one robust codebase.
        </p>

        <h3 style="margin:1.2em 0 0.4em 0; font-size:1.1em;">Phase 3 — MonkeysCMS (AI-Powered Content Engine) 🤖 <span style="color:#d9534f;">in development</span></h3>
        <p style="margin:0 0 1em 0;">
          Built on MonkeysLegion, MonkeysCMS will let non-technical teams spin up fully
          optimised sites in minutes. Features include generative AI for copy and images,
          real-time SEO tuning, and smart layout suggestions—all deployable to
          MonkeysCloud with a single click.
        </p>

        <h3 style="margin:1.5em 0 0.4em 0;">Key Highlights</h3>
        <ul style="margin:0 0 1em 1.2em; padding:0;">
          <li><strong>Launch-Ready Hosting:</strong> production-grade CI/CD, staging-to-prod pipelines, zero-downtime deploys.</li>
          <li><strong>Strategic Partnership:</strong> $300 k in Google Cloud credits ensure rock-solid scalability from day one.</li>
          <li><strong>Funding Objectives:</strong> launch MonkeysCloud, expand marketing, harden the platform, accelerate AI in both MonkeysLegion &amp; MonkeysCMS.</li>
        </ul>

        <h3 style="margin:1.5em 0 0.4em 0;">Why Invest in Monkeys?</h3>
        <ul style="margin:0 0 1em 1.2em; padding:0;">
          <li><strong>Unified Stack:</strong> one framework powers hosting, framework, and upcoming CMS.</li>
          <li><strong>Built-in Edge:</strong> automated testing, predictive error detection, AI-generated content, instant SEO insights.</li>
          <li><strong>Clear Market Fit:</strong> agencies &amp; dev teams get a seamless path from commit → live.</li>
        </ul>

        <p style="margin:0 0 1em 0;">
          Your involvement could dramatically accelerate our roadmap and help us capture
          this rapidly expanding market. I’d be thrilled to schedule a call at your
          convenience.
        </p>

        <p style="margin:0 0 1.5em 0;">Thank you for your time and consideration—I look forward to the possibility of collaborating.</p>

        <p style="margin:0 0 0.2em 0;"><strong>Warm regards,</strong></p>
        <p style="margin:0;">
          Jorge Peraza<br>
          CEO, <a href="https://monkeys.cloud/" style="color:#0066cc;">monkeys.cloud</a><br>
          3000 Lawrence St #113&nbsp;· Denver, CO 80205-3422<br>
          <a href="tel:+17209792811" style="color:#0066cc;">+1&nbsp;720-979-2811</a>
        </p>
      </body>
    </html>
    """


def send_batch(domain: str, api_key: str, sender: tuple[str, str], subject: str, recipients: List[str], html_body: str) -> tuple[int, list[str]]:
    url = f"https://api.mailgun.net/v3/{domain}/messages"
    from_hdr = f"{sender[0]} <{sender[1]}>"
    data = {
        "from": from_hdr,
        "to": recipients,
        "subject": subject,
        "html": html_body,
        "o:tracking": "yes" if TRACK_OPENS else "no",
        "o:tag": ["outreach", "monkeyscloud"],
    }
    files = [
        ("attachment", (os.path.basename(p), open(p, "rb")))
        for p in ATTACH_PATHS if os.path.isfile(p)
    ]
    try:
        resp = session.post(url, auth=("api", api_key), data=data, files=files, timeout=30, allow_redirects=False)
    finally:
        for _, (_, fh) in files:
            fh.close()
    if resp.status_code == 200:
        return len(recipients), []
    print(f"[WARN] Mailgun {resp.status_code}: {resp.text}", file=sys.stderr)
    return 0, recipients

# ───────────────────────────────────
# Main
# ───────────────────────────────────

def main(csv_in: str):
    mg_domain = os.getenv("MAILGUN_DOMAIN")
    mg_key    = os.getenv("MAILGUN_API_KEY")
    if not (mg_domain and mg_key):
        sys.exit("Set MAILGUN_DOMAIN and MAILGUN_API_KEY env vars first.")
    print("Using Mailgun domain:", mg_domain)
    recipients = load_addresses(csv_in)
    print(f"Loaded {len(recipients)} unique addresses")

    failures, sent_total = defaultdict(list), 0
    for batch_no, batch in enumerate(chunk(recipients, BATCH_SIZE), 1):
        sender  = random.choice(FROM_POOL)
        subject = random.choice(SUBJECT_POOL)
        html    = render_html(batch[0])
        ok, bad = send_batch(mg_domain, mg_key, sender, subject, batch, html)
        sent_total += ok
        if bad:
            failures["failed"] += bad
        print(f"[{datetime.now():%H:%M:%S}] Batch {batch_no}: sent {ok}/{len(batch)}, failures {len(bad)}")
        # decide how long to wait before the *next* email
        more_left = batch_no * BATCH_SIZE < len(recipients)
        if not more_left:
            break                                 # all done

        if batch_no % MAX_PER_WINDOW == 0:        # after every 100 messages
            print(f"💤 cooling-off for {WINDOW_PAUSE/60:.0f} minutes …")
            time.sleep(WINDOW_PAUSE)
        else:
            time.sleep(SLEEP_BETWEEN)
    if failures:
        with open(CSV_FAILURES_FILE, "w", newline="") as fh:
            csv.writer(fh).writerows([[e] for e in failures["failed"]])
        print(f"❗  {len(failures['failed'])} failures written to {CSV_FAILURES_FILE}")
    print(f"✔  Finished — total delivered {sent_total}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Usage: python bulk_mailer.py emails.csv")
    main(sys.argv[1])