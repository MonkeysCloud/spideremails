import asyncio
import logging
import re
import time
from urllib.parse import urlparse, urljoin

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup, SoupStrainer
from playwright.async_api import async_playwright, Error as PWError
from tqdm.asyncio import tqdm

# ─────────────────────────────────────────
#  Common aliases we accept if found in text
# ─────────────────────────────────────────
COMMON_ALIASES = [
    "info", "contact", "hello", "hi", "team", "office", "admin", "support",
    "help", "inquiries", "enquiries", "mail", "mailbox", "mailroom",
    "ceo", "founder", "founders", "partners", "partner", "managingpartner",
    "invest", "investor", "investors", "venture", "capital", "fund", "funds",
    "fundraising", "lp", "dealflow", "sales", "bizdev", "business",
    "partnerships", "partnership", "outreach", "marketing", "press", "media",
    "pr", "comms", "careers", "jobs", "hr", "talent", "people", "recruiting",
    "ops", "operations", "services", "service", "webmaster", "postmaster",
]

# ─────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────
OUT_CSV            = "vc_emails.csv"
CONCURRENCY        = 20                                   # crawl more domains in parallel
TIMEOUT            = aiohttp.ClientTimeout(connect=10, sock_read=10, total=15)
MAX_DEPTH          = 3                                    # avoid deep blog archives
MAX_PAGES_PER_SITE = 50                                   # per-site crawl budget
NAV_TIMEOUT        = 10_000
RETRIES            = 3
SITE_TIMEOUT       = 240                                  # hard cap per domain (seconds)

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────
#  Email extraction utilities
# ─────────────────────────────────────────
MAILTO_RE = re.compile(
    r'href=["\']mailto:([A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,})',
    re.I,
)

EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)

OBFUSCATED_RE = re.compile(
    r"""
    (?P<local>[A-Z0-9._%+-]+)
    \s*(?:\(|\[|\{|\s)?\s*
    (?:@|&\#64;|&commat;|\s+at\s+|\s*\[at\]\s*)
    \s*(?:\)|\]|\}|\s)?\s*
    (?P<host>[A-Z0-9.-]+
        \s*(?:\.|dot|\[dot\]|\s+dot\s+)\s*
        [A-Z]{2,})
    """,
    re.I | re.X,
)

def _norm_obfus(m: re.Match) -> str:
    local = m.group("local")
    host  = re.sub(r"\s*(?:dot|\[dot\]|\s+dot\s+)\s*", ".", m.group("host"), flags=re.I)
    return f"{local}@{host}"

def extract_emails(html: str, host: str) -> set[str]:
    """Return addresses according to the 3-step rule."""
    # 1. explicit mailto links
    mailtos = set(MAILTO_RE.findall(html))
    if mailtos:
        return mailtos

    # 2. plain / obfuscated → keep only addresses with a common alias
    raw  = set(EMAIL_RE.findall(html))
    raw |= {_norm_obfus(m) for m in OBFUSCATED_RE.finditer(html)}

    return {
        e for e in raw
        if e.split("@", 1)[0].lower() in COMMON_ALIASES and e.lower().endswith(host)
    }

# ─────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────

def domain(u: str) -> str:
    return urlparse(u).netloc.lower().removeprefix("www.")

async def fetch(sess: aiohttp.ClientSession, url: str) -> str:
    try:
        async with sess.get(url, ssl=False) as r:
            return await r.text()
    except Exception as e:
        log.debug(f"fetch {url} → {e}")
        return ""

async def safe_goto(page, url: str, *, retries: int = RETRIES) -> bool:
    for attempt in range(1, retries + 1):
        try:
            await page.goto(url, timeout=NAV_TIMEOUT)
            return True
        except PWError as e:
            if attempt == 1:
                log.warning(f"goto error ({url}): {e}")
            await asyncio.sleep(2 ** attempt)
    return False

# ─────────────────────────────────────────
# 1. Gilion
# ─────────────────────────────────────────
GILION_START = "https://vc-mapping.gilion.com/venture-capital-firms/saas-investors"

async def gilion_sites(pw) -> list[str]:
    log.info("Gilion: scraping list…")
    browser = await pw.chromium.launch(headless=True)
    page    = await browser.new_page()
    await page.goto(GILION_START, timeout=0)

    while await page.is_visible('a.next.jetboost-pagination-next-1lee'):
        await page.click('a.next.jetboost-pagination-next-1lee')
        await page.wait_for_timeout(700)

    details = await page.eval_on_selector_all(
        'a[href^="/vc-firms/"]', 'els=>[...new Set(els.map(e=>e.href))]'
    )
    log.info(f"Gilion: {len(details)} firm detail pages")

    sites: list[str] = []
    for link in tqdm(details, desc="Gilion details"):
        if not await safe_goto(page, link):
            continue
        btn = await page.query_selector('a:has-text("Website")')
        if btn:
            href = await btn.get_attribute("href")
            if href and href.startswith("http"):
                sites.append(href)

    await browser.close()
    log.info(f"Gilion done → {len(sites)} websites")
    return sites

# ─────────────────────────────────────────
#  Crawl + email extraction
# ─────────────────────────────────────────
async def crawl_site(sess: aiohttp.ClientSession, root: str) -> set[str]:
    seen, q, emails, pages = {root}, [(root, 0)], set(), 0
    host = domain(root)
    log.info(f"crawl -> {root}")

    while q and pages < MAX_PAGES_PER_SITE:
        url, depth = q.pop(0)
        pages += 1
        html = await fetch(sess, url)
        emails |= extract_emails(html, host)

        if depth < MAX_DEPTH:
            for a in BeautifulSoup(html, "html.parser", parse_only=SoupStrainer("a")):
                nxt = urljoin(url, (a.get("href") or "").split("#")[0])
                if nxt.startswith("http") and domain(nxt) == host and nxt not in seen:
                    seen.add(nxt)
                    q.append((nxt, depth + 1))

    log.info(f"   {root} → {len(emails)} emails ({pages} pages)")
    return emails

# ─────────────────────────────────────────
#  Scheduler wrapper around per-site crawler
# ─────────────────────────────────────────
async def hunt_emails(sites: list[str]) -> list[tuple[str, str]]:
    conn = aiohttp.TCPConnector(limit=CONCURRENCY, ssl=False)
    async with aiohttp.ClientSession(connector=conn, timeout=TIMEOUT) as sess:
        rows: list[tuple[str, str]] = []
        sem  = asyncio.Semaphore(CONCURRENCY)

        async def _one(url: str):
            try:
                async with sem:
                    ems = await asyncio.wait_for(crawl_site(sess, url), timeout=SITE_TIMEOUT)
            except (asyncio.TimeoutError, asyncio.CancelledError) as e:
                log.warning(f"{url} skipped → {e}")
                ems = set()
            rows.extend([(url, e) for e in ems or [""]])

        # run concurrently; safeguard so one failure doesn't cancel the whole batch
        await asyncio.gather(*(_one(s) for s in sites), return_exceptions=True)
        return rows

# ─────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────
async def main() -> None:
    t0 = time.time()
    async with async_playwright() as pw:
        gil = await gilion_sites(pw)

    all_sites = [s.rstrip("/") for s in gil]
    log.info(f"Total unique firm domains to crawl: {len(all_sites)}")

    rows = await hunt_emails(all_sites)
    df   = pd.DataFrame(rows, columns=["website", "email"]).drop_duplicates()
    df.to_csv(OUT_CSV, index=False)

    log.info(f"✔ Wrote {len(df)} rows to {OUT_CSV} in {(time.time() - t0):.1f}s")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.warning("✋ Interrupted by user — partial CSV (if any) is intact.")
