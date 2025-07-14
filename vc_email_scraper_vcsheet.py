#!/usr/bin/env python3
"""
vc_email_scraper_vcsheet.py  –  resilient parallel crawler
(compatible with Playwright versions < 1.44)
"""

from __future__ import annotations
import asyncio, logging, os, re, time, json
from urllib.parse import urlparse
import tldextract
import pandas as pd
from playwright.async_api import (
    async_playwright, TimeoutError as PWTimeout, Error as PWError
)
from tqdm.asyncio import tqdm

# ─────────────────────────── CONFIG ────────────────────────────
EMAIL_RE          = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
OUT_CSV           = "vc_emails_vcsheet.csv"

MAX_DEPTH         = 6            # clicks per domain
MAX_PAGES         = 40           # pages per domain
PARALLEL_SITES    = 6            # ≤ simultaneous domains
SITE_TIMEOUT      = 180          # seconds per whole domain
NAV_TIMEOUT       = 30_000       # ms for page.goto
SKIP_PAGE_BYTES   = 2_000_000    # ignore huge blobs (> ~2 MB)

SOCIAL_SKIPS = {
    "twitter.com", "x.com", "linkedin.com", "facebook.com", "medium.com",
    "instagram.com", "youtube.com", "t.me", "github.com", "vcsheet.com",
}

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ───────────────────────── helpers ──────────────────────────
def dom(url: str) -> str:
    if not isinstance(url, str) or not url.startswith(("http://", "https://")):
        return ""
    try:
        h = urlparse(url).hostname or ""
    except ValueError:
        return ""
    return h.lower().removeprefix("www.")
_extract = tldextract.TLDExtract(cache_dir=False)
def regdom(url: str) -> str:
    """Registrable domain (eTLD+1) or ''."""
    if not isinstance(url, str) or not url.startswith(("http://", "https://")):
        return ""
    try:
        ext = _extract(urlparse(url).hostname or "")
    except ValueError:
        return ""
    if ext.domain and ext.suffix:
        return f"{ext.domain}.{ext.suffix}".lower()
    return ""

def extract_emails(text: str, host_regdom: str) -> set[str]:
    return {e for e in EMAIL_RE.findall(text)
            if regdom("http://" + e.split("@", 1)[-1]) == host_regdom}

async def safe_goto(page, url: str) -> bool:
    try:
        await page.goto(url, timeout=NAV_TIMEOUT, wait_until="domcontentloaded")
        return True
    except (PWTimeout, PWError):
        log.debug(f"goto failed → {url}")
        return False

# ───────────────────── VC-Sheet helpers ─────────────────────
async def fund_links(page) -> list[str]:
    # Fast path – Next.js payload
    try:
        data = await page.evaluate("window.__NEXT_DATA__")
        funds = data["props"]["pageProps"].get("funds", [])
        if funds:
            return [f"https://www.vcsheet.com/fund/{f['slug']}" for f in funds]
    except Exception:
        pass
    # Scroll fallback
    seen: set[str] = set()
    for _ in range(40):
        seen.update(await page.eval_on_selector_all(
            'a[href^="/fund"]', 'els=>els.map(e=>e.href)'))
        if seen:
            break
        await page.mouse.wheel(0, 2800)
        await page.wait_for_timeout(600)
    return list(seen)

async def website_links_from_detail(page) -> list[str]:
    primary = await page.eval_on_selector_all(
        'a:has-text("Website"), a:has-text("Visit")',
        'els=>els.map(e=>e.href)')
    if primary:
        return primary
    hrefs = await page.eval_on_selector_all(
        'a[href^="http"]', 'els=>els.map(e=>e.href)')
    for h in hrefs:
        d = dom(h)
        if d and d not in SOCIAL_SKIPS:
            return [h]
    return []

# ───────────────────── per-domain crawl ─────────────────────
async def crawl_domain(browser, start_url: str) -> set[str]:
    host = dom(start_url)
    if not host:
        return set()

    ctx  = await browser.new_context()
    page = await ctx.new_page()
    q    = [(start_url.rstrip("/"), 0)]
    seen = {q[0][0]}
    pages = 0
    emails: set[str] = set()

    while q and pages < MAX_PAGES:
        url, depth = q.pop(0)
        pages += 1
        if not await safe_goto(page, url):
            continue

        # skip very large downloads
        try:
            size = page.response().headers.get("content-length")
            if size and int(size) > SKIP_PAGE_BYTES:
                continue
        except Exception:
            pass

        try:
            html = await page.content()
        except PWError:
            continue

        emails |= extract_emails(html, host)

        if depth >= MAX_DEPTH:
            continue

        try:
            links = await page.eval_on_selector_all(
                'a[href^="http"]', 'els=>els.map(e=>e.href)')
        except PWError:
            continue

        for l in links:
            if dom(l) == host:
                l = l.rstrip("/")
                if l not in seen:
                    seen.add(l)
                    q.append((l, depth + 1))

    await ctx.close()
    log.info(f"{host:<30} → {len(emails):2d} emails ({pages} pages)")
    return emails

# ───────────────────────────── main ─────────────────────────────
async def main():
    started = time.time()
    headless = os.getenv("HEADLESS", "1") != "0"
    slow_mo  = 200 if not headless else 0
    rows: list[tuple[str, str]] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=headless, slow_mo=slow_mo)
        page    = await browser.new_page()

        await page.goto("https://www.vcsheet.com/funds", timeout=0)
        for sel in ('input[type="email"]',
                    'text="I have early access"',
                    'text="Continue without invite"',
                    'text="Enter site"'):
            try:
                if sel.startswith("input"):
                    await page.fill(sel, "bot@example.com", timeout=2500)
                    await page.keyboard.press("Enter")
                else:
                    await page.click(sel, timeout=2500)
            except PWTimeout:
                pass

        funds = await fund_links(page)
        log.info(f"fund detail pages: {len(funds)}")

        sites = []
        for d in tqdm(funds, desc="fund pages"):
            if not await safe_goto(page, d):
                continue
            sites += [s.rstrip("/") for s in await website_links_from_detail(page)]

        sites = [s for s in dict.fromkeys(sites) if dom(s) and dom(s) not in SOCIAL_SKIPS]
        log.info(f"website targets: {len(sites)}")

        sem = asyncio.Semaphore(PARALLEL_SITES)

        async def _crawl(site):
            try:
                async with sem:
                    ems = await asyncio.wait_for(crawl_domain(browser, site),
                                                 timeout=SITE_TIMEOUT)
                return site, ems
            except (asyncio.TimeoutError, PWError):
                log.warning(f"{dom(site)} – crawl failed")
                return site, set()

        for site, emails in tqdm(await asyncio.gather(*[_crawl(s) for s in sites]),
                                 total=len(sites), desc="website crawl"):
            rows.extend((site, e) for e in (emails or {""}))

        await browser.close()

    pd.DataFrame(rows, columns=["website", "email"]).drop_duplicates()\
        .to_csv(OUT_CSV, index=False)
    log.info(f"✔ {len(rows)} rows → {OUT_CSV}  in {time.time()-started:.1f}s")

if __name__ == "__main__":
    asyncio.run(main())