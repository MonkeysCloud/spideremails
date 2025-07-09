import asyncio, logging, re, time
from urllib.parse import urlparse, urljoin

import aiohttp, pandas as pd
from bs4 import BeautifulSoup, SoupStrainer
from tqdm.asyncio import tqdm

# ─────────────────────────────────────────
#  Common aliases we accept if found in text
# ─────────────────────────────────────────
COMMON_ALIASES = [
    # … (same list as before, trimmed for brevity)
    "info", "contact", "hello", "hi", "team", "office", "admin", "support",
    "careers", "jobs", "hr", "ops", "webmaster", "postmaster",
]

# ─────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────
OUT_CSV            = "vc_emails_vcsheet.csv"
CONCURRENCY        = 15
TIMEOUT            = aiohttp.ClientTimeout(total=25)
MAX_DEPTH          = 6
MAX_PAGES_PER_SITE = 200
RETRIES            = 3

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────
#  Email extraction utilities (unchanged)
# ─────────────────────────────────────────
MAILTO_RE = re.compile(
    r'href=["\']mailto:([A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,})', re.I
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
    mailtos = set(MAILTO_RE.findall(html))
    if mailtos:
        return mailtos
    raw  = set(EMAIL_RE.findall(html))
    raw |= {_norm_obfus(m) for m in OBFSUCATED_RE.finditer(html)}
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
        async with sess.get(url, timeout=TIMEOUT, ssl=False) as r:
            return await r.text()
    except Exception as e:
        log.debug(f"fetch {url} → {e}")
        return ""

# ─────────────────────────────────────────
# 1. VC Sheet scraper (collect external websites)
# ─────────────────────────────────────────
VCSHEET_START = "https://www.vcsheet.com"

async def vcsheet_sites(sess) -> list[str]:
    log.info("VC Sheet: scraping fund list…")
    html  = await fetch(sess, f"{VCSHEET_START}/funds")
    soup  = BeautifulSoup(html, "html.parser")
    slugs = {a["href"] for a in soup.select('a[href^="/funds/"]')}
    log.info(f"  → Found {len(slugs)} fund cards")

    sites = []
    for slug in tqdm(slugs, desc="VC Sheet details"):
        detail = await fetch(sess, urljoin(VCSHEET_START, slug))
        for a in BeautifulSoup(detail, "html.parser", parse_only=SoupStrainer("a")):
            href = a.get("href", "")
            if href.startswith("http") and "vcsheet.com" not in href:
                sites.append(href)
    log.info(f"VC Sheet done → {len(sites)} external sites")
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

async def hunt_emails(sites: list[str]) -> list[tuple[str, str]]:
    conn = aiohttp.TCPConnector(limit=CONCURRENCY, ssl=False)
    async with aiohttp.ClientSession(connector=conn, timeout=TIMEOUT) as sess:
        rows = []
        for s in tqdm(sites, desc="Crawling sites"):
            ems = await crawl_site(sess, s)
            rows += [(s, e) for e in ems or [""]]
        return rows

# ─────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────
async def main():
    t0 = time.time()
    async with aiohttp.ClientSession(timeout=TIMEOUT) as sess:
        sites = await vcsheet_sites(sess)

    sites = [s.rstrip("/") for s in sites]
    log.info(f"Total unique firm domains to crawl: {len(sites)}")

    rows = await hunt_emails(sites)
    df   = pd.DataFrame(rows, columns=["website", "email"]).drop_duplicates()
    df.to_csv(OUT_CSV, index=False)

    log.info(f"✔ Wrote {len(df)} rows to {OUT_CSV} in {(time.time() - t0):.1f}s")

if __name__ == "__main__":
    asyncio.run(main())