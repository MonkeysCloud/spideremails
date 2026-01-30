import asyncio
import logging
import aiohttp
from vc_email_scraper import crawl_site, extract_emails, fetch, domain, SITE_TIMEOUT, TIMEOUT

# Customize logging to see everything
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("debug_hunter")

async def debug_crawl(url: str):
    print(f"DEBUG: Starting crawl for {url}")
    conn = aiohttp.TCPConnector(limit=1, ssl=False)
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    async with aiohttp.ClientSession(connector=conn, timeout=TIMEOUT, headers=headers) as sess:
        # We manually run the crawl loop to print pages
        seen, q, emails, pages = {url}, [(url, 0)], set(), 0
        host = domain(url)
        MAX_PAGES_PER_SITE = 50
        MAX_DEPTH = 3

        while q and pages < MAX_PAGES_PER_SITE:
            curr_url, depth = q.pop(0)
            pages += 1
            print(f"Visiting: {curr_url} (Depth: {depth})")
            
            try:
                html = await fetch(sess, curr_url)
                if not html:
                    print(f"  -> Empty HTML or error fetching")
                    continue
                
                print(f"HTML Content Start: {html[:500]}...")

                
                # Check for specific email
                if "connect@nextplayventures.com" in html:
                     print(f"  -> FOUND 'connect@nextplayventures.com' in RAW HTML!")
                
                # Extraction
                extracted = extract_emails(html, host)
                if extracted:
                    print(f"  -> Extracted: {extracted}")
                emails |= extracted

                # Links
                from bs4 import BeautifulSoup, SoupStrainer
                from urllib.parse import urljoin
                
                if depth < MAX_DEPTH:
                    for a in BeautifulSoup(html, "html.parser", parse_only=SoupStrainer("a")):
                        href = a.get("href") or ""
                        nxt = urljoin(curr_url, href.split("#")[0])
                        
                        # Debug link finding
                        if "contact" in nxt:
                            print(f"    -> Found contact link: {nxt}")

                        if nxt.startswith("http") and domain(nxt) == host and nxt not in seen:
                            seen.add(nxt)
                            q.append((nxt, depth + 1))
                            
            except Exception as e:
                print(f"Error processing {curr_url}: {e}")

        print("\nFinal Emails Found:", emails)

if __name__ == "__main__":
    asyncio.run(debug_crawl("http://www.nextplayventures.com"))
