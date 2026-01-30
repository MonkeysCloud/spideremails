import asyncio
import logging
import time
import random
from urllib.parse import urlparse

import pandas as pd
from playwright.async_api import async_playwright
from tqdm.asyncio import tqdm

# Reuse email hunting logic from the existing scraper
# Ensure vc_email_scraper.py is in the same directory
from vc_email_scraper import hunt_emails

# ─────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────
OPENVC_START_URL = "https://www.openvc.app/search?s=&countries=&countries%5B%5D=USA&stages=&stages%5B%5D=1.+Idea+or+Patent&stages%5B%5D=2.+Prototype&round_size="
OUTPUT_CSV = "openvc_emails.csv"
MAX_PAGES = 120  # User mentioned 119 pages, setting 120 to be safe

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

async def get_openvc_sites(pw) -> list[str]:
    """
    Navigates OpenVC search results, opens modals, and extracts VC websites.
    """
    log.info("Starting OpenVC scraper using CDP...")
    log.info("Ensure you have started Chrome with: --remote-debugging-port=9222")
    
    try:
        browser = await pw.chromium.connect_over_cdp("http://localhost:9222")
        # Use simple context from the browser
        context = browser.contexts[0]
        page = context.pages[0] if context.pages else await context.new_page()
    except Exception as e:
        log.error(f"Could not connect to Chrome on port 9222. Is it running? Error: {e}")
        return []

    # Updated to start from a specific page if needed, avoiding re-scraping page 1 immediately if blocked
    all_websites = set()
    START_PAGE = 1 
    
    try:
        # User is already manually handling the session, so we just use the current page or navigate if needed
        # We assume the user might be at the start URL or we navigate there.
        # Check current URL - must contain 'search' to be the results page
        if "search" not in page.url:
            log.info(f"Navigating to Search URL: {OPENVC_START_URL}...")
            await page.goto(OPENVC_START_URL + f"&page={START_PAGE}", timeout=60000)
        else:
            log.info("Already on OpenVC Search, continuing...")
        
        # Wait for results to load - wait for the table or VClinks
        log.info("Waiting for results...")
        await page.wait_for_selector('a.VClink', timeout=60000)

        for page_num in range(START_PAGE, MAX_PAGES + 1):
            log.info(f"Processing page {page_num}...")
            
            # Re-navigate only if not on the first page iteration or if we need to change pages
            if page_num > START_PAGE or (page_num == START_PAGE and f"page={START_PAGE}" not in page.url):
                target_url = f"{OPENVC_START_URL}&page={page_num}"
                if page.url != target_url:
                    await page.goto(target_url, timeout=60000)
                    await page.wait_for_selector('a.VClink', timeout=30000)
                    # Sleep after navigation - mimic reading the new page
                    await asyncio.sleep(random.uniform(20, 30))

            # Find all fund links (names)
            fund_links = await page.query_selector_all('td.nameCell a.VClink')
            
            # Deduplicate by href
            unique_links = {}
            for link in fund_links:
                href = await link.get_attribute('href')
                if href and href not in unique_links:
                    unique_links[href] = link
            
            cards = list(unique_links.values())
            log.info(f"Found {len(cards)} funds on page {page_num}")

            if not cards:
                log.warning(f"No funds found on page {page_num}, stopping.")
                break

            for i, card in enumerate(cards):
                # Rate limit: Sleep between cards - significant delay
                await asyncio.sleep(random.uniform(8, 15))
                
                # Take a longer break every 10 cards to look "human"
                if i > 0 and i % 10 == 0:
                    log.info("Taking a coffee break (30s)...")
                    await asyncio.sleep(30)
                
                try:
                    # Ensure no modal is blocking us
                    # Sometimes a previous modal or a "sign up" modal might be open.
                    if await page.is_visible('.modal.show'):
                        log.info("Modal blocking click, attempting to close...")
                        await page.keyboard.press('Escape')
                        await asyncio.sleep(1)
                        if await page.is_visible('.modal.show'):
                            # Try clicking the backdrop or close button
                            try:
                                await page.click('.modal.show .close', timeout=2000)
                            except:
                                # JS force close
                                await page.evaluate("document.querySelectorAll('.modal.show').forEach(m => $(m).modal('hide'))")
                        await asyncio.sleep(1)

                    # Click the name to open modal
                    # Remove target to keep in same page/modal context
                    await card.evaluate("el => el.removeAttribute('target')")
                    
                    # Scroll into view gently
                    await card.scroll_into_view_if_needed()
                    
                    # Use force click if needed, but standard click is better for triggering events
                    await card.click()
                    
                    # Wait for modal or new content
                    try:
                        await page.wait_for_selector('#socialIcons', timeout=5000)
                        
                        # Extract website
                        link_element = await page.query_selector('#socialIcons a:has(i.fa-link)')
                        if link_element:
                            href = await link_element.get_attribute('href')
                            if href:
                                log.info(f"Found website: {href}")
                                all_websites.add(href)
                                # Save immediately!
                                pd.DataFrame(list(all_websites), columns=["website"]).to_csv("openvc_websites_checkpoint.csv", index=False)
                        
                        # Close modal or go back
                        if "fund/" in page.url:
                            await page.go_back()
                            await page.wait_for_selector('a.VClink', timeout=10000)
                        else:
                            # Must be a modal
                            await page.keyboard.press('Escape')
                            await page.wait_for_selector('#socialIcons', state='hidden', timeout=3000)
                        
                    except Exception as e:
                        # Check for potential block message?
                        content = await page.content()
                        if "too fast" in content.lower() or "blocked" in content.lower():
                            log.error("Detected block message! Pausing for 5 minutes...")
                            await asyncio.sleep(300)
                        
                        log.warning(f"Error extracting from detail {i}: {e}")
                        # Recover
                        if "fund/" in page.url:
                            await page.go_back()

                except Exception as e:
                    log.error(f"Error interacting with card {i}: {e}")
            
            # Save intermediate progress
            pd.DataFrame(list(all_websites), columns=["website"]).to_csv("openvc_websites_checkpoint.csv", index=False)



            # Optional: Check if there's a next button to verify if we should stop
            # But we are iterating fixed pages as requested.
            
    except Exception as e:
        log.error(f"Global error during scraping: {e}")
    finally:
        await browser.close()

    log.info(f"Finished OpenVC scrape. Found {len(all_websites)} unique websites.")
    return list(all_websites)


async def main():
    start_time = time.time()
    
    # 1. Scrape OpenVC for websites
    async with async_playwright() as pw:
        sites = await get_openvc_sites(pw)
    
    if not sites:
        log.warning("No websites found from OpenVC. Exiting.")
        return

    # Filter out empty or invalid
    sites = [s for s in sites if s and s.startswith('http') and 'linkedin.com' not in s]
    
    # Save intermediate results
    pd.DataFrame(sites, columns=["website"]).to_csv("openvc_websites_raw.csv", index=False)
    log.info(f"Saved {len(sites)} websites to openvc_websites_raw.csv")

    log.info(f"Starting email hunt for {len(sites)} sites...")

    # 2. Hunt for emails using the logic from vc_email_scraper.py
    # This returns a list of (website, email) tuples
    results = await hunt_emails(sites)

    # 3. Save to CSV
    df = pd.DataFrame(results, columns=["Page", "Email"])
    # "Page" here effectively means the VC website URL
    
    # User asked for "name of the person or the page"
    # Our data structure is (url, email). The email usually implies the person or generic.
    # We will save what we have.
    
    df.drop_duplicates(inplace=True)
    df.to_csv(OUTPUT_CSV, index=False)
    
    duration = time.time() - start_time
    log.info(f"✔ Done! Saved {len(df)} rows to {OUTPUT_CSV} in {duration:.1f}s")


if __name__ == "__main__":
    asyncio.run(main())
