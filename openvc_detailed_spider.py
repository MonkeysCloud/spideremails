import asyncio
import logging
import time
import random
import os
import aiohttp
import pandas as pd
from playwright.async_api import async_playwright

# ─────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────
OPENVC_START_URL = "https://www.openvc.app/search"
OUTPUT_CSV = "openvc_detailed_results.csv"
LOGOS_DIR = "logos"
MAX_PAGES = 900 # Adjust as needed

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

async def download_logo(page, url, fund_name):
    """Downloads the logo image and saves it locally using the browser context."""
    if not url:
        return None
    
    # Handle relative URLs if necessary (though usually they are fully qualified or absolute path from root)
    if url.startswith('./'):
        url = "https://www.openvc.app" + url[1:]
    elif url.startswith('/'):
        url = "https://www.openvc.app" + url
        
    log.info(f"Attempting to download logo for {fund_name} from {url}")

    try:
        # Create a safe filename
        safe_name = "".join([c for c in fund_name if c.isalpha() or c.isdigit() or c==' ']).strip().replace(" ", "_")
        ext = url.split('.')[-1]
        if len(ext) > 4 or '/' in ext: # fallback extension
            ext = 'jpg'
        
        filename = f"{safe_name}.{ext}"
        filepath = os.path.join(LOGOS_DIR, filename)
        
        # Don't re-download if exists
        if os.path.exists(filepath):
            log.info(f"Logo already exists: {filepath}")
            return filepath
            
        # Use page context to get proper cookies/headers
        response = await page.context.request.get(url)
        if response.status == 200:
            data = await response.body()
            with open(filepath, 'wb') as f:
                f.write(data)
            log.info(f"Downloaded logo to {filepath}")
            return filepath
        else:
            log.warning(f"Failed to download logo: status {response.status}")

    except Exception as e:
        log.warning(f"Failed to download logo for {fund_name}: {e}")
    return None

async def extract_modal_data(page):
    """Extracts all details from the currently open modal."""
    data = {}
    
    try:
        # 1. Header Info
        header_sel = '#fundHeader'
        await page.wait_for_selector(header_sel, timeout=5000)
        
        # Fund Name
        name_el = await page.query_selector(f'{header_sel} h1')
        data['Fund Name'] = await name_el.inner_text() if name_el else "Unknown"
        
        # Verified
        verified_el = await page.query_selector(f'{header_sel} .badge-verified')
        data['Verified'] = True if verified_el else False
        
        # Logo URL
        img_el = await page.query_selector(f'{header_sel} img')
        logo_url = await img_el.get_attribute('src') if img_el else None
        data['Logo URL'] = logo_url
        
        if logo_url and data['Fund Name']:
             data['Local Logo Path'] = await download_logo(page, logo_url, data['Fund Name'])

        # Socials
        social_links = await page.query_selector_all('#socialIcons a')
        for link in social_links:
            href = await link.get_attribute('href')
            icon_cls = await link.eval_on_selector('i', 'el => el.className')
            if 'linkedin' in icon_cls:
                data['LinkedIn'] = href
            elif 'link' in icon_cls or 'fa-link' in icon_cls:
                data['Website'] = href
            elif 'twitter' in icon_cls:
                data['Twitter'] = href

        # Helper to extract key-value from tables
        async def parse_table(selector):
            rows = await page.query_selector_all(f'{selector} + table tr')
            for row in rows:
                cols = await row.query_selector_all('td')
                if len(cols) == 2:
                    key = await cols[0].inner_text()
                    key = key.strip()
                    
                    # Special handling based on key
                    if key in ['Funding stages', 'Target countries', 'Firm type']:
                        # Extract text from badges if present
                        badges = await cols[1].query_selector_all('.badge')
                        if badges:
                            vals = [await b.inner_text() for b in badges]
                            val = "; ".join([v.strip() for v in vals if v.strip()])
                        else:
                            val = await cols[1].inner_text()
                    
                    elif key == 'Check size':
                        # Try to get data attributes from the KEY cell
                        min_val = await cols[0].get_attribute('data-min')
                        max_val = await cols[0].get_attribute('data-max')
                        
                        if min_val:
                            data['Check Size Min'] = min_val
                        if max_val:
                            data['Check Size Max'] = max_val
                            
                        val = await cols[1].inner_text() # Keep original text too
                        
                    else:
                        val = await cols[1].inner_text()
                    
                    data[key] = val.strip()

        # 2. Overview Table
        await parse_table('#overview')

        # 3. Thesis Table
        await parse_table('#thesis')

        # 4. Team
        team_rows = await page.query_selector_all('#team + table tr')
        team_members = []
        for row in team_rows:
            # Name often in a link or just text in the first cell
            name_el = await row.query_selector('a.profileCont') 
            name = await name_el.inner_text() if name_el else ""
            
            role_el = await row.query_selector('span.font-weight-normal') # "GP/MD"
            role = await role_el.inner_text() if role_el else ""
            
            if name:
                team_members.append(f"{name} ({role})")
        
        data['Team'] = "; ".join(team_members)

    except Exception as e:
        log.error(f"Error extracting modal data: {e}")
    
    return data

async def run_spider(pw):
    log.info("Connecting to Chrome (CDP)...")
    try:
        browser = await pw.chromium.connect_over_cdp("http://localhost:9222")
        context = browser.contexts[0]
        page = context.pages[0] if context.pages else await context.new_page()
    except Exception as e:
        log.error(f"Could not connect to Chrome on port 9222. Error: {e}")
        return

    # Results list
    all_data = []
    existing_urls = set()
    
    # URL params
    START_PAGE = 1
    
    # Resume logic
    if os.path.exists(OUTPUT_CSV):
        try:
            log.info("Checking for existing data to resume...")
            existing_df = pd.read_csv(OUTPUT_CSV)
            if not existing_df.empty:
                all_data = existing_df.to_dict('records')
                # Get max page
                if 'Source Page' in existing_df.columns:
                    START_PAGE = int(existing_df['Source Page'].max())
                    log.info(f"Resuming from page {START_PAGE}")
                
                # Track existing URLs to avoid dupes
                if 'Profile URL' in existing_df.columns:
                    existing_urls = set(existing_df['Profile URL'].dropna().tolist())
                    
                log.info(f"Loaded {len(all_data)} existing funds.")
        except Exception as e:
            log.error(f"Error loading existing CSV: {e}")
    
    # Ensure we are on search page
    if "search" not in page.url:
        log.info(f"Navigating to {OPENVC_START_URL}...")
        await page.goto(OPENVC_START_URL + f"?page={START_PAGE}", timeout=60000)
    
    await page.wait_for_selector('a.VClink', timeout=60000)

    for page_num in range(START_PAGE, MAX_PAGES + 1):
        log.info(f"Processing page {page_num}...")

        # Always navigate to the correct page
        target_url = f"{OPENVC_START_URL}?page={page_num}"
        
        if f"page={page_num}" not in page.url:
            log.info(f"Navigating to {target_url}")
            await page.goto(target_url, timeout=60000)
            
            # Cloudflare / Protection Handling
            try:
                await page.wait_for_selector('a.VClink', timeout=30000)
            except Exception:
                log.warning("Timeout waiting for funds. Checking for Cloudflare/CAPTCHA...")
                # Loop while Cloudflare is present or funds are missing
                while True:
                    title = await page.title()
                    content = await page.content()
                    
                    if "Just a moment" in title or "challenge" in content.lower() or "cloudflare" in content.lower():
                        log.warning("Cloudflare detected! Please solve the CAPTCHA in the browser window.")
                        log.info("Waiting 10s...")
                        await asyncio.sleep(10)
                        
                        # Check if solved
                        try:
                            if await page.query_selector('a.VClink'):
                                log.info("CAPTCHA solved! Resuming...")
                                break
                        except:
                            pass
                    else:
                        # Maybe just a slow load or empty page?
                        if await page.query_selector('a.VClink'):
                            break
                        else:
                            log.error("No funds found and not obviously Cloudflare. Stopping.")
                            break

            await asyncio.sleep(random.uniform(5, 8))

        # Get all funding cards
        fund_links = await page.query_selector_all('td.nameCell a.VClink')
        
        # Dedupe
        unique_links = {}
        for link in fund_links:
            href = await link.get_attribute('href')
            if href and href not in unique_links:
                unique_links[href] = link
        
        cards = list(unique_links.values())
        log.info(f"Found {len(cards)} funds on page {page_num}")

        if not cards:
            log.warning("No cards found. Stopping.")
            break

        for i, card in enumerate(cards):
            # Wait Logic
            await asyncio.sleep(random.uniform(10, 15)) # 3-5x slower than naive to be safe
            
            if i > 0 and i % 10 == 0:
                log.info("Taking a break (20s)...")
                await asyncio.sleep(20)

            try:
                # Close any existing modals
                if await page.is_visible('.modal.show'):
                    await page.click('.modal.show .close')
                    await asyncio.sleep(1)

                # Open Modal
                # Remove target to open in same tab if it was a link, though VClink usually opens modal via JS
                # But let's be safe
                await card.evaluate("el => el.removeAttribute('target')")
                await card.click()

                # Wait for Modal Content
                try:
                    # Wait for header or specific modal content
                    await page.wait_for_selector('#fundHeader', timeout=5000)
                    await asyncio.sleep(1) # Let animations finish
                    
                    # Extract Data
                    fund_data = await extract_modal_data(page)
                    fund_data['Source Page'] = page_num
                    fund_data['Profile URL'] = await card.get_attribute('href')
                    
                    if fund_data['Profile URL'] in existing_urls:
                         log.info(f"Skipping {fund_data.get('Fund Name')} (Already extracted)")
                    else:
                        all_data.append(fund_data)
                        existing_urls.add(fund_data['Profile URL'])
                        log.info(f"Extracted: {fund_data.get('Fund Name', 'Unknown')}")
                    
                    # Save immediately
                    df = pd.DataFrame(all_data)
                    df.to_csv(OUTPUT_CSV, index=False)
                    
                    # Close Modal
                    await page.keyboard.press('Escape')
                    await page.wait_for_selector('.modal.show', state='hidden', timeout=3000)

                except Exception as e:
                    log.error(f"Error processing modal for card {i}: {e}")
                    # Recovery: try to close modal or go back
                    await page.keyboard.press('Escape')

            except Exception as e:
                log.error(f"Error clicking card {i}: {e}")

        # Save Checkpoint after each page (redundant now but safe)
        if all_data:
            df = pd.DataFrame(all_data)
            df.to_csv(OUTPUT_CSV, index=False)
            log.info(f"Saved {len(df)} rows to {OUTPUT_CSV}")

    await browser.close()

async def main():
    async with async_playwright() as pw:
        await run_spider(pw)

if __name__ == "__main__":
    asyncio.run(main())
