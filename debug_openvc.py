import asyncio
from playwright.async_api import async_playwright

URL = "https://www.openvc.app/search?s=&countries=&countries%5B%5D=USA&stages=&stages%5B%5D=1.+Idea+or+Patent&stages%5B%5D=2.+Prototype&round_size=&page=2"

async def main():
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page()
        print(f"Navigating to {URL}")
        await page.goto(URL, timeout=60000)
        
        print("Waiting for content...")
        try:
            # wait for something generic
            await page.wait_for_timeout(10000) 
            
            # Save screenshot
            await page.screenshot(path="debug_page.png")
            print("Saved debug_page.png")
            
            # Save HTML
            with open("debug_page.html", "w") as f:
                f.write(await page.content())
            print("Saved debug_page.html")
            
        except Exception as e:
            print(f"Error: {e}")
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
