import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as pw:
        try:
            print("Connecting to CDP...")
            browser = await pw.chromium.connect_over_cdp("http://localhost:9222")
            context = browser.contexts[0]
            print(f"Connected. Found {len(context.pages)} pages.")
            
            for i, page in enumerate(context.pages):
                title = await page.title()
                url = page.url
                print(f"Page {i}: Title='{title}', URL='{url}'")
                
                # Save content of the first page (likely the active one)
                if i == 0:
                    content = await page.content()
                    with open("debug_cdp.html", "w") as f:
                        f.write(content)
                    print("Saved debug_cdp.html")
                
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
