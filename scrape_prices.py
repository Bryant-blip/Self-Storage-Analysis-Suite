#!/usr/bin/env python3
"""
Scrape storage facility pricing from JavaScript-rendered websites using Playwright.
"""
import sys
import asyncio
from playwright.async_api import async_playwright

async def scrape_url(url):
    """Scrape a URL and return visible text."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        try:
            await page.goto(url, wait_until='networkidle')

            # Get all text content
            all_text = await page.evaluate('() => document.body.innerText')
            print(all_text)
        except Exception as e:
            print(f"Error scraping {url}: {e}", file=sys.stderr)
        finally:
            await browser.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scrape_prices.py <url>")
        sys.exit(1)

    url = sys.argv[1]
    asyncio.run(scrape_url(url))
