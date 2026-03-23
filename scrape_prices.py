#!/usr/bin/env python3
"""
Playwright scraper for JS-rendered storage facility websites.

Usage:
    python scrape_prices.py <url>
    python scrape_prices.py <url> --timeout 20000

Launches a headless Chromium browser, renders JavaScript, and prints
the visible page text to stdout. The agent parses stdout for pricing.
"""

import sys
import argparse
from playwright.sync_api import sync_playwright


def scrape(url: str, timeout: int = 15000) -> str:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
            locale="en-US",
            timezone_id="America/Chicago",
        )
        page = context.new_page()

        # Hide webdriver flag
        page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        # Block images/fonts/media to speed up loading
        page.route(
            "**/*.{png,jpg,jpeg,gif,svg,woff,woff2,ttf,mp4,webm,ico}",
            lambda route: route.abort(),
        )

        page.goto(url, wait_until="networkidle", timeout=timeout)

        # Wait for late-loading JS price widgets
        page.wait_for_timeout(2000)

        # Try clicking "See All Units" / "View Prices" buttons if present
        for selector in [
            "text=See All",
            "text=View All",
            "text=View Prices",
            "text=Show All",
            "text=See Units",
            "text=View Units",
        ]:
            try:
                btn = page.locator(selector).first
                if btn.is_visible(timeout=500):
                    btn.click()
                    page.wait_for_timeout(1500)
            except Exception:
                pass

        text = page.inner_text("body")
        browser.close()

    return text


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape a JS-rendered webpage and print visible text."
    )
    parser.add_argument("url", help="URL to scrape")
    parser.add_argument(
        "--timeout",
        type=int,
        default=15000,
        help="Page load timeout in ms (default: 15000)",
    )
    args = parser.parse_args()

    try:
        content = scrape(args.url, args.timeout)
        print(content)
    except Exception as e:
        print(f"SCRAPE_ERROR: {e}", file=sys.stderr)
        sys.exit(1)
