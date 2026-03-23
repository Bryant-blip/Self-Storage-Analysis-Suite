#!/usr/bin/env python3
"""
Scrape a webpage using Firecrawl API and return clean Markdown.

Firecrawl renders JavaScript server-side and converts the page to
structured Markdown — much easier for AI agents to parse than raw HTML.

Usage:
    python firecrawl_scrape.py <url>

Requires FIRECRAWL_API_KEY environment variable.
"""

import os
import sys
from firecrawl import FirecrawlApp


def scrape(url: str) -> str:
    api_key = os.environ.get("FIRECRAWL_API_KEY")
    if not api_key:
        print("ERROR: FIRECRAWL_API_KEY environment variable not set.", file=sys.stderr)
        sys.exit(1)

    app = FirecrawlApp(api_key=api_key)
    result = app.scrape(url, formats=["markdown"])

    if result and hasattr(result, "markdown") and result.markdown:
        return result.markdown
    elif result and hasattr(result, "content") and result.content:
        return result.content
    else:
        return "SCRAPE_ERROR: No content returned by Firecrawl."


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python firecrawl_scrape.py <url>")
        sys.exit(1)

    url = sys.argv[1]

    try:
        content = scrape(url)
        print(content)
    except Exception as e:
        print(f"SCRAPE_ERROR: {e}", file=sys.stderr)
        sys.exit(1)
