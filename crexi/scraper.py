"""
crexi/scraper.py

Firecrawl-based scraper for Crexi search results and individual listing pages.

Stage 1: scrape_search_results() — Crexi search page for a given market
Stage 2b: scrape_listing_detail() — individual Crexi listing page
"""

import re
import time
import logging
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# Markers that confirm a real Crexi search results page was returned.
# If none of these appear, we likely got a bot-detection page.
SEARCH_PAGE_MARKERS = [
    "listing",
    "properties",
    "acres",
    "for sale",
    "land",
]

# Markers that confirm an individual listing page was returned.
LISTING_PAGE_MARKERS = [
    "property details",
    "listing details",
    "asking price",
    "contact broker",
    "cap rate",
    "acreage",
    "square feet",
    "land",
]


class CrexiBlockedError(Exception):
    """Raised when Firecrawl returns a bot-detection page instead of real Crexi content."""


class CrexiRedirectError(Exception):
    """Raised when a listing URL redirects to a 'Similar Listings' aggregator page."""


# Map full state names to Crexi's 2-letter abbreviation used in URLs
STATE_ABBREVIATIONS = {
    "washington": "WA", "north carolina": "NC", "new jersey": "NJ",
    "colorado": "CO", "virginia": "VA", "arizona": "AZ", "texas": "TX",
    "florida": "FL", "georgia": "GA", "california": "CA", "nevada": "NV",
    "utah": "UT", "oregon": "OR", "idaho": "ID", "montana": "MT",
}


def _state_abbrev(market: str) -> str:
    """Convert full state name to 2-letter abbreviation, or return as-is if already short."""
    m = market.strip()
    if len(m) == 2:
        return m.upper()
    return STATE_ABBREVIATIONS.get(m.lower(), m.upper()[:2])


def _firecrawl_scrape(url: str, api_key: str, retries: int = 2,
                      use_raw_html: bool = False) -> str:
    """
    Scrape a URL via Firecrawl. Returns markdown (or raw HTML if use_raw_html=True).
    Retries with exponential backoff.
    """
    endpoint = "https://api.firecrawl.dev/v1/scrape"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    fmt = "rawHtml" if use_raw_html else "markdown"
    payload = {
        "url": url,
        "formats": [fmt],
        "onlyMainContent": False,
        "waitFor": 5000,
    }

    for attempt in range(retries + 1):
        try:
            resp = requests.post(endpoint, json=payload, headers=headers, timeout=90)
            resp.raise_for_status()
            data = resp.json()
            content = data.get("data", {}).get(fmt, "") or ""
            if content.strip():
                return content
            logger.warning("Firecrawl returned empty content for %s (attempt %d)", url, attempt + 1)
        except requests.RequestException as exc:
            logger.warning("Firecrawl request error for %s (attempt %d): %s", url, attempt + 1, exc)

        if attempt < retries:
            time.sleep(2 ** attempt)

    return ""


def _build_search_urls(market: str, page: int = 1) -> list[str]:
    """
    Build Crexi land search URLs for a given market.

    Uses Crexi's subcategory URL pattern which is confirmed to work:
        /properties/{STATE}/Commercial-Land
        /properties/{STATE}/Industrial-Land

    Acre filtering is done in parser.py (URL params were unreliable).
    Returns two URLs — one per zoning category.
    """
    abbrev = _state_abbrev(market)
    base = "https://www.crexi.com/properties"
    page_param = f"?page={page}" if page > 1 else ""
    return [
        f"{base}/{abbrev}/Commercial-Land{page_param}",
        f"{base}/{abbrev}/Industrial-Land{page_param}",
    ]


def _extract_listing_id(url: str) -> Optional[str]:
    """Extract Crexi listing ID from a listing URL slug."""
    # e.g. https://www.crexi.com/properties/for-sale/abc-street/123456
    # or   https://www.crexi.com/land/abc-street/123456
    match = re.search(r"/(\d{4,})", url)
    return match.group(1) if match else None


def _sanity_check_search_page(content: str, url: str) -> None:
    """Raise CrexiBlockedError if content doesn't look like a real Crexi search results page."""
    content_lower = content.lower()
    hits = sum(1 for marker in SEARCH_PAGE_MARKERS if marker in content_lower)
    if hits < 2:
        raise CrexiBlockedError(
            f"Crexi search page sanity check failed for {url} — "
            f"only {hits}/{len(SEARCH_PAGE_MARKERS)} expected markers found. "
            "Likely a bot-detection or error page."
        )
    # Extra check: if "oh no" error message is present AND no numeric listing IDs, it's an empty results page
    if "oh no" in content_lower and not re.search(r'/properties/\d{4,}/', content):
        raise CrexiBlockedError(
            f"Crexi returned 'no results' page for {url} — "
            "filters may be too restrictive or URL format changed."
        )


def _sanity_check_listing_page(content: str, url: str) -> None:
    """Raise CrexiBlockedError if content doesn't look like a real Crexi listing page."""
    content_lower = content.lower()
    hits = sum(1 for marker in LISTING_PAGE_MARKERS if marker in content_lower)
    if hits < 2:
        raise CrexiBlockedError(
            f"Crexi listing page sanity check failed for {url} — "
            f"only {hits}/{len(LISTING_PAGE_MARKERS)} expected markers found."
        )


def _redirect_check(content: str, stage1_title: str, url: str) -> None:
    """
    Raise CrexiRedirectError if the scraped page title doesn't match the Stage 1 title.

    Uses token overlap: if fewer than 50% of Stage 1 title tokens appear in the
    scraped content, assume the listing was taken down and redirected.
    """
    if not stage1_title:
        return  # Can't check without a reference title

    stage1_tokens = set(re.findall(r"[a-z0-9]+", stage1_title.lower()))
    if not stage1_tokens:
        return

    content_lower = content.lower()
    matches = sum(1 for t in stage1_tokens if t in content_lower)
    overlap = matches / len(stage1_tokens)

    if overlap < 0.5:
        raise CrexiRedirectError(
            f"Redirect detected for {url} — only {overlap:.0%} of Stage 1 title tokens "
            f"found in scraped content. Listing likely taken down or redirected."
        )


def scrape_search_results(market: str, api_key: str, page: int = 1) -> list[dict]:
    """
    Scrape Crexi Commercial-Land and Industrial-Land pages for the given market.

    Returns a combined list of raw listing dicts:
        {listing_id, url, title, acres_hint, price_hint, raw_snippet, market}

    Raises CrexiBlockedError if any response looks like a bot-detection page.
    """
    urls = _build_search_urls(market, page=page)
    all_listings: list[dict] = []
    seen_ids: set[str] = set()

    for url in urls:
        logger.info("Scraping Crexi search: %s", url)
        html = _firecrawl_scrape(url, api_key, use_raw_html=True)
        if not html:
            raise CrexiBlockedError(f"Firecrawl returned empty content for: {url}")

        _sanity_check_search_page(html, url)

        listings = _parse_search_results_html(html, market)
        # Deduplicate across the two category pages
        for lst in listings:
            if lst["listing_id"] not in seen_ids:
                seen_ids.add(lst["listing_id"])
                all_listings.append(lst)

        time.sleep(1)  # brief pause between the two category requests

    logger.info("Found %d unique listings for market '%s' (page %d)", len(all_listings), market, page)
    return all_listings


def _parse_search_results_html(html: str, market: str) -> list[dict]:
    """
    Parse Crexi search results raw HTML to extract listing info.

    Crexi's confirmed listing URL pattern:
        https://www.crexi.com/properties/{numeric_id}/{state}-{slug}?recommId=...

    Extracts listing_id, clean url, title from alt text, acres/price hints from nearby text.
    """
    listings = []
    seen_ids: set[str] = set()

    # Match listing URLs with numeric IDs (real listings, not category pages)
    url_pattern = re.compile(
        r'href="(https?://(?:www\.)?crexi\.com/properties/(\d{4,})/[^"?]+)',
        re.IGNORECASE,
    )

    for match in url_pattern.finditer(html):
        raw_url = match.group(1)
        listing_id = match.group(2)

        if listing_id in seen_ids:
            continue
        seen_ids.add(listing_id)

        # Clean URL — strip tracking params
        clean_url = raw_url.split("?")[0]

        # Get surrounding HTML (~800 chars) for title/price/acres hints
        pos = match.start()
        snippet_html = html[max(0, pos - 200): pos + 800]

        # Title: look for alt text on property image near this URL
        title = ""
        alt_match = re.search(r'alt="(?:Pictures of [^"]*? located at )([^"]+?) for sales', snippet_html)
        if not alt_match:
            # Fallback: derive title from URL slug
            slug = raw_url.split("/")[-1].split("?")[0]
            title = slug.replace("-", " ").title()
        else:
            title = alt_match.group(1).strip()

        # Price hint from surrounding text
        price_hint = _extract_price_hint(snippet_html)

        # Acreage hint from surrounding text
        acres_hint = _extract_acres_hint(snippet_html)

        listings.append({
            "listing_id": listing_id,
            "url": clean_url,
            "title": title,
            "acres_hint": acres_hint,
            "price_hint": price_hint,
            "market": market,
            "raw_snippet": re.sub(r"<[^>]+>", " ", snippet_html)[:300],  # strip tags for readability
        })

    return listings


def _extract_acres_hint(text: str) -> Optional[float]:
    """Extract acreage from a text snippet. Returns float or None."""
    match = re.search(r"([\d,.]+)\s*ac(?:res?)?", text, re.IGNORECASE)
    if match:
        try:
            return float(match.group(1).replace(",", ""))
        except ValueError:
            pass
    return None


def _extract_price_hint(text: str) -> Optional[float]:
    """Extract asking price from a text snippet. Returns float or None."""
    # Matches "$1,200,000" or "$1.2M" or "1200000"
    match = re.search(r"\$\s*([\d,]+(?:\.\d+)?)\s*([MmKk]?)", text)
    if match:
        try:
            val = float(match.group(1).replace(",", ""))
            suffix = match.group(2).upper()
            if suffix == "M":
                val *= 1_000_000
            elif suffix == "K":
                val *= 1_000
            return val
        except ValueError:
            pass
    return None


def scrape_listing_detail(listing_url: str, stage1_title: str, api_key: str) -> dict:
    """
    Scrape an individual Crexi listing page to extract full deal details.

    Performs:
    1. Sanity check — confirms it's a real listing page
    2. Redirect check — fuzzy-matches against Stage 1 title

    Returns a dict:
        {address, asking_price, acres, zoning, description, raw_content}

    Raises:
        CrexiBlockedError — bot-detection page returned
        CrexiRedirectError — listing taken down / redirected
    """
    logger.info("Scraping Crexi listing: %s", listing_url)

    content = _firecrawl_scrape(listing_url, api_key)
    if not content:
        raise CrexiBlockedError(f"Firecrawl returned empty content for listing: {listing_url}")

    _sanity_check_listing_page(content, listing_url)
    _redirect_check(content, stage1_title, listing_url)

    return _parse_listing_detail_markdown(content)


def _parse_listing_detail_markdown(content: str) -> dict:
    """
    Parse individual Crexi listing page markdown.

    Confirmed Crexi listing page structure (from live data):
    - Address in H1: "# W Boone Avenue & N Monroe Street, Spokane, WA 99201"
    - Price standalone: "$675,000"
    - Data fields on own lines: "Acreage  0.390", "Zoning  CC2", "Sub Type  Commercial"
    - Description under "### Marketing description"
    """
    result = {
        "address": None,
        "asking_price": None,
        "acres": None,
        "zoning": None,
        "description": None,
        "raw_content": content[:3000],
    }

    # Address — H1 heading that contains a US state abbreviation (e.g. "WA 99201")
    # Pattern: "# Some Address, City, ST NNNNN"
    addr_match = re.search(
        r"^#\s+([^\n]+?,\s*[A-Z]{2}\s+\d{5}[^\n]*)",
        content, re.MULTILINE
    )
    if not addr_match:
        # Fallback: H1 that contains a recognizable address pattern
        addr_match = re.search(r"^#\s+([^\n]{10,})", content, re.MULTILINE)
    if addr_match:
        candidate = addr_match.group(1).strip()
        # Filter out nav-bar H1s like "Washington Properties for Sale"
        if not re.search(r"properties for sale|search results", candidate, re.IGNORECASE):
            result["address"] = candidate

    # Asking price — standalone "$X,XXX" or "$X.XM" line (not inside a sentence)
    # Skip "Contact Broker" / "Unpriced" — check only the first 800 chars where the listing's
    # own price block appears; later pages contain "Contact Broker" buttons for other listings.
    price_header = content[:800]
    if not re.search(r"contact\s+broker|unpriced|call\s+for\s+price|price\s+upon\s+request",
                     price_header, re.IGNORECASE):
        price_match = None
        extracted_price: Optional[float] = None

        # Pattern 1: standalone line — plain or markdown-bold: $1,250,000 or **$1,250,000**
        price_match = re.search(
            r"(?:^|\n)\s*\*{0,2}\$\s*([\d,]+(?:\.\d+)?)\s*([MmKk]?)\*{0,2}\s*(?:\r?\n|\||\Z)",
            content
        )
        # Pattern 2: labeled same line — **Asking Price:** $995,000 / Sale Price $850,000
        if not price_match:
            price_match = re.search(
                r"\*{0,2}(?:sale\s+price|asking\s+price|list\s+price|price)\*{0,2}"
                r"[\s\*:]{0,6}\$\s*([\d,]+(?:\.\d+)?)\s*([MmKk]?)",
                content, re.IGNORECASE
            )
        # Pattern 3: label on one line, price on the next line
        if not price_match:
            price_match = re.search(
                r"(?:sale\s+price|asking\s+price|list\s+price|price)[^\n]{0,20}\n"
                r"\s*\*{0,2}\$\s*([\d,]+(?:\.\d+)?)\s*([MmKk]?)",
                content, re.IGNORECASE
            )
        # Pattern 4: last resort — first plausible land price ($50K–$100M) in first 2000 chars
        if not price_match:
            for _m in re.finditer(r"\$\s*([\d,]+(?:\.\d+)?)\s*([MmKk]?)", content[:2000]):
                try:
                    _v = float(_m.group(1).replace(",", ""))
                    _s = _m.group(2).upper()
                    if _s == "M":
                        _v *= 1_000_000
                    elif _s == "K":
                        _v *= 1_000
                    if 50_000 <= _v <= 100_000_000:
                        extracted_price = _v
                        break
                except ValueError:
                    pass

        if price_match:
            try:
                val = float(price_match.group(1).replace(",", ""))
                suffix = price_match.group(2).upper()
                if suffix == "M":
                    val *= 1_000_000
                elif suffix == "K":
                    val *= 1_000
                result["asking_price"] = val
            except ValueError:
                pass
        elif extracted_price is not None:
            result["asking_price"] = extracted_price

    # Acreage — "Acreage  0.390" line (preferred) or "X.XX acres" anywhere
    acres_match = re.search(r"^Acreage\s+([\d.]+)", content, re.MULTILINE)
    if not acres_match:
        acres_match = re.search(r"([\d.]+)\s+acres?\b", content, re.IGNORECASE)
    if acres_match:
        try:
            result["acres"] = float(acres_match.group(1))
        except ValueError:
            pass

    # Zoning — "Zoning  CC2" line
    zoning_match = re.search(r"^Zoning\s+(\S[^\n]*)", content, re.MULTILINE)
    if zoning_match:
        result["zoning"] = zoning_match.group(1).strip()

    # Description — text under "### Marketing description" or "### Investment highlights"
    desc_match = re.search(
        r"#{1,3}\s+(?:marketing description|investment highlights|description)\s*\n+(.{30,}?)(?:\n#{1,3}|\Z)",
        content, re.IGNORECASE | re.DOTALL
    )
    if desc_match:
        result["description"] = desc_match.group(1).strip()[:1000]

    return result
