"""
crexi/parser.py

Validates and enriches raw listing data from scraper.py:
- Address confidence scoring
- Negative keyword filter
- Acreage range filter
- Zoning filter
"""

import re
import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

# Deal is skipped at Stage 1 (title/snippet) and again at Stage 2b (full description)
SKIP_KEYWORDS = [
    "wetlands", "wetland", "floodplain", "flood plain", "flood zone",
    "landfill", "easement", "conservation", "superfund",
    "contaminated", "contamination", "brownfield", "brown field",
    "restricted", "protected land", "wildlife refuge",
]

ACRES_MIN = 1.5
ACRES_MAX = 7.0

VALID_ZONING_KEYWORDS = ["commercial", "industrial", "c-1", "c-2", "c-3", "m-1", "m-2", "b-1", "b-2", "c"]


@dataclass
class CrexiDeal:
    listing_id: str
    url: str
    title: str
    market: str

    # Stage 1 hints (from search results page)
    acres_hint: Optional[float] = None
    price_hint: Optional[float] = None
    raw_snippet: str = ""

    # Stage 2b full detail (from individual listing page)
    address: Optional[str] = None
    asking_price: Optional[float] = None
    acres: Optional[float] = None
    zoning: Optional[str] = None
    description: Optional[str] = None

    # Derived
    address_confidence: str = "unknown"   # "full" | "intersection" | "city_only" | "unknown"
    skip_reason: Optional[str] = None
    scraped: bool = False
    processed: bool = False

    def best_acres(self) -> Optional[float]:
        """Return Stage 2b acreage if available, else Stage 1 hint."""
        return self.acres if self.acres is not None else self.acres_hint

    def best_price(self) -> Optional[float]:
        return self.asking_price if self.asking_price is not None else self.price_hint


def address_confidence(address: str) -> str:
    """
    Score the geocodability of an address string.

    "full"         — has a leading house number → geocodes to exact parcel
    "intersection" — contains & or "and" between street names → ~1 block accuracy
    "city_only"    — no street info → too vague, skip pipeline
    """
    if not address or not address.strip():
        return "city_only"

    addr = address.strip()

    # Full street address: starts with a number
    if re.match(r"^\d+\s+\S", addr):
        return "full"

    # Intersection: "Main St & 1st Ave" or "W Boone Avenue & N Monroe Street"
    # Note: & is a non-word char so \b doesn't work around it — check separately
    if re.search(r"&|\band\b", addr, re.IGNORECASE):
        # Match both abbreviations (st, ave) and full words (street, avenue, drive, ...)
        if re.search(
            r"\b(st|ave|avenue|blvd|boulevard|rd|road|dr|drive|ln|lane|"
            r"way|hwy|highway|pkwy|parkway|ct|court|pl|place|cir|circle|"
            r"street|terrace|terr|trail|trl)\b",
            addr, re.IGNORECASE
        ):
            return "intersection"

    return "city_only"


def negative_keyword_check(text: str) -> Optional[str]:
    """
    Return the first matching skip keyword found in text, or None if clean.
    Case-insensitive.
    """
    if not text:
        return None
    text_lower = text.lower()
    for kw in SKIP_KEYWORDS:
        if kw in text_lower:
            return kw
    return None


def filter_stage1(deal: CrexiDeal) -> Optional[str]:
    """
    Apply Stage 1 paper filters to a raw deal from search results.

    Returns a skip_reason string if the deal should be skipped, else None.
    Checks: acreage range, negative keywords in title + snippet.
    """
    # Acreage range check (only if we have a hint)
    acres = deal.acres_hint
    if acres is not None:
        if acres < ACRES_MIN:
            return f"too small ({acres:.1f} ac < {ACRES_MIN} ac minimum)"
        if acres > ACRES_MAX:
            return f"too large ({acres:.1f} ac > {ACRES_MAX} ac maximum)"

    # Negative keyword check on title + snippet
    combined = f"{deal.title} {deal.raw_snippet}"
    kw = negative_keyword_check(combined)
    if kw:
        return f"negative keyword in Stage 1 data: '{kw}'"

    return None


def filter_stage2b(deal: CrexiDeal) -> Optional[str]:
    """
    Apply Stage 2b filters after full listing detail is scraped.

    Returns a skip_reason string if the deal should be skipped, else None.
    Checks: acreage range (confirmed), zoning, negative keywords in description.
    """
    # Confirmed acreage range
    acres = deal.acres
    if acres is not None:
        if acres < ACRES_MIN:
            return f"too small ({acres:.1f} ac < {ACRES_MIN} ac minimum)"
        if acres > ACRES_MAX:
            return f"too large ({acres:.1f} ac > {ACRES_MAX} ac maximum)"

    # Zoning check — must contain at least one valid zoning keyword
    if deal.zoning:
        zoning_lower = deal.zoning.lower()
        if not any(kw in zoning_lower for kw in VALID_ZONING_KEYWORDS):
            return f"zoning '{deal.zoning}' does not match commercial/industrial criteria"
        if "residential" in zoning_lower:
            return f"zoning '{deal.zoning}' includes residential — not suitable for commercial development"

    # Negative keyword check on full description
    kw = negative_keyword_check(deal.description or "")
    if kw:
        return f"negative keyword in listing description: '{kw}'"

    return None


def enrich_with_detail(deal: CrexiDeal, detail: dict) -> None:
    """
    Populate a CrexiDeal with fields returned by scraper.scrape_listing_detail().
    Mutates the deal in place.
    """
    deal.address = detail.get("address") or deal.address
    deal.asking_price = detail.get("asking_price")
    deal.acres = detail.get("acres")
    deal.zoning = detail.get("zoning")
    deal.description = detail.get("description")

    # Assign address confidence now that we have the full address
    if deal.address:
        deal.address_confidence = address_confidence(deal.address)
    else:
        deal.address_confidence = "city_only"


def approximate_location_note(deal: CrexiDeal) -> Optional[str]:
    """
    Return a warning note for Excel if address confidence is not 'full'.
    Returns None if address is a full street address.
    """
    if deal.address_confidence == "intersection":
        return f"Location Approximate — address is intersection ({deal.address}); comps radius may be off-center"
    if deal.address_confidence == "city_only":
        return f"Location Approximate — only city/state available ({deal.address or deal.market})"
    return None
