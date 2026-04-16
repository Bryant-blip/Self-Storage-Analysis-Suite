"""
crexi/dedup.py

Two-state persistence for seen Crexi deals.

State model per listing_id:
  scraped=True,  processed=False  — seen in Stage 1; Stage 2 not yet complete
                                    (vague address, rural, bad keywords, or mid-pipeline crash)
  scraped=True,  processed=True   — fully analyzed; Excel report saved

A deal re-enters the pipeline queue on the next run only if processed=False.
This means a crash mid-pipeline does NOT lose the deal.
"""

import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

DEFAULT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "data", "seen_deals.json"
)


def path_for_market(market: str) -> str:
    """Return the per-market dedup file path, e.g. data/seen_deals_washington.json."""
    safe = market.strip().lower().replace(" ", "_")
    return os.path.join(os.path.dirname(DEFAULT_PATH), f"seen_deals_{safe}.json")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def load(path: str = DEFAULT_PATH) -> dict:
    """Load seen_deals.json. Returns empty dict if file doesn't exist."""
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Could not load seen_deals.json (%s) — starting fresh", exc)
        return {}


def save(data: dict, path: str = DEFAULT_PATH) -> None:
    """Write seen_deals.json atomically (write to .tmp then rename)."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = path + ".tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp_path, path)
    except OSError as exc:
        logger.error("Failed to save seen_deals.json: %s", exc)
        raise


def is_new(listing_id: str, data: dict) -> bool:
    """Return True if this listing_id has never been seen before."""
    return listing_id not in data


def needs_processing(listing_id: str, data: dict) -> bool:
    """
    Return True if the deal was scraped but not yet successfully processed.
    This catches deals that were interrupted mid-pipeline.
    Deals with a skip_reason are permanently skipped and excluded.
    """
    entry = data.get(listing_id)
    if not entry:
        return False
    return (entry.get("scraped", False)
            and not entry.get("processed", False)
            and not entry.get("skip_reason"))


def mark_scraped(listing_id: str, data: dict, address: str = "", title: str = "",
                 skip_reason: Optional[str] = None, market: str = "",
                 url: str = "") -> None:
    """
    Mark a deal as seen at Stage 1 (scraped=True, processed=False).
    If skip_reason is provided, the deal is noted as skipped (won't be re-queued).
    """
    existing = data.get(listing_id, {})
    data[listing_id] = {
        "scraped": True,
        "processed": False,
        "first_seen": existing.get("first_seen", _now_iso()),
        "last_seen": _now_iso(),
        "address": address or existing.get("address", ""),
        "title": title or existing.get("title", ""),
        "market": market or existing.get("market", ""),
        "url": url or existing.get("url", ""),
        "skip_reason": skip_reason,
        "report_path": existing.get("report_path"),
    }


def mark_processed(listing_id: str, data: dict, report_path: str,
                   market: str = "",
                   population_3mi: int = None,
                   zip_code: str = None,
                   zip_pool_count: int = None,
                   pop_gate_passed: str = None) -> None:
    """
    Mark a deal as fully processed (scraped=True, processed=True).
    Stores the path to the generated Excel report and Census population data.
    """
    existing = data.get(listing_id, {})
    data[listing_id] = {
        **existing,
        "scraped": True,
        "processed": True,
        "last_seen": _now_iso(),
        "report_path": report_path,
        "market": market or existing.get("market", ""),
        "skip_reason": None,
        "population_3mi": population_3mi,
        "zip_code": zip_code,
        "zip_pool_count": zip_pool_count,
        "pop_gate_passed": pop_gate_passed,
    }


def backfill_market_from_url(data: dict) -> int:
    """
    Backfill missing market fields for existing entries using three signals (in priority order):
      1. Stored URL — /properties/WA/ → 'Washington'  (new entries only)
      2. Address    — ', WA 98001' state abbreviation
      3. Title      — title starts with the full state name ('Utah ...', 'North Carolina ...')
    Only updates entries where market is empty/missing.
    Returns count of entries updated.
    """
    from crexi.scraper import STATE_ABBREVIATIONS
    abbrev_to_name = {v: k.title() for k, v in STATE_ABBREVIATIONS.items()}
    # Sorted longest first so "North Carolina" matches before "Carolina"
    known_names = sorted(abbrev_to_name.values(), key=len, reverse=True)

    updated = 0
    for entry in data.values():
        if entry.get("market"):
            continue  # already set

        market = ""

        # 1. URL (present on new entries)
        url = entry.get("url", "")
        m = re.search(r"/properties/([A-Z]{2})/", url)
        if m:
            market = abbrev_to_name.get(m.group(1), m.group(1))

        # 2. Address — look for ", XX " state abbreviation near end
        if not market:
            addr = entry.get("address", "") or ""
            m = re.search(r",\s*([A-Z]{2})\s+\d{5}", addr)
            if m:
                market = abbrev_to_name.get(m.group(1), "")

        # 3. Title — starts with full state name
        if not market:
            title = (entry.get("title", "") or "").strip()
            for name in known_names:
                if title.lower().startswith(name.lower()):
                    market = name
                    break

        if market:
            entry["market"] = market
            updated += 1
    return updated


def get_unprocessed(data: dict) -> list[str]:
    """
    Return listing_ids that were scraped but not yet processed
    (eligible for re-queuing on this run).
    """
    return [
        lid for lid, entry in data.items()
        if entry.get("scraped") and not entry.get("processed")
        and not entry.get("skip_reason")  # permanently skipped entries are excluded
    ]


def summary(data: dict) -> str:
    """Return a one-line summary of the current seen_deals state."""
    total = len(data)
    processed = sum(1 for e in data.values() if e.get("processed"))
    skipped = sum(1 for e in data.values() if e.get("skip_reason"))
    pending = total - processed - skipped
    return f"{total} total | {processed} processed | {skipped} skipped | {pending} pending"
