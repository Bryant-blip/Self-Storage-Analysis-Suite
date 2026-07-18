"""
crexi_watcher.py

CLI runner for the Crexi land-deal watcher (Phase 1).

Usage:
    python crexi_watcher.py [options]

Options:
    --market      Market to search (default: Washington)
    --max-deals   Max new deals to run full pipeline on per run (default: 3)
    --dry-run     Scrape and parse Crexi, but skip comps pipeline entirely
    --reset-dedup Clear seen_deals.json (use for fresh start / testing)

Startup sequence (to avoid burning API quota):
  Step 1:  python crexi_watcher.py --dry-run --max-deals 0
           → scrape search page only, print raw listings, 0 Google calls
  Step 2:  python crexi_watcher.py --dry-run --max-deals 3
           → scrape listing detail + population check, verify address extraction
  Step 3:  python crexi_watcher.py --max-deals 1
           → full pipeline on 1 deal, review Excel output
  Step 4:  python crexi_watcher.py --max-deals 3
           → normal operation
"""

import argparse
import logging
import os
import sys
import time

from dotenv import load_dotenv

import comps_pipeline
from db_utils import write_deal_to_db
from crexi import census_pop as census_module
from crexi import dedup as dedup_module
from crexi import parser as parser_module
from crexi import scraper as scraper_module
from crexi.parser import CrexiDeal
from crexi.scraper import CrexiBlockedError, CrexiRedirectError

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def _geocode(address: str, api_key: str):
    """Geocode an address using Google Geocoding API. Returns (lat, lng) or None."""
    import requests
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {"address": address, "key": api_key}
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        results = resp.json().get("results", [])
        if results:
            loc = results[0]["geometry"]["location"]
            return loc["lat"], loc["lng"]
    except Exception as exc:
        logger.warning("Geocode failed for '%s': %s", address, exc)
    return None



# ---------------------------------------------------------------------------
# Main pipeline per deal
# ---------------------------------------------------------------------------

def _output_path_for_deal(deal: CrexiDeal, market: str = "") -> str:
    address = deal.address or deal.title or deal.listing_id
    safe = (
        address.replace(" ", "_")
                .replace(",", "")
                .replace("/", "-")
                .replace("\\", "-")
                .replace(":", "")
    )[:60]
    out_dir = os.path.join(os.path.dirname(__file__), "reports")
    os.makedirs(out_dir, exist_ok=True)
    return os.path.join(out_dir, f"crexi_{safe}.xlsx")


def process_deal(deal: CrexiDeal, api_keys: dict, dry_run: bool,
                 seen_data: dict, dedup_path: str, market: str = "") -> bool:
    """
    Run Stages 2a–2c for a single deal.

    Returns True if report was successfully generated (or dry-run completed).
    Mutates seen_data and writes dedup_path on success.
    """
    logger.info("─" * 60)
    logger.info("Processing deal: %s | %s", deal.listing_id, deal.title or "(no title)")

    # ── Stage 2b: scrape listing detail ────────────────────────────────────
    logger.info("  Stage 2b — scraping listing detail: %s", deal.url)
    try:
        detail = scraper_module.scrape_listing_detail(deal.url, deal.title, api_keys["firecrawl"])
    except CrexiRedirectError as exc:
        logger.warning("  SKIP — redirect detected: %s", exc)
        dedup_module.mark_scraped(deal.listing_id, seen_data,
                                  address=deal.address or "",
                                  title=deal.title,
                                  skip_reason="redirect_detected",
                                  market=market, url=deal.url)
        dedup_module.save(seen_data, dedup_path)
        return False
    except CrexiBlockedError as exc:
        logger.error("  SKIP — Crexi blocked on listing page: %s", exc)
        return False

    parser_module.enrich_with_detail(deal, detail)
    logger.info("  Address: %s (confidence: %s)", deal.address, deal.address_confidence)
    logger.info("  Asking price extracted: %s",
                f"${deal.asking_price:,.0f}" if deal.asking_price else
                f"none (price_hint: ${deal.price_hint:,.0f})" if deal.price_hint else "none")

    # ── Address confidence gate ─────────────────────────────────────────────
    if deal.address_confidence == "city_only":
        reason = f"address too vague for pipeline: '{deal.address or deal.market}'"
        logger.warning("  SKIP — %s", reason)
        dedup_module.mark_scraped(deal.listing_id, seen_data,
                                  address=deal.address or "",
                                  title=deal.title,
                                  skip_reason=reason,
                                  market=market, url=deal.url)
        dedup_module.save(seen_data, dedup_path)
        return False

    # ── Stage 2b filter (negative keywords, zoning, confirmed acreage) ──────
    skip = parser_module.filter_stage2b(deal)
    if skip:
        logger.warning("  SKIP — Stage 2b filter: %s", skip)
        dedup_module.mark_scraped(deal.listing_id, seen_data,
                                  address=deal.address or "",
                                  title=deal.title,
                                  skip_reason=skip,
                                  market=market, url=deal.url)
        dedup_module.save(seen_data, dedup_path)
        return False

    # ── Stage 2a: Census population gate (skipped in dry-run) ──────────────────
    pop_result: dict = {}
    if not dry_run:
        geocode_address = deal.address or deal.market
        logger.info("  Stage 2a — geocoding: %s", geocode_address)
        coords = _geocode(geocode_address, api_keys["google"])

        if not coords:
            logger.warning("  SKIP — geocode failed for '%s'", geocode_address)
            return False

        lat, lng = coords
        min_pop    = int(os.environ.get("MIN_POPULATION_3MI", "30000"))
        cache_days = int(os.environ.get("POPULATION_CACHE_DAYS", "365"))

        logger.info("  Stage 2a — census population check...")
        pop_result = census_module.check_population_gate(
            lat, lng, deal.address or "",
            census_api_key=api_keys["census"],
            min_population=min_pop,
            cache_days=cache_days,
        )
        logger.info(
            "  Gate: %s | Population: %s | City pool: %d | Subject: %s",
            pop_result["pop_gate_passed"] or "NONE",
            f"{pop_result['population_total']:,}" if pop_result["population_total"] else "unknown",
            pop_result["zip_pool_count"],
            pop_result.get("city_name") or "unknown",
        )

        if not pop_result["passes"]:
            reason = pop_result["skip_reason"]
            logger.warning("  SKIP — %s", reason)
            dedup_module.mark_scraped(deal.listing_id, seen_data,
                                      address=deal.address or "",
                                      title=deal.title,
                                      skip_reason=reason,
                                      market=market, url=deal.url)
            dedup_module.save(seen_data, dedup_path)
            return False

    # ── Print deal summary (always shown) ───────────────────────────────────
    logger.info("  PASS all filters ✓")
    logger.info("  Address:       %s", deal.address)
    logger.info("  Acres:         %s", deal.best_acres())
    logger.info("  Asking Price:  %s", f"${deal.best_price():,.0f}" if deal.best_price() else "Contact Broker")
    logger.info("  Zoning:        %s", deal.zoning or "Unknown")
    logger.info("  Confidence:    %s", deal.address_confidence)
    note = parser_module.approximate_location_note(deal)
    if note:
        logger.info("  ⚠ %s", note)

    if dry_run:
        logger.info("  [DRY RUN] Skipping comps pipeline.")
        return True

    # ── Stage 2c: Full comps pipeline ───────────────────────────────────────
    output_path = _output_path_for_deal(deal, market=market)
    radius = float(os.environ.get("DEFAULT_RADIUS_MILES", "1.5"))

    logger.info("  Stage 2c — running comps pipeline (radius: %.1f mi)...", radius)
    logger.info("  Output: %s", output_path)

    try:
        result_path, facilities = comps_pipeline.run_comps_pipeline(
            location=deal.address,
            radius_miles=radius,
            output_path=output_path,
            api_keys=api_keys,
            progress_cb=lambda pct, msg: logger.info("    [%s%%] %s", pct if pct is not None else "--", msg),
            acres=deal.best_acres(),
            asking_price=deal.best_price(),
            crexi_url=deal.url,
        )
        logger.info("  Report saved: %s", result_path)

        dedup_module.mark_processed(
            deal.listing_id, seen_data,
            report_path=result_path or output_path,
            market=market,
            population_3mi=pop_result.get("population_3mi"),
            zip_code=pop_result.get("zip_code"),
            zip_pool_count=pop_result.get("zip_pool_count"),
            pop_gate_passed=pop_result.get("pop_gate_passed"),
        )
        dedup_module.save(seen_data, dedup_path)

        try:
            write_deal_to_db(
                listing_id=deal.listing_id,
                report_path=result_path or output_path,
                market=market,
                address=deal.address or "",
                url=deal.url or "",
                lat=lat if not dry_run else None,
                lng=lng if not dry_run else None,
                population_3mi=pop_result.get("population_3mi"),
                zip_code=pop_result.get("zip_code"),
                zip_pool_count=pop_result.get("zip_pool_count"),
                first_seen=seen_data[deal.listing_id].get("first_seen"),
                facilities=facilities,
                pop_gate_passed=pop_result.get("pop_gate_passed"),
                city_name=pop_result.get("city_name"),
            )
            logger.info("  SQLite record written for %s", deal.listing_id)
        except Exception as exc:
            logger.warning("  SQLite write failed (non-fatal): %s", exc)

        return True

    except Exception as exc:
        logger.error("  Comps pipeline error: %s", exc)
        # Do NOT mark processed — deal stays in pending state for retry
        return False


# ---------------------------------------------------------------------------
# Main entrypoint
# ---------------------------------------------------------------------------

def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description="Crexi land-deal watcher")
    parser.add_argument("--market", default=os.environ.get("CREXI_MARKET", "Washington"),
                        help="Market to search (default: Washington)")
    parser.add_argument("--max-deals", type=int,
                        default=int(os.environ.get("MAX_DEALS_PER_RUN", "3")),
                        help="Max new deals to process per run (default: 3)")
    parser.add_argument("--max-pages", type=int, default=None,
                        help="Max Crexi search result pages to scrape (default: 1 page, or 0 for all if explicitly set)")
    parser.add_argument("--dry-run", action="store_true",
                        default=os.environ.get("DRY_RUN", "false").lower() == "true",
                        help="Scrape + parse Crexi but skip comps pipeline")
    parser.add_argument("--reset-dedup", action="store_true",
                        help="Clear seen_deals.json before running (fresh start)")
    args = parser.parse_args()

    # ── API keys ─────────────────────────────────────────────────────────────
    api_keys = {
        "google":    os.environ.get("GOOGLE_PLACES_API_KEY", ""),
        "firecrawl": comps_pipeline._get_env("FIRECRAWL_API_KEY"),
        "anthropic": comps_pipeline._get_env("ANTHROPIC_API_KEY"),
        "census":    os.environ.get("CENSUS_API_KEY", ""),
    }
    # Census key is optional — ACS API works without a key at lower rate limits
    required_keys = ["google", "firecrawl", "anthropic"]
    missing = [k for k in required_keys if not api_keys[k]]
    if missing and not args.dry_run:
        logger.error("Missing API keys: %s — set them in .env", ", ".join(missing))
        sys.exit(1)
    if missing and args.dry_run:
        logger.warning("Missing API keys (%s) — OK for dry run", ", ".join(missing))

    # ── Dedup state ───────────────────────────────────────────────────────────
    dedup_path = dedup_module.path_for_market(args.market)
    if args.reset_dedup and os.path.exists(dedup_path):
        os.remove(dedup_path)
        logger.info("Cleared %s", os.path.basename(dedup_path))

    seen_data = dedup_module.load(dedup_path)

    # One-time migration: seed from legacy seen_deals.json if market file is new/empty
    if not seen_data:
        legacy = dedup_module.load(dedup_module.DEFAULT_PATH)
        if legacy:
            market_entries = {
                lid: e for lid, e in legacy.items()
                if (e.get("market") or "").lower() == args.market.lower()
            }
            if market_entries:
                seen_data = market_entries
                dedup_module.save(seen_data, dedup_path)
                logger.info("Migrated %d entries from seen_deals.json -> %s",
                            len(market_entries), os.path.basename(dedup_path))

    backfilled = dedup_module.backfill_market_from_url(seen_data)
    if backfilled:
        logger.info("Backfilled market field for %d entries", backfilled)
        dedup_module.save(seen_data, dedup_path)
    logger.info("Dedup state: %s", dedup_module.summary(seen_data))

    # ── Stage 1: Discover listings ────────────────────────────────────────────
    # --max-pages overrides MAX_SEARCH_PAGES env var. 0 means paginate until empty.
    if args.max_pages is not None:
        max_pages = args.max_pages
    else:
        max_pages = int(os.environ.get("MAX_SEARCH_PAGES", "1"))

    pages_label = f"up to {max_pages}" if max_pages > 0 else "all"
    logger.info("=" * 60)
    logger.info("Market: %s | Max deals: %d | Max pages: %s | Dry run: %s",
                args.market, args.max_deals, pages_label, args.dry_run)
    logger.info("=" * 60)

    all_raw_listings = []
    page = 1
    pages_scraped = 0
    while True:
        if max_pages > 0 and page > max_pages:
            logger.info("Reached max pages limit (%d) — stopping search.", max_pages)
            break
        logger.info("Stage 1 — scraping search results (page %d%s)...",
                    page, f"/{max_pages}" if max_pages > 0 else "")
        try:
            raw = scraper_module.scrape_search_results(args.market, api_keys["firecrawl"], page=page)
            all_raw_listings.extend(raw)
            pages_scraped += 1
            logger.info("  Found %d listings on page %d (running total: %d)",
                        len(raw), page, len(all_raw_listings))
            if not raw:
                logger.info("  No listings on page %d — reached end of results.", page)
                break
            page += 1
            time.sleep(1)
        except CrexiBlockedError as exc:
            if page == 1:
                logger.error("Crexi blocked on search page 1: %s", exc)
                logger.error("Aborting. Check if Firecrawl credits are available or try again later.")
                sys.exit(1)
            else:
                logger.error("Page %d blocked (%s) — stopping pagination. Results may be TRUNCATED; later pages were not scraped.", page, exc)
                break

    logger.info("Stage 1 complete: scraped %d pages, found %d total raw listings.", pages_scraped, len(all_raw_listings))

    if not all_raw_listings:
        logger.warning("No listings found. Possible causes: Crexi block, no matching results, or URL params changed.")
        sys.exit(0)

    logger.info("Total raw listings from search: %d", len(all_raw_listings))

    # ── Convert to CrexiDeal objects and apply Stage 1 filters ───────────────
    new_deals: list[CrexiDeal] = []
    retry_deals: list[CrexiDeal] = []

    for raw in all_raw_listings:
        lid = raw["listing_id"]
        deal = CrexiDeal(
            listing_id=lid,
            url=raw["url"],
            title=raw.get("title", ""),
            market=raw.get("market", args.market),
            acres_hint=raw.get("acres_hint"),
            price_hint=raw.get("price_hint"),
            raw_snippet=raw.get("raw_snippet", ""),
        )

        # Stage 1 filter
        skip = parser_module.filter_stage1(deal)
        if skip:
            logger.debug("  SKIP (Stage 1 filter) %s — %s", lid, skip)
            dedup_module.mark_scraped(lid, seen_data, title=deal.title, skip_reason=skip,
                                      market=args.market, url=deal.url)
            continue

        if dedup_module.is_new(lid, seen_data):
            dedup_module.mark_scraped(lid, seen_data, title=deal.title,
                                      market=args.market, url=deal.url)
            new_deals.append(deal)
        elif dedup_module.needs_processing(lid, seen_data):
            retry_deals.append(deal)

    # Save dedup after Stage 1 (captures all scraped=true entries even if we crash later)
    dedup_module.save(seen_data, dedup_path)

    logger.info(
        "After Stage 1: %d new | %d pending retry | %d already processed",
        len(new_deals), len(retry_deals),
        sum(1 for e in seen_data.values() if e.get("processed"))
    )

    # ── Decide which deals to process ────────────────────────────────────────
    # Process new deals first, then retries, up to max_deals
    queue = new_deals + retry_deals
    to_process = queue[:args.max_deals]

    if not to_process:
        logger.info("No new deals to process this run.")
        sys.exit(0)

    if args.max_deals == 0:
        logger.info("[max-deals=0] Stage 1 complete — inspect output above, no Stage 2 calls made.")
        sys.exit(0)

    # ── Stage 2: Process each deal sequentially ───────────────────────────────
    logger.info("Processing %d deal(s) sequentially...", len(to_process))
    successes = 0
    for i, deal in enumerate(to_process, 1):
        logger.info("\nDeal %d / %d", i, len(to_process))
        ok = process_deal(deal, api_keys, dry_run=args.dry_run,
                          seen_data=seen_data, dedup_path=dedup_path, market=args.market)
        if ok:
            successes += 1
        # Brief pause between deals to avoid Google QPS spikes
        if i < len(to_process):
            time.sleep(2)

    logger.info("=" * 60)
    logger.info("Run complete: %d / %d deals processed successfully", successes, len(to_process))
    logger.info("Dedup state: %s", dedup_module.summary(seen_data))


if __name__ == "__main__":
    main()
