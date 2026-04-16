"""
backfill_population.py

One-time script to fill population_3mi for deals that have a zip_code
but no population data (typically: deals migrated/backfilled before the
Census integration existed).

For each deal:
  1. Look up the ZIP centroid to get lat/lng (needed for neighbor search)
  2. Call check_population_gate() — uses local cache first, Census API second
  3. Write population_3mi, pop_gate_passed, and zip_pool_count back to deals table

Usage:
    python backfill_population.py [--dry-run]
"""

import argparse
import os
import sys
import logging

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Backfill census population for existing deals")
    parser.add_argument("--dry-run", action="store_true", help="Fetch data but do not write to DB")
    args = parser.parse_args()

    census_api_key = os.getenv("CENSUS_API_KEY", "")
    if not census_api_key:
        logger.warning("CENSUS_API_KEY not set — Census API calls will be unauthenticated (rate-limited)")

    from db_utils import get_db, recalculate_scores
    from crexi.census_pop import check_population_gate, load_zip_centroids

    centroids = load_zip_centroids()
    logger.info("Loaded %d ZIP centroids", len(centroids))

    conn = get_db()
    rows = conn.execute("""
        SELECT listing_id, zip_code, address
        FROM deals
        WHERE zip_code IS NOT NULL
          AND zip_code != ''
          AND (population_3mi IS NULL OR population_3mi = 0)
        ORDER BY listing_id
    """).fetchall()

    total = len(rows)
    logger.info("Found %d deals needing population backfill", total)

    if total == 0:
        logger.info("Nothing to do.")
        conn.close()
        return

    passed = 0
    failed = 0
    skipped = 0
    errors = 0

    for i, row in enumerate(rows, 1):
        lid     = row["listing_id"]
        zip_code = row["zip_code"]
        address  = row["address"] or ""

        # Get lat/lng from centroid file
        if zip_code not in centroids:
            logger.warning("[%d/%d] %s — ZIP %s not in centroids, skipping", i, total, lid, zip_code)
            skipped += 1
            continue

        lat, lng = centroids[zip_code]

        logger.info("[%d/%d] %s | ZIP %s | (%.4f, %.4f)", i, total, lid, zip_code, lat, lng)

        try:
            pop_result = check_population_gate(
                lat=lat,
                lng=lng,
                address=address or zip_code,  # pass address; ZIP already known so parse will find it
                census_api_key=census_api_key,
            )
        except Exception as exc:
            logger.error("  ERROR: %s", exc)
            errors += 1
            continue

        pop_3mi    = pop_result.get("population_3mi", 0)
        gate       = pop_result.get("pop_gate_passed")
        pool_count = pop_result.get("zip_pool_count", 1)
        passes     = pop_result.get("passes", False)

        status = f"PASS ({gate})" if passes else f"FAIL — {pop_result.get('skip_reason')}"
        logger.info("  population_3mi=%s | gate=%s | pool=%d | %s",
                    f"{pop_3mi:,}" if pop_3mi else "0", gate, pool_count, status)

        if passes:
            passed += 1
        else:
            failed += 1

        if args.dry_run:
            continue

        conn.execute("""
            UPDATE deals
            SET population_3mi = ?,
                pop_gate_passed = ?,
                zip_pool_count  = ?
            WHERE listing_id = ?
        """, (pop_3mi or None, gate, pool_count, lid))

    if not args.dry_run:
        conn.commit()
        logger.info("Recalculating deal scores...")
        recalculate_scores(conn)
        conn.commit()

    conn.close()

    logger.info("")
    logger.info("=" * 60)
    logger.info("Backfill complete%s", " (DRY RUN — no writes)" if args.dry_run else "")
    logger.info("  Processed : %d / %d", passed + failed + errors, total)
    logger.info("  Passed    : %d", passed)
    logger.info("  Failed    : %d (below threshold)", failed)
    logger.info("  Skipped   : %d (ZIP not in centroids)", skipped)
    logger.info("  Errors    : %d", errors)


if __name__ == "__main__":
    main()
