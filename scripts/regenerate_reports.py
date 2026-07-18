"""
regenerate_reports.py

Rewrite existing Excel reports using the new logic (in-store rates for E6,
facility-type proforma) without re-scraping. Reads stored comps from the SQLite
DB and rebuilds each report in place.

Skips deals that have no stored comps (comps table empty for that listing_id) —
those would need a full re-run with re-scraping.

Usage:
    python regenerate_reports.py [--dry-run]
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # run from repo root or scripts/

import argparse
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def _build_facilities_from_comps(conn, listing_id: str) -> list:
    """Reconstruct the facilities list from the comps table for one deal."""
    rows = conn.execute("""
        SELECT facility_name, facility_address, distance_miles,
               unit_size, unit_type, web_rate, in_store_rate
        FROM comps
        WHERE listing_id = ?
        ORDER BY distance_miles ASC, facility_name
    """, (listing_id,)).fetchall()

    fac_map: dict[tuple, dict] = {}
    for r in rows:
        key = (r["facility_name"], r["facility_address"])
        if key not in fac_map:
            fac_map[key] = {
                "name":            r["facility_name"],
                "address":         r["facility_address"],
                "distance_miles":  r["distance_miles"],
                "drive_time_min":  None,
                "phone":           "",
                "website":         "",
                "pricing":         [],
            }
        fac_map[key]["pricing"].append({
            "size":          r["unit_size"],
            "type":          r["unit_type"],
            "unit_type":     r["unit_type"],
            "web_rate":      r["web_rate"],
            "in_store_rate": r["in_store_rate"],
        })

    return sorted(fac_map.values(), key=lambda f: f["distance_miles"] or 9999)


def main():
    parser = argparse.ArgumentParser(description="Regenerate Excel reports with new logic")
    parser.add_argument("--dry-run", action="store_true", help="List what would be rewritten without writing")
    args = parser.parse_args()

    from db_utils import get_db, write_deal_to_db, recalculate_scores
    from comps_pipeline import write_comps_excel

    conn = get_db()
    rows = conn.execute("""
        SELECT d.listing_id, d.address, d.acres, d.asking_price, d.crexi_url,
               d.report_path, d.market, d.lat, d.lng, d.zip_code,
               d.zip_pool_count, d.population_3mi, d.pop_gate_passed, d.city_name,
               d.scraped_at,
               (SELECT COUNT(*) FROM comps c WHERE c.listing_id = d.listing_id) AS comp_count
        FROM deals d
        WHERE d.processed_at IS NOT NULL
          AND d.skip_reason IS NULL
          AND d.report_path IS NOT NULL
        ORDER BY d.listing_id
    """).fetchall()

    total = len(rows)
    logger.info("Found %d processed deals with reports", total)

    rewritten = 0
    no_comps = 0
    no_file = 0
    errors = 0

    for i, row in enumerate(rows, 1):
        lid          = row["listing_id"]
        report_path  = row["report_path"]
        address      = row["address"] or ""
        acres        = row["acres"]
        asking_price = row["asking_price"]
        crexi_url    = row["crexi_url"] or ""
        comp_count   = row["comp_count"]

        if not report_path:
            logger.warning("[%d/%d] %s -- no report_path, skipping", i, total, lid)
            no_file += 1
            continue

        if comp_count == 0:
            logger.warning("[%d/%d] %s -- no stored comps (would need re-scrape), skipping",
                           i, total, lid)
            no_comps += 1
            continue

        logger.info("[%d/%d] %s | %s | acres=%s | comps=%d",
                    i, total, lid, address[:50], acres, comp_count)

        facilities = _build_facilities_from_comps(conn, lid)

        if args.dry_run:
            rewritten += 1
            continue

        try:
            write_comps_excel(
                facilities=facilities,
                output_path=report_path,
                location=address,
                acres=acres,
                asking_price=asking_price,
                crexi_url=crexi_url,
            )
            rewritten += 1

            # Re-write deal to DB to recalc derived fields (yield_on_cost, avg_psf etc.)
            write_deal_to_db(
                listing_id=lid,
                report_path=report_path,
                market=row["market"] or "",
                address=address,
                url=crexi_url,
                lat=row["lat"],
                lng=row["lng"],
                population_3mi=row["population_3mi"],
                zip_code=row["zip_code"],
                zip_pool_count=row["zip_pool_count"],
                first_seen=row["scraped_at"],
                facilities=facilities,
                pop_gate_passed=row["pop_gate_passed"],
                city_name=row["city_name"],
                recalc=False,
            )
        except Exception as exc:
            logger.error("  ERROR: %s", exc)
            errors += 1

    if not args.dry_run and rewritten > 0:
        conn.commit()
        logger.info("Recalculating deal scores...")
        recalculate_scores(conn)
        conn.commit()

    conn.close()

    logger.info("")
    logger.info("=" * 60)
    logger.info("Report regeneration complete%s",
                " (DRY RUN -- no writes)" if args.dry_run else "")
    logger.info("  Rewritten     : %d", rewritten)
    logger.info("  No stored comps: %d (need full re-scrape)", no_comps)
    logger.info("  No file path  : %d", no_file)
    logger.info("  Errors        : %d", errors)


if __name__ == "__main__":
    main()
