"""
crexi/census_pop.py

Census Bureau population gate for Stage 2a.

Builds a ZIP code pool around the subject property using three sources:
  A) The subject ZIP itself
  B) Any ZIP whose centroid is within 3 miles (from local zip_centroids.csv)
  C) Any ZIP that physically borders the subject ZIP (from local zip_adjacency.csv)

Queries ACS 5-Year population estimates for each ZIP in the pool, caches
results in a local SQLite database, and returns a pass/fail result.
"""

import csv
import logging
import math
import os
import re
import sqlite3
from datetime import datetime, timezone
from typing import Optional

import requests

logger = logging.getLogger(__name__)

DATA_DIR          = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
ZIP_CENTROIDS_PATH = os.path.join(DATA_DIR, "zip_centroids.csv")
ZIP_ADJACENCY_PATH = os.path.join(DATA_DIR, "zip_adjacency.csv")
CENSUS_CACHE_DB    = os.path.join(DATA_DIR, "census_cache.db")

# Module-level lazy cache — loaded once per process
_centroids: Optional[dict] = None
_adjacency: Optional[dict] = None


# ── Geometry ──────────────────────────────────────────────────────────────────

def haversine(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Return great-circle distance in miles between two lat/lng points."""
    R = 3959
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi    = math.radians(lat2 - lat1)
    dlambda = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


# ── ZIP parsing / lookup ──────────────────────────────────────────────────────

def parse_zip_from_address(address: str) -> Optional[str]:
    """Extract 5-digit ZIP code from an address string. Returns None if not found."""
    if not address:
        return None
    # Take the LAST 5-digit group — earlier ones are often street numbers.
    matches = re.findall(r"\b(\d{5})(?:-\d{4})?\b", address)
    return matches[-1] if matches else None


def census_geocode_zip(lat: float, lng: float) -> Optional[str]:
    """
    Use the Census Geocoder API to resolve lat/lng → ZIP code.
    Only called when ZIP cannot be parsed from the address string.
    """
    url = (
        "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"
        f"?x={lng}&y={lat}"
        "&benchmark=Public_AR_Current&vintage=Current_Current&layers=2&format=json"
    )
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        geographies = data.get("result", {}).get("geographies", {})
        zctas = geographies.get("2020 Census ZIP Code Tabulation Areas", [])
        if zctas:
            return str(zctas[0].get("ZCTA5", "")).strip() or None
    except Exception as exc:
        logger.warning("Census geocoder failed for (%.4f, %.4f): %s", lat, lng, exc)
    return None


# ── Static file loaders ───────────────────────────────────────────────────────

def load_zip_centroids() -> dict[str, tuple[float, float]]:
    """
    Load data/zip_centroids.csv into {zip: (lat, lng)}.
    Module-level cache — file is read only once per process.
    """
    global _centroids
    if _centroids is not None:
        return _centroids

    if not os.path.exists(ZIP_CENTROIDS_PATH):
        logger.error(
            "zip_centroids.csv not found at %s — run: python crexi/download_census_data.py",
            ZIP_CENTROIDS_PATH,
        )
        _centroids = {}
        return _centroids

    result: dict[str, tuple[float, float]] = {}
    with open(ZIP_CENTROIDS_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                result[row["zip"]] = (float(row["lat"]), float(row["lng"]))
            except (KeyError, ValueError):
                continue

    logger.debug("Loaded %d ZIP centroids", len(result))
    _centroids = result
    return _centroids


def load_zip_adjacency() -> dict[str, set[str]]:
    """
    Load data/zip_adjacency.csv into {zip: {adjacent_zip, ...}}.
    Module-level cache — file is read only once per process.
    """
    global _adjacency
    if _adjacency is not None:
        return _adjacency

    if not os.path.exists(ZIP_ADJACENCY_PATH):
        logger.error(
            "zip_adjacency.csv not found at %s — run: python crexi/download_census_data.py",
            ZIP_ADJACENCY_PATH,
        )
        _adjacency = {}
        return _adjacency

    result: dict[str, set[str]] = {}
    with open(ZIP_ADJACENCY_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                z  = row["zip"].strip()
                az = row["adjacent_zip"].strip()
                if z and az:
                    result.setdefault(z, set()).add(az)
            except KeyError:
                continue

    logger.debug("Loaded adjacency for %d ZIPs", len(result))
    _adjacency = result
    return _adjacency


# ── SQLite cache ──────────────────────────────────────────────────────────────

def _ensure_cache_db(db_path: str = CENSUS_CACHE_DB) -> None:
    """Create census_cache table if it doesn't exist."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS census_cache (
                zip_code   TEXT PRIMARY KEY,
                population INTEGER,
                queried_at TEXT
            )
        """)
        conn.commit()


def get_cached_population(zip_code: str, db_path: str = CENSUS_CACHE_DB,
                          max_age_days: int = 365) -> Optional[int]:
    """Return cached population for zip_code if within max_age_days, else None."""
    _ensure_cache_db(db_path)
    try:
        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                "SELECT population, queried_at FROM census_cache WHERE zip_code = ?",
                (zip_code,),
            ).fetchone()
        if not row:
            return None
        population, queried_at = row
        age_days = (datetime.now(timezone.utc) - datetime.fromisoformat(queried_at)).days
        if age_days <= max_age_days:
            return population
    except Exception as exc:
        logger.warning("Census cache read error for ZIP %s: %s", zip_code, exc)
    return None


def cache_population(zip_code: str, population: int,
                     db_path: str = CENSUS_CACHE_DB) -> None:
    """Upsert population for zip_code into the cache."""
    _ensure_cache_db(db_path)
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """INSERT INTO census_cache (zip_code, population, queried_at)
                   VALUES (?, ?, ?)
                   ON CONFLICT(zip_code) DO UPDATE SET
                       population = excluded.population,
                       queried_at = excluded.queried_at""",
                (zip_code, population, datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()
    except Exception as exc:
        logger.warning("Census cache write error for ZIP %s: %s", zip_code, exc)


# ── Census API ────────────────────────────────────────────────────────────────

def fetch_census_population(zip_code: str, api_key: str) -> Optional[int]:
    """
    Query ACS 5-Year Estimates for total population of a single ZIP code.
    Returns integer population or None if the API call fails or ZIP not found.
    """
    url = (
        "https://api.census.gov/data/2022/acs/acs5"
        f"?get=B01003_001E,NAME"
        f"&for=zip%20code%20tabulation%20area:{zip_code}"
        + (f"&key={api_key}" if api_key else "")
    )
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        # Response format: [["B01003_001E","NAME","zip code tabulation area"], ["12345","...", "XXXXX"]]
        if len(data) >= 2 and data[1][0] not in (None, "null", "-666666666"):
            return int(data[1][0])
    except (requests.RequestException, ValueError, IndexError) as exc:
        logger.warning("Census API failed for ZIP %s: %s", zip_code, exc)
    return None


# ── ZIP pool builder ──────────────────────────────────────────────────────────

def get_zip_pool(subject_zip: str, lat: float, lng: float,
                 centroids: dict, adjacency: dict,
                 radius_miles: float = 3.0) -> set[str]:
    """
    Build the ZIP pool for a subject property from three sources:
      A) The subject ZIP itself
      B) ZIPs whose centroid is within radius_miles of (lat, lng)
      C) ZIPs that physically border the subject ZIP (adjacency file)

    Returns deduplicated set of ZIP codes.
    """
    pool: set[str] = set()

    # Source A — subject ZIP
    if subject_zip:
        pool.add(subject_zip)

    # Source B — centroid neighbors within radius
    for z, (zlat, zlng) in centroids.items():
        if haversine(lat, lng, zlat, zlng) <= radius_miles:
            pool.add(z)

    # Source C — adjacent ZIPs (boundary safety net)
    if subject_zip in adjacency:
        pool.update(adjacency[subject_zip])

    return pool


# ── Main entry point ──────────────────────────────────────────────────────────

def check_population_gate(
    lat: float,
    lng: float,
    address: str,
    census_api_key: str,
    min_population: int = 30000,
    cache_days: int = 365,
    db_path: str = CENSUS_CACHE_DB,
) -> dict:
    """
    Triple-Gate Census population check for a deal location.

    Gates are tested in order; the first pass wins.
      Gate 1 (Subject Hit):    subject ZIP population >= min_population
      Gate 2 (Combo Hit):      subject ZIP + 1 closest neighbor >= 40,000
      Gate 3 (Neighbor Hit):   any neighbor ZIP within 3 miles >= min_population

    Returns a dict:
      passes           bool  — True if any gate passed
      population_3mi   int   — sum of ALL ZIP populations within 3 miles (subject
                               included). Rural deals with no nearby ZIPs = subject
                               ZIP alone. This is what goes in the deals table / UI.
      population_total int   — population value that triggered the gate
                               (subject pop | combo sum | best neighbor pop)
      zip_code         str   — subject ZIP (None if could not be determined)
      zip_pool_count   int   — 1 (subject) + number of neighbors within 3 miles
      pop_gate_passed  str   — 'subject_hit' | 'combo_hit' | 'neighbor_hit' | None
      skip_reason      str   — set if passes=False, else None
    """
    result = {
        "passes":           False,
        "population_3mi":   0,
        "population_total": 0,
        "zip_code":         None,
        "zip_pool_count":   0,
        "pop_gate_passed":  None,
        "skip_reason":      None,
    }

    # Step 1 — Determine subject ZIP
    subject_zip = parse_zip_from_address(address)
    if not subject_zip:
        logger.info("  ZIP not found in address — falling back to Census geocoder")
        subject_zip = census_geocode_zip(lat, lng)

    if not subject_zip:
        logger.warning("  Could not determine subject ZIP — skipping population gate")
        result["skip_reason"] = "population_check_failed: could not determine ZIP code"
        return result

    result["zip_code"] = subject_zip

    # Step 2 — Find neighbor ZIPs within 3-mile radius, sorted by distance
    centroids = load_zip_centroids()
    neighbors_by_distance: list[tuple[float, str]] = []
    for z, (zlat, zlng) in centroids.items():
        if z == subject_zip:
            continue
        d = haversine(lat, lng, zlat, zlng)
        if d <= 3.0:
            neighbors_by_distance.append((d, z))
    neighbors_by_distance.sort()

    result["zip_pool_count"] = 1 + len(neighbors_by_distance)
    logger.debug("  Subject ZIP: %s | Neighbors within 3 mi: %d",
                 subject_zip, len(neighbors_by_distance))

    # Helper: population lookup (cache-first, then Census API)
    def _get_pop(z: str) -> int:
        cached = get_cached_population(z, db_path=db_path, max_age_days=cache_days)
        if cached is not None:
            logger.debug("  ZIP %s: %d (cached)", z, cached)
            return cached
        fetched = fetch_census_population(z, census_api_key)
        if fetched is not None:
            cache_population(z, fetched, db_path=db_path)
            logger.debug("  ZIP %s: %d (fetched)", z, fetched)
            return fetched
        logger.debug("  ZIP %s: no data", z)
        return 0

    # Step 3 — Fetch populations for all ZIPs in pool (subject + all neighbors).
    # We always compute the full 3-mile sum so the UI column is consistent —
    # rural deals with no nearby ZIPs get subject-only; urban deals sum all of them.
    subject_pop   = _get_pop(subject_zip)
    neighbor_pops = {z: _get_pop(z) for _, z in neighbors_by_distance}
    population_3mi = subject_pop + sum(neighbor_pops.values())
    result["population_3mi"] = population_3mi

    # Gate 1: Subject ZIP population >= min_population
    if subject_pop >= min_population:
        result["passes"]           = True
        result["population_total"] = subject_pop
        result["pop_gate_passed"]  = "subject_hit"
        return result

    # Gate 2: Subject + closest single neighbor >= 40,000
    if neighbors_by_distance:
        closest_zip = neighbors_by_distance[0][1]
        closest_pop = neighbor_pops[closest_zip]
        combo = subject_pop + closest_pop
        if combo >= 40000:
            result["passes"]           = True
            result["population_total"] = combo
            result["pop_gate_passed"]  = "combo_hit"
            return result

    # Gate 3: Any neighbor within 3 miles >= min_population
    best_neighbor_pop = 0
    for _, z in neighbors_by_distance:
        pop = neighbor_pops[z]
        if pop > best_neighbor_pop:
            best_neighbor_pop = pop
        if pop >= min_population:
            result["passes"]           = True
            result["population_total"] = pop
            result["pop_gate_passed"]  = "neighbor_hit"
            return result

    # All three gates failed
    result["population_total"] = max(subject_pop, best_neighbor_pop)
    result["skip_reason"]      = "population_below_threshold"
    return result
