"""
crexi/census_pop.py

Census Bureau population gate for Stage 2a.

Builds a city pool around the subject property:
  A) The subject city (parsed from address or geocoded)
  B) Any incorporated place whose centroid is within 3 miles

Queries ACS 5-Year population estimates for each city in the pool, caches
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

DATA_DIR             = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
PLACE_CENTROIDS_PATH = os.path.join(DATA_DIR, "place_centroids.csv")
CENSUS_CACHE_DB      = os.path.join(DATA_DIR, "census_cache.db")

_places: Optional[dict] = None
_place_name_lookup: Optional[dict] = None


# ── Geometry ──────────────────────────────────────────────────────────────────

def haversine(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Return great-circle distance in miles between two lat/lng points."""
    R = 3959
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi    = math.radians(lat2 - lat1)
    dlambda = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


# ── Address parsing ──────────────────────────────────────────────────────────

def parse_city_state_from_address(address: str) -> tuple[Optional[str], Optional[str]]:
    """
    Extract city and state abbreviation from an address string.
    E.g. "123 Main St, Austin, TX 75001" -> ("Austin", "TX")
    """
    if not address:
        return None, None
    m = re.search(r",\s*([A-Za-z][A-Za-z .'-]+?)\s*,\s*([A-Z]{2})\b", address)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    m = re.search(r"([A-Za-z][A-Za-z .'-]+?)\s*,\s*([A-Z]{2})\s+\d{5}", address)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    m = re.search(r"([A-Za-z][A-Za-z .'-]+?)\s*,\s*([A-Z]{2})\s*$", address)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return None, None


def parse_zip_from_address(address: str) -> Optional[str]:
    """Extract 5-digit ZIP code from an address string. Returns None if not found."""
    if not address:
        return None
    matches = re.findall(r"\b(\d{5})(?:-\d{4})?\b", address)
    return matches[-1] if matches else None


# ── Place centroids loader ───────────────────────────────────────────────────

def _strip_place_suffix(name: str) -> str:
    """Remove Census place type suffixes like 'city', 'town', 'CDP', etc."""
    return re.sub(r"\s+(city|town|village|borough|CDP|municipality|plantation|comunidad)$",
                  "", name, flags=re.IGNORECASE).strip()


def load_place_centroids() -> dict:
    """
    Load data/place_centroids.csv into a dict keyed by (state_fips, place_fips).
    Also builds a reverse lookup by (name_lower, state_abbrev) -> (state_fips, place_fips).
    Module-level cache — file is read only once per process.
    """
    global _places, _place_name_lookup
    if _places is not None:
        return _places

    if not os.path.exists(PLACE_CENTROIDS_PATH):
        logger.error(
            "place_centroids.csv not found at %s — run: python crexi/download_census_data.py",
            PLACE_CENTROIDS_PATH,
        )
        _places = {}
        _place_name_lookup = {}
        return _places

    _places = {}
    _place_name_lookup = {}

    with open(PLACE_CENTROIDS_PATH, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                key = (row["state_fips"], row["place_fips"])
                raw_name = row["name"]
                clean_name = _strip_place_suffix(raw_name)
                entry = {
                    "name": clean_name,
                    "raw_name": raw_name,
                    "state": row["state"],
                    "lat": float(row["lat"]),
                    "lng": float(row["lng"]),
                }
                _places[key] = entry
                name_key = (clean_name.lower(), row["state"])
                if name_key not in _place_name_lookup:
                    _place_name_lookup[name_key] = key
            except (KeyError, ValueError):
                continue

    logger.debug("Loaded %d place centroids", len(_places))
    return _places


def get_place_name_lookup() -> dict:
    """Return the {(name_lower, state): (state_fips, place_fips)} reverse lookup."""
    global _place_name_lookup
    if _place_name_lookup is None:
        load_place_centroids()
    return _place_name_lookup


# ── Census Geocoder ──────────────────────────────────────────────────────────

def census_geocode_place(lat: float, lng: float) -> Optional[tuple[str, str]]:
    """
    Use the Census Geocoder API to resolve lat/lng -> (state_fips, place_fips).
    Returns None if the location is not within an incorporated place.
    """
    url = (
        "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"
        f"?x={lng}&y={lat}"
        "&benchmark=Public_AR_Current&vintage=Current_Current&layers=28&format=json"
    )
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        geographies = data.get("result", {}).get("geographies", {})
        places = geographies.get("Incorporated Places", [])
        if places:
            geoid = str(places[0].get("GEOID", "")).strip()
            if len(geoid) >= 7:
                return (geoid[:2], geoid[2:])
    except Exception as exc:
        logger.warning("Census geocoder failed for (%.4f, %.4f): %s", lat, lng, exc)
    return None


# ── SQLite cache ──────────────────────────────────────────────────────────────

def _ensure_cache_db(db_path: str = CENSUS_CACHE_DB) -> None:
    """Create census_place_cache table if it doesn't exist."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS census_place_cache (
                place_key  TEXT PRIMARY KEY,
                name       TEXT,
                state      TEXT,
                population INTEGER,
                queried_at TEXT
            )
        """)
        conn.commit()


def get_cached_population(place_key: str, db_path: str = CENSUS_CACHE_DB,
                          max_age_days: int = 365) -> Optional[int]:
    """Return cached population for place_key if within max_age_days, else None."""
    _ensure_cache_db(db_path)
    try:
        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                "SELECT population, queried_at FROM census_place_cache WHERE place_key = ?",
                (place_key,),
            ).fetchone()
        if not row:
            return None
        population, queried_at = row
        age_days = (datetime.now(timezone.utc) - datetime.fromisoformat(queried_at)).days
        if age_days <= max_age_days:
            return population
    except Exception as exc:
        logger.warning("Census cache read error for %s: %s", place_key, exc)
    return None


def cache_population(place_key: str, population: int, name: str = "",
                     state: str = "", db_path: str = CENSUS_CACHE_DB) -> None:
    """Upsert population for a place into the cache."""
    _ensure_cache_db(db_path)
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """INSERT INTO census_place_cache (place_key, name, state, population, queried_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(place_key) DO UPDATE SET
                       population = excluded.population,
                       queried_at = excluded.queried_at""",
                (place_key, name, state, population,
                 datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()
    except Exception as exc:
        logger.warning("Census cache write error for %s: %s", place_key, exc)


# ── Census API ────────────────────────────────────────────────────────────────

def fetch_census_place_population(state_fips: str, place_fips: str,
                                  api_key: str) -> Optional[int]:
    """
    Query ACS 5-Year Estimates for total population of an incorporated place.
    Returns integer population or None if the API call fails.
    """
    url = (
        "https://api.census.gov/data/2023/acs/acs5"
        f"?get=B01003_001E,NAME"
        f"&for=place:{place_fips}"
        f"&in=state:{state_fips}"
        + (f"&key={api_key}" if api_key else "")
    )
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if len(data) >= 2 and data[1][0] not in (None, "null", "-666666666"):
            return int(data[1][0])
    except (requests.RequestException, ValueError, IndexError) as exc:
        logger.warning("Census API failed for place %s_%s: %s", state_fips, place_fips, exc)
    return None


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
    Triple-Gate Census population check for a deal location using city/place data.

    Gates are tested in order; the first pass wins.
      Gate 1 (Subject Hit):    subject city population >= min_population
      Gate 2 (Combo Hit):      subject city + closest neighbor city within 3 mi >= 40,000
      Gate 3 (Neighbor Hit):   any neighbor city within 3 miles >= min_population

    Returns a dict:
      passes           bool  — True if any gate passed
      population_3mi   int   — sum of ALL city populations within 3 miles
      population_total int   — population value that triggered the gate
      city_name        str   — subject city display name (e.g. "Austin, TX")
      zip_code         str   — subject ZIP (parsed from address, for DB compatibility)
      zip_pool_count   int   — 1 (subject) + number of neighbor cities within 3 miles
      pop_gate_passed  str   — 'subject_hit' | 'combo_hit' | 'neighbor_hit' | None
      skip_reason      str   — set if passes=False, else None
    """
    result = {
        "passes":           False,
        "population_3mi":   0,
        "population_total": 0,
        "city_name":        None,
        "zip_code":         parse_zip_from_address(address),
        "zip_pool_count":   0,
        "pop_gate_passed":  None,
        "skip_reason":      None,
    }

    # Step 1 — Determine subject city FIPS
    places = load_place_centroids()
    name_lookup = get_place_name_lookup()

    city, state = parse_city_state_from_address(address)
    subject_fips = None

    if city and state:
        subject_fips = name_lookup.get((city.lower(), state))

    if not subject_fips:
        logger.info("  City not found in address — falling back to Census geocoder")
        subject_fips = census_geocode_place(lat, lng)

    if not subject_fips:
        logger.warning("  Could not determine subject city — skipping population gate")
        result["skip_reason"] = "population_check_failed: could not determine city"
        return result

    subject_info = places.get(subject_fips, {})
    subject_display = f"{subject_info.get('name', '?')}, {subject_info.get('state', '?')}"
    result["city_name"] = subject_display

    # Step 2 — Find neighbor cities within 3-mile radius, sorted by distance
    neighbors_by_distance: list[tuple[float, tuple[str, str]]] = []
    for fips_key, info in places.items():
        if fips_key == subject_fips:
            continue
        d = haversine(lat, lng, info["lat"], info["lng"])
        if d <= 3.0:
            neighbors_by_distance.append((d, fips_key))
    neighbors_by_distance.sort()

    result["zip_pool_count"] = 1 + len(neighbors_by_distance)
    logger.debug("  Subject city: %s | Neighbors within 3 mi: %d",
                 subject_display, len(neighbors_by_distance))

    # Helper: population lookup (cache-first, then Census API)
    def _make_key(fips: tuple[str, str]) -> str:
        return f"{fips[0]}_{fips[1]}"

    def _get_pop(fips: tuple[str, str]) -> int:
        pk = _make_key(fips)
        cached = get_cached_population(pk, db_path=db_path, max_age_days=cache_days)
        if cached is not None:
            logger.debug("  %s: %d (cached)", pk, cached)
            return cached
        fetched = fetch_census_place_population(fips[0], fips[1], census_api_key)
        if fetched is not None:
            info = places.get(fips, {})
            cache_population(pk, fetched, name=info.get("name", ""),
                             state=info.get("state", ""), db_path=db_path)
            logger.debug("  %s: %d (fetched)", pk, fetched)
            return fetched
        logger.debug("  %s: no data", pk)
        return 0

    # Step 3 — Fetch populations for subject + all neighbors
    subject_pop   = _get_pop(subject_fips)
    neighbor_pops = {fips: _get_pop(fips) for _, fips in neighbors_by_distance}
    population_3mi = subject_pop + sum(neighbor_pops.values())
    result["population_3mi"] = population_3mi

    # Gate 1: Subject city population >= min_population
    if subject_pop >= min_population:
        result["passes"]           = True
        result["population_total"] = subject_pop
        result["pop_gate_passed"]  = "subject_hit"
        return result

    # Gate 2: Subject + closest neighbor city within 3 miles >= 40,000
    if neighbors_by_distance:
        closest_fips = neighbors_by_distance[0][1]
        closest_pop  = neighbor_pops[closest_fips]
        combo = subject_pop + closest_pop
        if combo >= 40000:
            result["passes"]           = True
            result["population_total"] = combo
            result["pop_gate_passed"]  = "combo_hit"
            return result

    # Gate 3: Any neighbor city within 3 miles >= min_population
    best_neighbor_pop = 0
    for _, fips in neighbors_by_distance:
        pop = neighbor_pops[fips]
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
