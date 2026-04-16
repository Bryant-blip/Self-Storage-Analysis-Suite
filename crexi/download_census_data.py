"""
crexi/download_census_data.py

One-time setup script — downloads two static Census Bureau files and converts
them to the CSV format expected by census_pop.py.

Run once from the project root:
    python crexi/download_census_data.py

Creates:
    data/zip_centroids.csv   (~33,000 rows)  zip, lat, lng
    data/zip_adjacency.csv   (~180,000 rows) zip, adjacent_zip

These files change only with decennial Census updates (~every 10 years).
Commit them to the repo after downloading.
"""

import csv
import io
import math
import os
import sys
import zipfile

import requests

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
os.makedirs(DATA_DIR, exist_ok=True)

CENTROIDS_OUT  = os.path.join(DATA_DIR, "zip_centroids.csv")
ADJACENCY_OUT  = os.path.join(DATA_DIR, "zip_adjacency.csv")

# 2023 ZCTA Gazetteer — tab-delimited, zipped
GAZETTEER_URL = (
    "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/"
    "2023_Gazetteer/2023_Gaz_zcta_national.zip"
)

# 2020 ZCTA-to-ZCTA adjacency file — pipe-delimited text
ADJACENCY_URL = (
    "https://www2.census.gov/geo/docs/maps-data/data/rel2020/"
    "zcta520/tab20_zcta520_zcta520_natl.txt"
)


def download_bytes(url: str, label: str) -> bytes:
    print(f"Downloading {label}...")
    resp = requests.get(url, timeout=120, stream=True)
    resp.raise_for_status()
    chunks = []
    total = 0
    for chunk in resp.iter_content(chunk_size=65536):
        chunks.append(chunk)
        total += len(chunk)
        print(f"  {total:,} bytes", end="\r")
    print(f"  {total:,} bytes — done")
    return b"".join(chunks)


def build_centroids():
    raw = download_bytes(GAZETTEER_URL, "ZCTA Gazetteer (centroids)")

    # The zip archive contains a single .txt file
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        names = [n for n in zf.namelist() if n.endswith(".txt")]
        if not names:
            print("ERROR: No .txt file found inside Gazetteer zip")
            sys.exit(1)
        txt_bytes = zf.read(names[0])

    lines = txt_bytes.decode("utf-8").splitlines()
    # Tab-delimited; first line is header
    # Columns include: GEOID  ALAND  AWATER  ALAND_SQMI  AWATER_SQMI  INTPTLAT  INTPTLONG
    header = [h.strip() for h in lines[0].split("\t")]
    try:
        idx_zip = header.index("GEOID")
        idx_lat = header.index("INTPTLAT")
        idx_lng = header.index("INTPTLONG")
    except ValueError:
        # Some years use slightly different column names
        print(f"  Header columns: {header}")
        # Try alternate names
        alt_lat = next((i for i, h in enumerate(header) if "LAT" in h.upper()), None)
        alt_lng = next((i for i, h in enumerate(header) if "LON" in h.upper() or "LNG" in h.upper()), None)
        alt_zip = next((i for i, h in enumerate(header) if "GEOID" in h.upper() or "ZCTA" in h.upper()), None)
        if alt_zip is None or alt_lat is None or alt_lng is None:
            print("ERROR: Could not identify required columns in Gazetteer file")
            sys.exit(1)
        idx_zip, idx_lat, idx_lng = alt_zip, alt_lat, alt_lng

    written = 0
    with open(CENTROIDS_OUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["zip", "lat", "lng"])
        for line in lines[1:]:
            parts = line.split("\t")
            if len(parts) <= max(idx_zip, idx_lat, idx_lng):
                continue
            try:
                z   = parts[idx_zip].strip().zfill(5)
                lat = float(parts[idx_lat].strip())
                lng = float(parts[idx_lng].strip())
                writer.writerow([z, lat, lng])
                written += 1
            except (ValueError, IndexError):
                continue

    print(f"  Wrote {written:,} rows -> {CENTROIDS_OUT}")
    return written


def build_adjacency(max_miles: float = 10.0):
    """
    Build ZIP adjacency by finding all pairs of ZIPs whose centroids are
    within max_miles of each other. Uses a degree-grid index so the full
    O(n^2) comparison is never done — only candidate pairs within the same
    grid cell are compared.

    Census does not publish a downloadable ZCTA-to-ZCTA adjacency file, so
    we derive adjacency from the centroid distances instead. A 10-mile cutoff
    captures essentially all real neighboring ZIPs in suburban/urban areas.
    """
    print("Building adjacency from centroids (pairs within 10 miles)...")

    if not os.path.exists(CENTROIDS_OUT):
        print(f"  ERROR: {CENTROIDS_OUT} not found — run build_centroids() first")
        return 0

    # Load centroids
    zips: list[tuple[str, float, float]] = []
    with open(CENTROIDS_OUT, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                zips.append((row["zip"], float(row["lat"]), float(row["lng"])))
            except (KeyError, ValueError):
                continue
    print(f"  Loaded {len(zips):,} centroids")

    # Build a 1-degree lat/lng grid index (~69 miles per degree)
    # Cells that are within 1 grid cell of each other can contain adjacent ZIPs
    from collections import defaultdict
    grid: dict[tuple[int, int], list] = defaultdict(list)
    for z, lat, lng in zips:
        cell = (int(lat), int(lng))
        grid[cell].append((z, lat, lng))

    # For each ZIP, check only ZIPs in neighboring grid cells (3x3 window)
    written = 0

    # 1 degree latitude ~ 69 miles, so a 3x3 grid window covers ~207 miles
    # which is more than enough for a 10-mile radius
    MILES_PER_DEGREE_LAT = 69.0

    with open(ADJACENCY_OUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["zip", "adjacent_zip"])

        for z1, lat1, lng1 in zips:
            cell_lat = int(lat1)
            cell_lng = int(lng1)
            candidates = []
            for dlat in (-1, 0, 1):
                for dlng in (-1, 0, 1):
                    candidates.extend(grid.get((cell_lat + dlat, cell_lng + dlng), []))

            for z2, lat2, lng2 in candidates:
                if z1 >= z2:   # process each pair once; skip self
                    continue
                # Quick bounding-box pre-filter before full Haversine
                if abs(lat2 - lat1) * MILES_PER_DEGREE_LAT > max_miles:
                    continue
                dist = haversine_simple(lat1, lng1, lat2, lng2)
                if dist <= max_miles:
                    writer.writerow([z1, z2])
                    writer.writerow([z2, z1])
                    written += 2

    print(f"  Wrote {written:,} rows -> {ADJACENCY_OUT}")
    return written


def haversine_simple(lat1, lng1, lat2, lng2):
    R = 3959
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi    = math.radians(lat2 - lat1)
    dlambda = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


if __name__ == "__main__":
    print("=" * 60)
    print("Census Bureau data download")
    print("=" * 60)

    c_rows = build_centroids()
    print()
    a_rows = build_adjacency()

    print()
    print("=" * 60)
    print("Done.")
    print(f"  zip_centroids.csv : {c_rows:,} ZIPs")
    print(f"  zip_adjacency.csv : {a_rows:,} adjacency pairs")
    print()
    print("Commit these files to the repo — they rarely change.")
    print("=" * 60)
