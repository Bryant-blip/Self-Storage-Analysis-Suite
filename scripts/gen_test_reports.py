"""Generate 3 test Excel reports to verify facility-type proforma logic."""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # run from repo root or scripts/
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from comps_pipeline import write_comps_excel

# Fake facilities with both drive-up and CC pricing
FAKE_FACILITIES = [
    {
        "name": "StorageMart - Main St",
        "address": "123 Main St, Anytown, TX 75001",
        "distance_miles": 1.2,
        "drive_time_min": 4,
        "phone": "(555) 111-2222",
        "website": "https://example.com/storagemart",
        "pricing": [
            {"size": "5x5",   "type": "drive_up",        "web_rate": 45,  "in_store_rate": 55},
            {"size": "5x10",  "type": "drive_up",        "web_rate": 70,  "in_store_rate": 85},
            {"size": "10x10", "type": "drive_up",        "web_rate": 115, "in_store_rate": 135},
            {"size": "10x15", "type": "drive_up",        "web_rate": 155, "in_store_rate": 180},
            {"size": "10x20", "type": "drive_up",        "web_rate": 195, "in_store_rate": 225},
            {"size": "10x30", "type": "drive_up",        "web_rate": 270, "in_store_rate": 310},
            {"size": "5x5",   "type": "climate_control", "web_rate": 65,  "in_store_rate": 75},
            {"size": "5x10",  "type": "climate_control", "web_rate": 95,  "in_store_rate": 110},
            {"size": "10x10", "type": "climate_control", "web_rate": 155, "in_store_rate": 175},
            {"size": "10x15", "type": "climate_control", "web_rate": 210, "in_store_rate": 240},
            {"size": "10x20", "type": "climate_control", "web_rate": 265, "in_store_rate": 300},
        ],
    },
    {
        "name": "Extra Space Storage",
        "address": "456 Oak Ave, Anytown, TX 75001",
        "distance_miles": 2.5,
        "drive_time_min": 7,
        "phone": "(555) 333-4444",
        "website": "https://example.com/extraspace",
        "pricing": [
            {"size": "5x5",   "type": "drive_up",        "web_rate": 50,  "in_store_rate": 60},
            {"size": "5x10",  "type": "drive_up",        "web_rate": 75,  "in_store_rate": 90},
            {"size": "10x10", "type": "drive_up",        "web_rate": 120, "in_store_rate": 140},
            {"size": "10x15", "type": "drive_up",        "web_rate": 160, "in_store_rate": 190},
            {"size": "10x20", "type": "drive_up",        "web_rate": 200, "in_store_rate": 230},
            {"size": "5x5",   "type": "climate_control", "web_rate": 70,  "in_store_rate": 80},
            {"size": "5x10",  "type": "climate_control", "web_rate": 100, "in_store_rate": 115},
            {"size": "10x10", "type": "climate_control", "web_rate": 160, "in_store_rate": 180},
            {"size": "10x15", "type": "climate_control", "web_rate": 220, "in_store_rate": 250},
            {"size": "10x20", "type": "climate_control", "web_rate": 275, "in_store_rate": 310},
            {"size": "10x30", "type": "climate_control", "web_rate": 380, "in_store_rate": 420},
        ],
    },
]

os.makedirs("reports/tests", exist_ok=True)

tests = [
    ("reports/tests/test_SINGLE_STORY_5.5ac.xlsx", 5.5, 350000, "100 Rural Rd, Anytown, TX 75001"),
    ("reports/tests/test_MULTI_STORY_1.6ac.xlsx",   1.6, 800000, "200 Urban Blvd, Anytown, TX 75001"),
    ("reports/tests/test_MIXED_3.0ac.xlsx",          3.0, 500000, "300 Suburban Dr, Anytown, TX 75001"),
]

for path, acres, price, location in tests:
    print(f"Generating {path} ({acres} acres)...")
    write_comps_excel(
        facilities=FAKE_FACILITIES,
        output_path=path,
        location=location,
        acres=acres,
        asking_price=price,
    )
    print(f"  Done -> {path}")

print("\nAll 3 test files generated.")
