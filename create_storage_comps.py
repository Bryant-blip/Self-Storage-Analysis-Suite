#!/usr/bin/env python3
"""
Create storage comps Excel file with distance calculations.
"""
import math
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from datetime import datetime

# Subject location coordinates (Paseo del Norte & Woodmont, Albuquerque, NM 87114)
SUBJECT_LAT = 35.1404
SUBJECT_LON = -106.5644

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate distance in miles using Haversine formula."""
    R = 3959  # Earth's radius in miles
    
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    
    a = math.sin(delta_lat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))
    
    return R * c

def calculate_drive_time(distance_miles):
    """Estimate drive time at 25 mph average."""
    return round(distance_miles / 25 * 60)

# Facility data compiled from search
facilities_data = [
    {
        'name': 'Golden Target Self Storage',
        'address': '8601 Montgomery Blvd NE',
        'city': 'Albuquerque, NM 87111',
        'phone': '(505) 323-8703',
        'website': 'https://www.goldentargetselfstorage.com',
        'lat': 35.1519, 'lon': -106.5628,
        'units': [
            {'size': '5x5', 'sqft': 25, 'type': 'Drive-Up', 'cc': 'No', 'online': 29, 'instore': 41},
            {'size': '5x10', 'sqft': 50, 'type': 'Drive-Up', 'cc': 'No', 'online': 49, 'instore': 65},
            {'size': '10x10', 'sqft': 100, 'type': 'Drive-Up', 'cc': 'No', 'online': 90, 'instore': None},
            {'size': '10x15', 'sqft': 150, 'type': 'Drive-Up', 'cc': 'No', 'online': 125, 'instore': 184},
            {'size': '10x20', 'sqft': 200, 'type': 'Drive-Up', 'cc': 'No', 'online': 179, 'instore': 235},
            {'size': '5x10', 'sqft': 50, 'type': 'Inside Hallway', 'cc': 'No', 'online': 50, 'instore': 55},
            {'size': '10x10', 'sqft': 100, 'type': 'Inside Hallway', 'cc': 'No', 'online': 109, 'instore': 119},
        ]
    },
    {
        'name': 'Extra Space Storage - Lomas',
        'address': '11820 Lomas Boulevard NE',
        'city': 'Albuquerque, NM 87112',
        'phone': '(505) 328-7804',
        'website': 'https://www.extraspace.com',
        'lat': 35.1215, 'lon': -106.5456,
        'units': [
            {'size': '5x5', 'sqft': 25, 'type': 'Drive-Up', 'cc': 'No', 'online': 54, 'instore': 57},
            {'size': '5x10', 'sqft': 50, 'type': 'Drive-Up', 'cc': 'No', 'online': 83, 'instore': 87},
            {'size': '10x10', 'sqft': 100, 'type': 'Drive-Up', 'cc': 'No', 'online': 115, 'instore': 121},
        ]
    },
    {
        'name': 'Hideaway Self Storage',
        'address': '10408 Menaul Boulevard NE',
        'city': 'Albuquerque, NM 87112',
        'phone': '(505) 807-9897',
        'website': 'https://www.storemystuffabq.com',
        'lat': 35.1134, 'lon': -106.5456,
        'units': [
            {'size': '5x10', 'sqft': 50, 'type': 'Drive-Up', 'cc': 'No', 'online': 109, 'instore': None},
        ]
    },
    {
        'name': 'Volcano Self Storage',
        'address': '3000 Todos Santos Street',
        'city': 'Albuquerque, NM 87120',
        'phone': '(505) 830-1400',
        'website': 'https://www.volcanoselfstorage.com',
        'lat': 35.0876, 'lon': -106.6321,
        'units': [
            {'size': '5x5', 'sqft': 25, 'type': 'Drive-Up', 'cc': 'No', 'online': 44, 'instore': None},
            {'size': '10x10', 'sqft': 100, 'type': 'Drive-Up', 'cc': 'No', 'online': 79, 'instore': None},
            {'size': '10x15', 'sqft': 150, 'type': 'Drive-Up', 'cc': 'No', 'online': 108, 'instore': None},
        ]
    },
    {
        'name': 'U-Stor-It - Baylor',
        'address': '2640 Baylor Drive SE',
        'city': 'Albuquerque, NM 87106',
        'phone': '(505) 266-3035',
        'website': 'https://www.ustoritministorage.com',
        'lat': 35.0833, 'lon': -106.5678,
        'units': [
            {'size': '5x5', 'sqft': 25, 'type': 'Drive-Up', 'cc': 'No', 'online': 35, 'instore': None},
            {'size': '10x10', 'sqft': 100, 'type': 'Drive-Up', 'cc': 'No', 'online': 75, 'instore': 149},
        ]
    },
    {
        'name': 'U-Stor-It - Cutler',
        'address': '4701 Cutler Avenue NE',
        'city': 'Albuquerque, NM 87109',
        'phone': '(505) 884-8886',
        'website': 'https://www.ustoritministorage.com',
        'lat': 35.1589, 'lon': -106.5133,
        'units': [
            {'size': '5x5', 'sqft': 25, 'type': 'Drive-Up', 'cc': 'No', 'online': 64, 'instore': None},
            {'size': '10x10', 'sqft': 100, 'type': 'Drive-Up', 'cc': 'No', 'online': 75, 'instore': 89},
        ]
    },
    {
        'name': 'U-Stor-It - Indian School',
        'address': '8519 Indian School Road NE',
        'city': 'Albuquerque, NM 87112',
        'phone': '(505) 275-3035',
        'website': 'https://www.ustoritministorage.com',
        'lat': 35.1045, 'lon': -106.5267,
        'units': [
            {'size': '10x10', 'sqft': 100, 'type': 'Drive-Up', 'cc': 'No', 'online': 159, 'instore': 169},
        ]
    },
    {
        'name': 'AAA U-Lock-It - Moon',
        'address': '2125 Moon Street NE',
        'city': 'Albuquerque, NM 87112',
        'phone': '(505) 275-4002',
        'website': 'https://www.aaaulockitselfstorage.com',
        'lat': 35.1089, 'lon': -106.5522,
        'units': [
            {'size': '5x5', 'sqft': 25, 'type': 'Drive-Up', 'cc': 'No', 'online': 20, 'instore': None},
            {'size': '10x10', 'sqft
