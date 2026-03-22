#!/usr/bin/env python3
"""
Self Storage Market Rent Comps Agent

Finds self-storage competitors near a subject property, collects online and
in-store pricing, calculates distance/drive time from the subject, and
produces a formatted Excel spreadsheet.

Usage:
    python storage_comps_agent.py "Austin, TX 78701" --radius 5
    python storage_comps_agent.py  (interactive prompts)
"""

import os
import sys
import argparse
from datetime import date

import anyio
from claude_agent_sdk import (
    query,
    ClaudeAgentOptions,
    ResultMessage,
    AssistantMessage,
    TextBlock,
)

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── System prompt ──────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """
You are a self-storage market research analyst. Your job is to find EVERY
self-storage facility within the search radius and collect ACTUAL PRICING
for as many unit types as possible. Output a formatted Excel file.

## Research Strategy

### Phase 1 — Find all facilities
1. WebSearch: "self storage units near [location]"
2. WebSearch: "self storage [city] [state] prices"
3. From these results, build a list of every facility within the radius.

### Phase 2 — Collect pricing for each facility
Work through facilities ONE AT A TIME. For each facility:
1. WebSearch: "[facility name] [city] self storage unit prices"
2. **Check search snippets first** — prices often appear directly in result text.
3. **Fetch priority order** (try until you get pricing):
   a. StorageUnits.com or SelfStorage.com result (static HTML, most reliable)
   b. StorageCafe.com result
   c. The facility's own website (WARNING: many use JavaScript and return empty
      content — if fetch returns no pricing, move on immediately)
   d. SpareFoot result (often blocked — try last)
4. If the first fetch has no pricing, try up to 2 MORE pages from search results.
5. If all fetches fail, try ONE fallback search:
   "site:storageunits.com [facility name] [city]"
6. Extract ALL unit types and rates from whichever source works.

### Common issues:
- **JavaScript sites:** Major chains (Public Storage, Extra Space, CubeSmart,
  Life Storage) render prices via JS. Their direct websites return empty pricing.
  ALWAYS use aggregator sites for these chains.
- **Empty/blocked pages:** If WebFetch returns a CAPTCHA, "Access Denied," or
  very little text, do NOT retry that domain. Move to the next source.
- **Rate formats:** Prices appear as "$89", "$89.00", "$89/mo", "From $89".
  Extract the dollar amount.

## Target Unit Types
Collect pricing for ALL of these sizes when available:
5x5, 5x10, 10x10, 10x15, 10x20, 10x25, 10x30, Parking/Vehicle Storage

Do NOT limit to just 10x10 and 10x20. Include every unit size the facility offers.

## Collect per facility:
Name | Address | Phone | Website | Online rates by unit size | In-store rates |
Climate controlled (Yes/No) | Promotions | Distance (mi) & drive time (min)
from subject using Haversine formula, 25 mph estimate.

Include only facilities within the search radius. Sort closest first.

## Excel — 3 Tabs

Tab 1 "Comps Detail" — one row per facility × unit type
Columns: Facility Name | Address | Distance (mi) | Unit Type | Sq Ft |
  Climate Controlled | Online Rate ($/mo) | In-Store Rate ($/mo) |
  Notes/Promotions | Date Pulled | Source URL | Drive Time (min)
Format: bold header (#BDD7EE), currency, auto-width columns, freeze row 1

Tab 2 "Market Summary" — two sections separated by a blank row
  Section 1 header: "CLIMATE CONTROLLED UNITS" (bold, green #E2EFDA)
  Section 2 header: "DRIVE UP / STANDARD UNITS" (bold, orange #FCE4D6)
  Each section columns: Unit Type | Sq Ft | Avg Online | Min Online | Max Online |
    Avg In-Store | Min In-Store | Max In-Store | # Comps
  Currency format on all rate columns.

Tab 3 "Facility List" — one row per facility
Columns: Facility Name | Address | Distance (mi) | Drive Time (min) | Phone | Website
Format: bold header (#FCE4D6), sorted by distance

## Rules
- Never fabricate pricing. Mark missing data as N/A.
- Distance required on every row — exclude facility if unknown.
- Write Excel via openpyxl using the Bash tool.
"""


# ── Agent runner ───────────────────────────────────────────────────────────────
async def run_agent(location: str, radius: float) -> None:
    today = date.today().strftime("%b-%d-%y")
    safe_loc = location.replace(" ", "_").replace(",", "").replace("/", "-").strip()
    output_file = os.path.join(OUTPUT_DIR, f"storage_comps_{safe_loc}_{today}.xlsx")

    prompt = f"""
Find self-storage market rent comps for:
  Location : {location}
  Radius   : {radius} miles
  Date     : {date.today().strftime("%B %d, %Y")}
  Save to  : {output_file}

Instructions:
1. Find ALL self-storage facilities within {radius} miles of {location}.
2. For each facility, search for pricing (use aggregator sites like StorageUnits.com
   and SelfStorage.com — they return static HTML with actual prices).
3. Collect ALL unit sizes (5x5, 5x10, 10x10, 10x15, 10x20, 10x25, 10x30).
4. Calculate distance/drive time from "{location}" for each facility.
5. Write the Excel file using openpyxl (3-tab format per system prompt).
6. Print a brief summary: facilities found, price ranges by unit size.

No fabricated data — mark missing as N/A.
"""

    print()
    print("=" * 65)
    print("  SELF STORAGE MARKET RENT COMPS AGENT")
    print("=" * 65)
    print(f"  Location : {location}")
    print(f"  Radius   : {radius} miles")
    print(f"  Output   : {output_file}")
    print("=" * 65)
    print()

    async for message in query(
        prompt=prompt,
        options=ClaudeAgentOptions(
            system_prompt=SYSTEM_PROMPT,
            allowed_tools=["WebSearch", "WebFetch", "Bash", "Write"],
            permission_mode="acceptEdits",
            cwd=BASE_DIR,
            max_turns=75,
            model="claude-sonnet-4-6",
        ),
    ):
        if isinstance(message, AssistantMessage):
            for block in message.content:
                if isinstance(block, TextBlock) and block.text.strip():
                    print(block.text)
        elif isinstance(message, ResultMessage):
            print()
            print("=" * 65)
            print("  DONE")
            print("=" * 65)
            if message.result:
                print(message.result)
            print(f"\n  File saved to: {output_file}")
            print()


# ── CLI ────────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Find self-storage market rent comparables for a given area.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python storage_comps_agent.py "78701"
  python storage_comps_agent.py "Austin, TX 78701" --radius 3
  python storage_comps_agent.py "123 Main St, Denver CO" --radius 10
        """,
    )
    parser.add_argument(
        "location",
        nargs="?",
        help="Subject property location (address, city, or zip code)",
    )
    parser.add_argument(
        "--radius",
        type=float,
        default=5.0,
        help="Search radius in miles (default: 5)",
    )

    args = parser.parse_args()

    # Interactive prompts if not provided via CLI
    if not args.location:
        print()
        print("Self Storage Market Rent Comps Agent")
        print("-" * 40)
        args.location = input("Subject property location (address, city, or zip): ").strip()
        if not args.location:
            print("Error: Location is required.")
            sys.exit(1)

        radius_input = input(f"Search radius in miles [default: {args.radius}]: ").strip()
        if radius_input:
            try:
                args.radius = float(radius_input)
            except ValueError:
                print(f"Invalid radius — using default: {args.radius} miles")

    anyio.run(run_agent, args.location, args.radius)


if __name__ == "__main__":
    main()
