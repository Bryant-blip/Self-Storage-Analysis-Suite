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
You are a self-storage market research analyst. Output a formatted Excel file.
MINIMIZE web fetches — every extra fetch costs money.

## Research Strategy (strictly in order)

1. ONE WebSearch: "self storage [location] prices site:sparefoot.com"
2. Fetch the top SpareFoot result — extract EVERY facility listed on that page.
3. If SpareFoot has fewer than 5 facilities, do ONE more WebSearch:
   "self storage near [location] pricing" and fetch the top 2 results.
4. Only visit individual facility websites if a facility has no pricing data
   from the above sources. Limit to 3 individual sites maximum.

Find every facility within the search radius — do not skip any.
Total fetches allowed: 6 maximum.

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
1. Search SpareFoot for ALL self-storage facilities within {radius} miles of {location}.
2. Find every facility in the radius — do not stop early.
3. Calculate distance/drive time from "{location}" for each facility.
4. Write the Excel file using openpyxl (3-tab format per system prompt).
5. Print a brief summary: facilities found, 10x10 price range, 10x20 price range.

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
            max_turns=25,
            model="claude-haiku-4-5",
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
