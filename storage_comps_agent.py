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

## Research Strategy — Tiered Escalation (cheapest first)

Always start at Tier 1. Only escalate to the next tier for facilities that
still have missing prices. Track which facilities have prices and which don't.

### Tier 1 — Search snippets (FREE — no page fetches)
1. WebSearch: "self storage units near [location] prices"
2. WebSearch: "self storage [city] [state] unit prices rates"
3. READ THE SEARCH SNIPPETS CAREFULLY. Prices often appear directly in result
   text (e.g. "5x10 starting at $89/mo"). Extract every price you can find
   from snippets alone — no fetching yet.
4. Also note facility names, addresses, and URLs from results for later tiers.

### Tier 2 — One aggregator area page (1 fetch, many facilities)
1. WebSearch: "self storage units near [location] site:storageunits.com"
2. WebFetch the top StorageUnits.com area page. These list MULTIPLE facilities
   with prices on a SINGLE page. Extract every name, address, and price.
3. If StorageUnits.com didn't work, try: "self storage [city] [state] site:selfstorage.com"
   and fetch that area page instead.
4. This one fetch may fill in most of your remaining gaps.

**>>> WRITE THE EXCEL FILE NOW <<<**
After Tiers 1-2, write the Excel file with whatever data you have.
Leave cells blank for missing prices. This ensures a file always exists.

### Tier 3 — Individual WebFetch (1 fetch per facility with missing prices)
For each facility STILL missing prices:
1. WebSearch: "[facility name] [city] self storage unit prices"
2. Check snippets first — if prices are in the snippet, skip the fetch.
3. WebFetch ONE of these (in order of reliability):
   a. StorageUnits.com or SelfStorage.com individual listing
   b. StorageCafe.com listing
   c. Facility's own website (works for sites with static HTML)
4. After each facility lookup, REWRITE the Excel file with updated data.

### Tier 4 — Playwright scraper (LAST RESORT — slowest)
Only for facilities that STILL have no prices after Tiers 1-3.
Use the Playwright scraper via Bash to render JavaScript:

    python scrape_prices.py "<facility-website-url>"

This launches a headless browser and prints the visible page text.
Use it ONLY for:
- Facility websites where WebFetch returned empty/no pricing (JS-rendered)
- Major chains (Public Storage, Extra Space, CubeSmart, Life Storage)
Do NOT use it for:
- Aggregator sites — WebFetch already works for those
- Pages that returned CAPTCHA or "Access Denied" — scraper won't help either
After each Playwright lookup, REWRITE the Excel file with updated data.

### Key rules:
- **Never skip Tier 1.** Snippets are free and often have prices.
- **Don't fetch what you already have.** If Tier 1 snippets gave you prices
  for a facility, don't fetch that facility again in Tier 2/3/4.
- **Escalate per-facility, not globally.** If 6 of 8 facilities have prices
  after Tier 2, only run Tier 3 on the remaining 2.
- **Rate formats:** Prices appear as "$89", "$89.00", "$89/mo", "From $89".
  Extract the dollar amount.
- **Slashed/strikethrough prices = In-Store rate.** Many facility websites show
  a crossed-out original price next to a discounted "Web Rate" or "Online Rate".
  The slashed-out price is the IN-STORE rate. The discounted price is the ONLINE rate.
  Example: "~~$72~~ $36" means In-Store = $72, Online = $36. Capture BOTH.
- **Empty/blocked pages:** If any fetch returns CAPTCHA or "Access Denied,"
  do NOT retry that domain. Move to the next source or tier.

## Target Unit Types
Collect pricing for ALL of these sizes when available, for BOTH
Climate Controlled AND Drive-Up units:
5x5, 5x10, 10x10, 10x15, 10x20, 10x25, 10x30, Parking/Vehicle Storage

If a facility offers a 5x5 climate controlled unit, include it. If they offer
a 5x5 drive-up unit, include that too. Do NOT skip smaller sizes like 5x5.
Include every unit size the facility offers.

## Collect per facility:
Name | Address | Phone | Website | Online rates by unit size | In-store rates |
Climate controlled (Yes/No) | Promotions | Distance (mi) & drive time (min)
from subject using Haversine formula, 25 mph estimate.

Include only facilities within the search radius. Sort closest first.

## Excel — 3 Tabs

Tab 1 "Market Comps" — side-by-side comparison layout
  LEFT SIDE: "DRIVE-UP / STANDARD UNITS" header (bold, orange #FCE4D6)
  RIGHT SIDE: "CLIMATE CONTROLLED UNITS" header (bold, green #E2EFDA)
  Leave a 1-column gap between the two sides.

  Each side has this IDENTICAL structure:

  Section 1: "In-Store" sub-header
  Columns: Sq Ft | Size | [Facility 1 Name] | [Facility 2 Name] | ...
  Rows (one per unit size): 25/5x5, 50/5x10, 100/10x10, 150/10x15, 200/10x20, 250/10x25, 300/10x30
  Cell values are dollar amounts (e.g. $115.00). Leave cell BLANK if no data.

  Then a BLANK ROW separator.

  Section 2: "Online (Discounted)" sub-header
  Same columns: Sq Ft | Size | [Facility 1 Name] | [Facility 2 Name] | ...
  Same rows as above.

  Facility columns should be in order: closest facility first (left) to farthest (right).
  Format: bold headers, currency format, auto-width columns.
  If a facility has no units of a given type (drive-up or climate controlled),
  omit that facility's column from that side entirely.

Tab 2 "Facility List" — one row per facility
Columns: Facility Name | Address | Distance (mi) | Drive Time (min) | Phone | Website
Format: bold header (#FCE4D6), sorted by distance

## Rules
- Never fabricate pricing. Leave the cell BLANK if no data — do NOT write "N/A".
- Distance required — exclude facility if unknown.
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
5. Write the Excel file using openpyxl (2-tab format per system prompt).
6. Print a brief summary: facilities found, price ranges by unit size.

No fabricated data — leave cells blank if no price found.
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
            max_turns=50,
            model="claude-haiku-4-5-20251001",
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
