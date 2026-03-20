#!/usr/bin/env python3
"""
Self Storage Market Comps — Fact-Check & Critique Agent

Reads an Excel comps file produced by the Market Comps agent, then
independently verifies the data and produces a scored audit report.

Usage:
    python fact_check_agent.py "output/storage_comps_Austin_TX_Mar-19-26.xlsx"
    python fact_check_agent.py   (interactive prompt)
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
FACT_CHECK_PROMPT = """
You are an aggressive, skeptical price auditor. Your job is to tear apart a
self-storage market comps spreadsheet and find every error, inconsistency, and
suspicious data point. Assume nothing is correct until you prove it. Be blunt,
be specific, and do not sugarcoat findings. This report goes to investors who
need to know exactly how trustworthy this data is before making million-dollar
decisions.

## Step 1 — Read the Excel file
Use the Bash tool to run a Python script that reads the workbook with openpyxl
and prints ALL data from EVERY tab (Comps Detail, Market Summary, Facility List).
Print every row so you have the complete dataset.

## Step 2 — Verify prices
Work through facilities ONE AT A TIME. For each unique facility name:

### Search strategy (follow this order):
1. Do ONE WebSearch: "[facility name] [city] self storage unit prices"
2. **CHECK SEARCH SNIPPETS FIRST.** Search results often show prices directly
   in the snippet text (e.g., "5x10 starting at $89/mo"). Extract any prices
   you can find from the snippets before fetching any pages.
3. **Fetch priority order** — try these in order until you get pricing data:
   a. StorageUnits.com or SelfStorage.com result (these return static HTML
      that is reliably parseable — they are your BEST sources)
   b. The facility's own website (but note: many use JavaScript-heavy pages
      that may return empty content — if the fetch returns no pricing data,
      move on immediately)
   c. iStorage, StorageCafe.com, or other aggregator results
   d. SpareFoot (often blocked — try last)
4. If the first fetch returns no pricing, try up to 2 MORE pages from the
   search results before giving up.
5. If all fetches fail, try ONE alternative search:
   "site:storageunits.com [facility name] [city]" or
   "site:selfstorage.com [facility name] [city]"
   and fetch the top result.
6. From the results, extract rates for ALL unit types at that facility
   and compare against every row in the spreadsheet for that facility.

### Common issues to handle:
- **Empty/blocked pages:** If a WebFetch returns very little text, a CAPTCHA
  page, or "Access Denied," do NOT retry that domain. Move to the next source.
- **JavaScript sites:** Major chains (Public Storage, Extra Space, CubeSmart,
  Life Storage) render prices via JavaScript. Their direct websites will
  likely return empty pricing. Always prefer aggregator sites for these chains.
- **Rate format variations:** Prices may appear as "$89", "$89.00", "$89/mo",
  "From $89". Extract the dollar amount and compare.

IMPORTANT: Do NOT search separately for each unit type — search ONCE per
facility and match all unit types from the results. This saves turns.

Prices are compared as whole dollar amounts. "$129.00" and "$129" are the
same price. Strip cents for comparison — round to nearest dollar.

## Step 3 — Verify Market Summary math
Recalculate EVERY number in the Market Summary tab from the raw Comps Detail data:
- Avg, Min, Max for each unit type × climate type (Drive-Up vs Climate Controlled)
- Verify that "# Comps" counts match actual row counts in Comps Detail
- Flag ANY discrepancy, even $0.01 off. Show your calculated value vs the report value.
- Check that the correct rows are being included/excluded (e.g., N/A rates should
  not be counted in averages).

## Step 4 — Write the verification report
Use the Bash tool to write an Excel report with openpyxl. Save it to the
specified output path.

### Tab 1 "Price Verification"
Row 1: "PRICE VERIFICATION REPORT" (bold, size 14)
Row 2: "File reviewed: [filename]"
Row 3: "Audit date: [today]"
Row 4: blank
Row 5: Header row (bold, #BDD7EE fill):
  Facility Name | Unit Type | Climate | Report Online Rate | Report In-Store Rate |
  Verified Rate | Difference | Source | Status
Row 6+: One row per facility × unit type combination you were able to check.
- Status values: "Verified" (exact dollar match), "Mismatch" (any difference),
  "Not Found" (couldn't find pricing for that unit type)
- Difference column: "$0" for Verified, "+$X" or "-$X" for Mismatch, "N/A" for Not Found
- Green (#E2EFDA) fill for Verified rows, Red (#FCE4D6) fill for Mismatch rows
- Auto-width all columns

### Tab 2 "Math Verification"
Columns: Unit Type | Climate | Metric | Report Value | Calculated Value | Difference | Status
- One row per Avg/Min/Max/Count per unit type × climate type
- Status: "Correct" or "ERROR — off by $X"
- Green fill for correct, Red fill for errors
- Bold header row, auto-width

### Tab 3 "Analysis Notes"
This is your written critique. Be harsh. Be specific. Hold nothing back.
Row 1: "ANALYSIS NOTES" (bold, size 14)
Row 2: "Auditor: AI Price Verification Agent"
Row 3: "Date: [today]"
Row 4: blank

Then write the following sections, each with a bold header row:

**"PRICE ACCURACY ASSESSMENT"**
- For each facility, write 1-2 sentences on whether its prices checked out.
  If a price was wrong, state exactly what the report says vs what you found
  and where you found it. Call out patterns — is the report consistently
  high, low, or using stale data?

**"SUSPICIOUS PRICING"**
- Flag any rate that looks fabricated, rounded suspiciously, or is a clear
  outlier vs the market. Be specific: "Facility X lists 10x10 at $250/mo
  while every other facility in the radius is $120-$160. This is either
  premium pricing that needs explanation or a data error."
- Flag any facility where online and in-store rates are identical — this
  is unusual and may indicate the agent only found one rate and copied it.
- Flag any rate below $20/mo or above $600/mo as likely erroneous.

**"MATH ERRORS"**
- List every Market Summary calculation that doesn't match your recalculation.
  Show the math: "Report says 10x10 Drive-Up Avg = $142.50 but actual average
  of [$130, $145, $155, $140] = $142.50 — Correct" or "Report says Avg = $150
  but I calculate $142.50 from the data — ERROR, overstated by $7.50."

**"DATA GAPS & CONCERNS"**
- How many unit types have zero verified prices? What % of rates are "N/A"?
- Are there facilities with no pricing at all? Why were they included?
- Any unit types completely missing from the market that should be there?

**"BOTTOM LINE"**
- 2-3 sentences maximum. Is this data reliable enough to base an investment
  decision on? Give a straight answer. If the prices are mostly wrong, say so
  plainly. If they're solid, acknowledge it — but still note any caveats.

Format: Use openpyxl to write each section. Bold headers in size 12. Regular
text in size 10. Wrap text in the notes column. Set column A width to 120
characters. Leave a blank row between sections.

### Tab 4 "Summary"
Row 1: "VERIFICATION SUMMARY" (bold, size 14)
Row 2: blank
Row 3+: Stats table:
  - Total facilities in report
  - Total facilities checked
  - Total prices verified
  - Prices confirmed accurate (count and %)
  - Prices with mismatches (count and %)
  - Prices unable to verify (count and %)
  - Market Summary calculations checked
  - Market Summary errors found
  - Overall accuracy score: X/10

Scoring:
- 9-10: All prices verified, math is correct, no concerns
- 7-8: Most prices check out, minor math rounding issues
- 5-6: Multiple price mismatches or math errors that affect reliability
- 3-4: Significant inaccuracies — do not rely on this data without re-running
- 1-2: Widespread fabrication or errors — data is unreliable

## Rules
- Never fabricate verification data — if you can't find a price, mark "Not Found."
- Format all currency values with $ and commas.
- Auto-width all columns in the Excel output.
- A price is "Verified" ONLY if it is an exact dollar match to the report rate
  (rounded to whole dollars — $129.00 = $129). Any difference, even $1, is a
  "Mismatch." Always show the difference amount in the Difference column.
- Be ruthlessly honest in Analysis Notes. Do not hedge or use soft language.
  Say "wrong," "fabricated," "error" — not "slightly different" or "minor gap."
"""


# ── Agent runner ───────────────────────────────────────────────────────────────
async def run_agent(excel_path: str) -> None:
    today = date.today().strftime("%b-%d-%y")
    basename = os.path.splitext(os.path.basename(excel_path))[0]
    output_file = os.path.join(OUTPUT_DIR, f"audit_{basename}_{today}.xlsx")

    abs_excel = os.path.abspath(excel_path)
    if not os.path.exists(abs_excel):
        print(f"Error: File not found: {abs_excel}")
        sys.exit(1)

    prompt = f"""
Audit the self-storage market comps spreadsheet:
  File     : {abs_excel}
  Date     : {date.today().strftime("%B %d, %Y")}
  Save to  : {output_file}

Follow the steps in the system prompt:
1. Read all data from the Excel file.
2. Verify prices for every facility (search once per facility, match all unit types).
3. Recalculate and verify all Market Summary math.
4. Write the verification report Excel file.
"""

    print()
    print("=" * 65)
    print("  MARKET COMPS FACT-CHECK & AUDIT")
    print("=" * 65)
    print(f"  File     : {abs_excel}")
    print(f"  Output   : {output_file}")
    print("=" * 65)
    print()

    async for message in query(
        prompt=prompt,
        options=ClaudeAgentOptions(
            system_prompt=FACT_CHECK_PROMPT,
            allowed_tools=["WebSearch", "WebFetch", "Bash", "Read"],
            permission_mode="acceptEdits",
            cwd=BASE_DIR,
            max_turns=50,
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
            print("  AUDIT COMPLETE")
            print("=" * 65)
            if message.result:
                print(message.result)
            print(f"\n  Report saved to: {output_file}")
            print()


# ── CLI ────────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fact-check and audit a market comps spreadsheet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python fact_check_agent.py "output/storage_comps_Austin_TX_Mar-19-26.xlsx"
  python fact_check_agent.py
        """,
    )
    parser.add_argument(
        "file",
        nargs="?",
        help="Path to the comps Excel file to audit",
    )

    args = parser.parse_args()

    if not args.file:
        print()
        print("Market Comps Fact-Check & Audit Agent")
        print("-" * 40)
        # List available comps files
        xlsx_files = [
            f for f in os.listdir(OUTPUT_DIR)
            if f.startswith("storage_comps_") and f.endswith(".xlsx")
        ]
        if xlsx_files:
            xlsx_files.sort(key=lambda f: os.path.getmtime(os.path.join(OUTPUT_DIR, f)), reverse=True)
            print("Available comps files:")
            for i, f in enumerate(xlsx_files[:10], 1):
                print(f"  {i}. {f}")
            choice = input("\nEnter file number or full path: ").strip()
            try:
                idx = int(choice) - 1
                if 0 <= idx < len(xlsx_files):
                    args.file = os.path.join(OUTPUT_DIR, xlsx_files[idx])
                else:
                    print("Invalid selection.")
                    sys.exit(1)
            except ValueError:
                args.file = choice
        else:
            args.file = input("Path to comps Excel file: ").strip()

        if not args.file:
            print("Error: File path is required.")
            sys.exit(1)

    anyio.run(run_agent, args.file)


if __name__ == "__main__":
    main()
