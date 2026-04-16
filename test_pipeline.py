"""
Debug script — run this to see exactly what Firecrawl returns and what Claude extracts.
Usage: python3.11.exe test_pipeline.py

Set FACILITY_WEBSITE to the URL shown on the facility's Google Maps listing.
"""
import os, json

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except ImportError:
    pass

from firecrawl import FirecrawlApp
import anthropic

FACILITY         = "CubeSmart Self Storage"
FACILITY_WEBSITE = "https://www.cubesmart.com/arizona-self-storage/tolleson-self-storage/1249/"

FIRECRAWL_KEY = os.environ.get("FIRECRAWL_API_KEY", "")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

print(f"FIRECRAWL_API_KEY: {'SET ✓' if FIRECRAWL_KEY else '*** MISSING ***'}")
print(f"ANTHROPIC_API_KEY: {'SET ✓' if ANTHROPIC_KEY else '*** MISSING ***'}")
print()

# ── Step 1: Firecrawl scrape ───────────────────────────────────────────────────
print("=" * 60)
print("FIRECRAWL SCRAPE RESULT")
print("=" * 60)
print(f"URL: {FACILITY_WEBSITE}")
print()

app = FirecrawlApp(api_key=FIRECRAWL_KEY)
result = app.scrape(FACILITY_WEBSITE, formats=["markdown"])

if result and hasattr(result, "markdown") and result.markdown:
    raw_text = result.markdown
elif result and hasattr(result, "content") and result.content:
    raw_text = result.content
else:
    raw_text = ""

print(f"Content length: {len(raw_text)} chars")
print()
print("--- First 1000 chars ---")
print(raw_text[:1000])
print("...")
print()

# ── Step 2: Claude extraction ──────────────────────────────────────────────────
print("=" * 60)
print("CLAUDE RESPONSE")
print("=" * 60)

if not raw_text.strip():
    print("No content returned by Firecrawl — cannot extract pricing.")
    input("\nPress Enter to exit...")
    exit()

EXTRACTION_SYSTEM = """You are a data extraction assistant for self-storage pricing.
Extract any unit pricing you find. Return ONLY a valid JSON array — no explanation, no markdown.

Each element must have these fields:
  {"size": "10x10", "type": "drive_up", "in_store_rate": 129.00, "web_rate": 99.00}

Size rules:
- Normalize all size formats to NxN: "10 x 10", "10X10", "10'x10'" all become "10x10"
- Only include these sizes: 5x5, 5x10, 10x10, 10x15, 10x20

Type rules:
- The "type" field must be EXACTLY one of: "drive_up" or "climate_control"
- "climate_control": explicitly climate controlled, air conditioned, temperature controlled
- "drive_up": everything else

Price rules:
- web_rate: online/discounted price — "Web Rate", "Online Rate", "Online-Only Price", "eRate"
- in_store_rate: regular/walk-in price — "Standard Price", "Street Rate", "Regular Price", or crossed-out price
- in_store_rate is ALWAYS higher than web_rate
- If only one price, put it in web_rate

Return [] if no prices found."""

aclient = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
resp = aclient.messages.create(
    model="claude-haiku-4-5-20251001",
    max_tokens=2048,
    system=EXTRACTION_SYSTEM,
    messages=[{
        "role":    "user",
        "content": f"Facility: {FACILITY}\n\nWebsite content:\n{raw_text[:50000]}",
    }],
)

raw_response = resp.content[0].text
print(f"Raw response:\n{raw_response}\n")

try:
    parsed = json.loads(raw_response.strip())
    print(f"Parsed successfully: {len(parsed)} pricing entries found")
    for p in parsed:
        print(f"  {p}")
except json.JSONDecodeError as e:
    print(f"JSON parse error: {e}")

input("\nPress Enter to exit...")
