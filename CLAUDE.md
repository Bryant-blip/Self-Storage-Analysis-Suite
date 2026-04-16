# Storage Tools — Self Storage Analysis Suite

## Project Overview
A self-storage analysis platform with both a **desktop app** (tkinter) and a **web app** (FastAPI).

### Tools
1. **Market Comps** — Automated rent comp analysis. Given a location and radius, the pipeline discovers nearby self-storage facilities via Google Places, scrapes each facility's own website using Firecrawl, extracts structured pricing with Claude Haiku, and outputs a formatted 3-tab Excel report.

---

## Standards & Principles

### Security — Non-Negotiable
- **Zero plaintext secrets.** API keys in `.env` files only. No exceptions.
- **No hardcoded credentials.** All secrets live in `.env` files that are `.gitignore`-protected.
- **Never commit `.env`, `*.db`, or generated files.**

### Functionality — Accuracy Over Speed
- **No fabricated data.** Market comps only include pricing sourced directly from each facility's website. If a rate can't be found, leave the cell blank — never guess.
- **Distance is mandatory.** Every facility must have a verified Haversine distance from the subject property. Hard radius enforcement — Google Places radius is a hint, not a guarantee.
### Professionalism — Institutional Quality
- **Structured output.** Excel reports match the proforma template exactly — loaded directly from `claude excel model template.xlsx`.
- **Error handling with grace.** Users see clear, actionable error messages — never raw stack traces.

---

## Architecture

### Desktop App (`storage_comps_app.py`)
- **Framework:** tkinter with dark theme (#0d1117 background)
- **Tabs:** Market Comps
- **Backend:** `comps_pipeline.py` — direct API pipeline (no Agent SDK)
- **Output:** Excel files via `openpyxl` saved to `output/`
- **Launch:** `Launch Storage Comps App.bat` (uses `pythonw3.11.exe`)
- **API keys:** GOOGLE_PLACES_API_KEY, FIRECRAWL_API_KEY, ANTHROPIC_API_KEY (from `.env`)
- **Auto-install:** `_ensure_deps()` bootstrap installs missing packages on launch

### Web App (`web/`)
- **Framework:** FastAPI + Jinja2 templates
- **Auth:** JWT tokens + bcrypt password hashing
- **Database:** SQLAlchemy — SQLite locally, PostgreSQL in production
- **API Key Model:** BYOK — each user provides their own Anthropic API key
- **Streaming:** Server-Sent Events (SSE) for real-time agent output
- **Launch:** `python -m uvicorn app:app --host 127.0.0.1 --port 5000`

---

## Market Comps Pipeline (`comps_pipeline.py`)

### Workflow
1. **Geocode** subject location (Google Geocoding API)
2. **Discover** nearby facilities (Google Places Nearby Search → Text Search fallback)
3. **Filter** — hard radius enforcement, exclude PODS/moving companies
4. **Per facility** (ThreadPoolExecutor, max 5 workers):
   - Get address, phone, website (Google Place Details)
   - Scrape website directly with Firecrawl (handles JS + Cloudflare)
   - Extract structured pricing with Claude Haiku (`claude-haiku-4-5-20251001`)
5. **Write** 3-tab Excel report

### Key Design Decisions
- **Firecrawl over Tavily:** Switched because Tavily search returned area/market pages instead of per-facility pages. Firecrawl goes directly to each facility's own website URL (from Google Places) and handles Cloudflare on major chains.
- **No website = no pricing:** If Google Places has no website URL for a facility, pricing is skipped. No fallback search.
- **50k char limit to Claude:** Large chain pages (Extra Space ~57-59k chars) need this to capture pricing tables.

### Pricing Extraction Rules
- **web_rate:** online/discounted price — "Web Rate", "Online Rate", "Online-Only Price", "eRate"
- **in_store_rate:** regular/walk-in price — "Standard Price", "Street Rate", "Regular Price", crossed-out price
- **Deduplication:** for each (size, type) keep lowest web_rate
- **Type classification:** "climate_control" only if explicitly temperature-controlled language; "drive_up" for everything else including indoor/interior without temp language

### Unit Sizes
`5x5, 5x10, 10x10, 10x15, 10x20, 10x25, 10x30`
- Empty size rows are skipped in Excel (no blank rows if no facility has that size)

---

## Excel Output — 3 Tabs

### Tab 1: Proforma
- Loaded directly from `claude excel model template.xlsx` — all formatting comes from the file
- Code fills: B3 (location), clears C5/C6/E6 to blank (user fills in Acres, Land Cost, Rent/sqft)
- All formulas in G/H columns preserved and live
- Sheet named "Initial look proforma" in template

### Tab 2: Market Comps
- LEFT: "Drive-Up / Standard Units" (orange #FCE4D6)
- RIGHT: "Climate Controlled Units" (green #E2EFDA)
- Each side: In-Store rates on top, Online (Discounted) rates below
- Facilities as columns (closest first), unit sizes as rows
- Averages summary section below grid: $/sqft per size for all 4 combinations
- Blank cells for missing data (never "N/A")

### Tab 3: Facility List
- Name, address, distance (mi), drive time (min), phone, website (hyperlinked)

---

## API Keys Required (Desktop)

Set in `.env` file in the project root:
```
GOOGLE_PLACES_API_KEY=   # Places API + Geocoding API enabled in Google Cloud Console
FIRECRAWL_API_KEY=       # firecrawl.dev — free tier 500 pages/month
ANTHROPIC_API_KEY=       # console.anthropic.com
```

---

## File Structure
```
Real Estate Project/
├── CLAUDE.md                           # This file
├── .env                                # API keys (never commit)
├── .env.example                        # Template for .env
├── comps_pipeline.py                   # Core pipeline — geocode → scrape → extract → Excel
├── storage_comps_app.py                # Desktop app (tkinter)
├── storage_comps_agent.py              # CLI agent script
├── firecrawl_scrape.py                 # Standalone Firecrawl scraper (reference)
├── test_pipeline.py                    # Single-facility debug script
├── claude excel model template.xlsx    # Proforma template — drives Tab 1 formatting
├── requirements.txt                    # Desktop dependencies
├── Launch Storage Comps App.bat        # Desktop launcher (pythonw3.11.exe)
└── output/                             # Generated Excel files
```

---

## Tech Stack
- **Language:** Python 3.11
- **Desktop UI:** tkinter + ttk
- **Competitor Discovery:** Google Places API (Nearby Search + Place Details + Geocoding)
- **Website Scraping:** Firecrawl (`firecrawl-py`) — JS rendering + Cloudflare bypass
- **Pricing Extraction:** Claude Haiku (`claude-haiku-4-5-20251001`) via direct Anthropic API
- **Excel:** openpyxl — reads template, writes dynamic tabs
- **Web Framework:** FastAPI + uvicorn (web app only)

## Desktop Dependencies
```
firecrawl-py, anthropic, openpyxl, requests, geopy, python-dotenv
```