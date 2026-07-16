# Storage Tools — Self Storage Analysis Suite

## Project Overview
A self-storage analysis platform: a **Flask web dashboard** (`app.py`) for deal analytics, plus an automated **Crexi deal-watcher pipeline** (`crexi_watcher.py`, with a tkinter launcher in `crexi_watcher_app.py`).

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
- **Structured output.** Excel reports match the proforma templates exactly — loaded directly from `claude excel model template.xlsx` (single/multi-story) or `mixed_proforma_template.xlsx` (mixed 2–4 acres).
- **Error handling with grace.** Users see clear, actionable error messages — never raw stack traces.

---

## Architecture

### Web Dashboard (`app.py`)
- **Framework:** Flask serving a single-page UI from `templates/index.html`
- **Database:** SQLite (`data/deals.db`) via `db_utils.py` — schema, migrations, and deal scoring live there
- **Features:** deal analytics, market/city breakdowns with drill-down, launching comps runs with streamed progress
- **Launch:** `python app.py` → http://127.0.0.1:5000

### Crexi Watcher (`crexi_watcher.py`)
- **CLI pipeline:** scrape Crexi search results (Firecrawl) → census population gate → comps pipeline → Excel report + SQLite record
- **Launcher:** `crexi_watcher_app.py` (tkinter, dark theme) / `Launch Crexi Watcher.bat`
- **Defaults:** 1 search page per run (`--max-pages 0` or `MAX_SEARCH_PAGES=0` for all pages), 3 deals per run
- **API keys:** GOOGLE_PLACES_API_KEY, FIRECRAWL_API_KEY, ANTHROPIC_API_KEY, optional CENSUS_API_KEY (from `.env`)

### Maintenance Scripts (`scripts/`)
One-off backfill/migration tools (population, land cost, market averages, report regeneration). Each has a `sys.path` shim so they run from the repo root or `scripts/` directly.

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

### Facility Type Classification
Parcel acreage determines the facility type, which drives yield, construction cost, and rent assumptions:
- **single_story (> 4 acres):** 40% yield, $50/sqft, drive-up comps
- **multi_story (< 2 acres):** 122% yield, $95/sqft, CC comps
- **mixed (2–4 acres):** Dynamic land split targeting 90,000 rentable sqft, separate CC + DU mini-proformas

Mixed facilities use a dedicated template (`mixed_proforma_template.xlsx`) with two mini-proformas that feed into a main summary. Single/multi-story use `claude excel model template.xlsx`.

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
- **Single/Multi-story:** Loaded from `claude excel model template.xlsx` — assumptions in D/E, outputs in G/H
- **Mixed (2–4 acres):** Loaded from `mixed_proforma_template.xlsx` — two mini-proformas (CC rows 13-20, DU rows 22-29) in columns B-G, main summary in columns I-J
- Code auto-fills: address, acres, land cost, rent/sqft, yield, construction cost based on facility type
- See `PROFORMA_LOGIC.md` for full cell map and assumption details

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
├── app.py                              # Flask analytics dashboard
├── templates/index.html                # Dashboard single-page UI
├── comps_pipeline.py                   # Core pipeline — geocode → scrape → extract → Excel
├── crexi_watcher.py                    # Crexi deal-watcher CLI
├── crexi_watcher_app.py                # tkinter launcher for the watcher
├── Launch Crexi Watcher.bat            # Watcher launcher
├── db_utils.py                         # SQLite schema, migrations, deal scoring
├── crexi/                              # Crexi scraper, census population gate, dedup
├── rank_reports.py                     # Ranked Excel of all deals (canonical deal_score)
├── scripts/                            # One-off backfill/migration/maintenance tools
├── storage_comps_agent.py              # CLI agent script
├── firecrawl_scrape.py                 # Standalone Firecrawl scraper (reference)
├── test_pipeline.py                    # Single-facility debug script
├── tests/                              # pytest suite (pure-logic + import smoke)
├── claude excel model template.xlsx    # Proforma template for single/multi-story
├── mixed_proforma_template.xlsx        # Proforma template for mixed facilities (2-4 acres)
├── requirements.txt                    # Dependencies
├── data/                               # place_centroids.csv, census cache, deals.db
├── output/                             # Generated comps Excel files
└── reports/                            # Per-market deal reports + rankings
```

---

## Tech Stack
- **Language:** Python 3.11
- **Desktop UI:** tkinter + ttk
- **Competitor Discovery:** Google Places API (Nearby Search + Place Details + Geocoding)
- **Website Scraping:** Firecrawl (`firecrawl-py`) — JS rendering + Cloudflare bypass
- **Pricing Extraction:** Claude Haiku (`claude-haiku-4-5-20251001`) via direct Anthropic API
- **Excel:** openpyxl — reads template, writes dynamic tabs
- **Web Framework:** Flask (analytics dashboard `app.py`)

## Core Dependencies
```
firecrawl-py, anthropic, openpyxl, requests, python-dotenv, flask, playwright
```