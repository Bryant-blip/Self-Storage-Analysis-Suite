# Storage Tools — Self Storage Analysis Suite

## Project Overview
A two-tool self-storage analysis platform with both a **desktop app** (tkinter) and a **web app** (FastAPI). The suite helps self-storage developers and investors analyze markets and estimate construction costs.

### Tools
1. **Market Comps** — AI-powered rent comp analysis. Given a location and radius, an AI agent searches the web for nearby self-storage facilities, collects pricing data, and outputs a formatted Excel spreadsheet.
2. **Cost Estimator** — Construction cost estimation for new self-storage facilities. Supports both Quick Estimate (instant, no API call) and Accurate Estimate (AI agent researches local costs). Covers Drive-Up and Climate Controlled building types with itemized hard costs, soft costs (~22.5%), and USACE location adjustment factors.

---

## Standards & Principles

### Security — Non-Negotiable
This platform handles user credentials and third-party API keys. Every decision must prioritize data protection.

- **Zero plaintext secrets.** API keys are encrypted at rest using Fernet (AES-128) with PBKDF2HMAC key derivation (100k iterations). Passwords are hashed with bcrypt. No exceptions.
- **No hardcoded credentials.** All secrets live in `.env` files that are `.gitignore`-protected. The app refuses to start without a properly configured `JWT_SECRET`.
- **BYOK model.** The server never stores or exposes its own API key. Each user provides and owns their own Anthropic key. This limits blast radius — a compromised account exposes only that user's key, not a shared resource.
- **Auth on every endpoint.** All data-access and agent endpoints require a valid JWT. File downloads authenticate via query-param token since browser downloads can't send headers.
- **Admin isolation.** Admin endpoints are gated by email whitelist (`ADMIN_EMAIL`). Admin can view users and disable accounts but cannot access user API keys.
- **Rate limiting.** Per-user daily caps prevent abuse and runaway API spend (default 50/day, configurable).
- **Never commit `.env`, `*.db`, or generated files.** The `.gitignore` enforces this at the repo level.

### Functionality — Accuracy Over Speed
The tools exist to support real investment decisions. Output must be reliable enough to present in a deal memo.

- **No fabricated data.** Market comps only include pricing sourced from the web. If a rate can't be found, mark it "N/A" — never guess.
- **Distance is mandatory.** Every facility must have a verified distance from the subject property. No facility is included without it.
- **Itemized cost breakdowns.** The cost estimator provides line-by-line hard costs and 9 itemized soft cost categories (~22.5%) — not a single opaque percentage. Every dollar is traceable.
- **Location-adjusted.** Construction costs are adjusted using USACE Area Cost Factors specific to each metro, not national averages.
- **Real-time feedback.** Agent operations stream output via SSE so users see progress, not a blank screen and a spinner.

### Professionalism — Institutional Quality
This is built to the standard of a tool used by a New York private equity firm's acquisitions team.

- **Clean, consistent UI.** Dark theme with precise spacing, no clutter. Every element earns its place on screen.
- **Structured output.** Excel reports include formatted headers, auto-sized columns, conditional formatting, and multiple tabs (Comps Detail, Market Summary, Facility List). Ready for deal books and IC presentations.
- **Error handling with grace.** Users see clear, actionable error messages — never raw stack traces or cryptic codes.
- **Audit trail.** Every search is logged with user, action type, location, and timestamp. Usage is queryable by admins.
- **Production-ready architecture.** SQLAlchemy ORM with SQLite for development and PostgreSQL for production. JWT auth, rate limiting, admin controls, and encrypted storage — not bolted on later, built in from the start.
- **No personal data leakage.** No emails, API keys, usernames, or local file paths in committed code. All user-specific values live in `.env` or the database.

---

## Architecture

### Desktop App (`storage_comps_app.py`)
- **Framework:** tkinter with dark theme (#0d1117 background)
- **Tabs:** Market Comps, Cost Estimator
- **AI Backend:** Claude Agent SDK (`query()`, `ClaudeAgentOptions`)
- **Output:** Excel files via `openpyxl` saved to `output/`
- **Launch:** `Launch Storage Comps App.bat` or `python storage_comps_app.py`

### Web App (`web/`)
- **Framework:** FastAPI + Jinja2 templates
- **Auth:** JWT tokens (python-jose) + bcrypt password hashing (passlib)
- **Database:** SQLAlchemy — SQLite locally, PostgreSQL in production
- **API Key Model:** BYOK (Bring Your Own Key) — each user provides their own Anthropic API key at signup
- **API Key Security:** Fernet encryption (AES-128) with PBKDF2HMAC key derivation from JWT_SECRET
- **Streaming:** Server-Sent Events (SSE) for real-time agent output
- **Rate Limiting:** Configurable daily search limit per user (default 50/day)
- **Admin:** ADMIN_EMAIL env var controls admin access; admin can view all users and toggle accounts
- **Launch:** `python -m uvicorn app:app --host 127.0.0.1 --port 5000`

### CLI Agent (`storage_comps_agent.py`)
- Standalone CLI script for running market comps via terminal

### Fact-Check Agent (`fact_check_agent.py`)
- Aggressive, skeptical price auditor for comps Excel files
- Reads all data from every tab, verifies each facility's prices against live websites/aggregators, and recalculates all Market Summary math
- Outputs a 4-tab Excel audit report: Price Verification (row-by-row status), Math Verification (recalculated Avg/Min/Max/Count), Analysis Notes (harsh written critique with sections: Price Accuracy Assessment, Suspicious Pricing, Math Errors, Data Gaps, Bottom Line), Summary (accuracy stats and X/10 score)
- Exact dollar match required for "Verified" ($129.00 = $129); any difference is "Mismatch" with +$X/-$X shown
- Searches once per facility, matches all unit types from that lookup; uses Sonnet with max_turns=50
- Integrated into both desktop app (orange "Fact-Check" button after comps complete) and web app (`POST /api/fact-check` SSE stream)

## Cost Data

### Hard Costs (per SF)
- **Drive-Up:** Site Work, Concrete Slab, Steel Structure, Metal Roofing, Electrical, Paving + lump sums (Roll-Up Doors, Security System, Office Buildout)
- **Climate Controlled:** All Drive-Up items + Insulation/Vapor Barrier, HVAC, Interior Partitions/Hallways, Elevator, Fire Suppression, Enhanced Electrical

### Soft Costs (~22.5% of hard costs, itemized)
Architectural & Engineering (5%), Permits & Impact Fees (2.5%), Geotechnical/Environmental (0.8%), Survey & Land Planning (0.4%), Legal & Closing (0.8%), Builder's Risk Insurance (0.7%), Construction Loan Interest (4%), Property Taxes During Construction (0.8%), Contingency (7.5%)

### Location Adjustment
USACE Area Cost Factors applied to base costs per city (e.g., NYC 1.35, Austin 0.92, etc.)

## Market Comps Output

### Excel Tabs
1. **Comps Detail** — full row-by-row data for every facility + unit type
2. **Market Summary** — average, min, max online/in-store rate per unit size (split by Drive-Up and Climate Controlled)
3. **Facility List** — one row per facility with address, distance, drive time, phone, website

### Target Unit Types
5x5, 5x10, 10x10, 10x15, 10x20, 10x25, 10x30, Parking/Vehicle Storage

### Key Rules
- Always collect both online and in-store rates; mark "N/A" if not found
- Distance from subject property is required for every facility
- Sort all output by distance (closest first)
- Do not fabricate pricing — only use data from web sources
- Capture promotions in Notes column
- Default radius: 5 miles (configurable)
- Filename: `storage_comps_[location]_[YYYYMMDD].xlsx`

## Web App Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/register` | Create account (email, password, API key) |
| POST | `/api/login` | Get JWT token |
| POST | `/api/update-api-key` | Update stored API key |
| GET | `/api/usage` | User's usage stats |
| POST | `/api/quick-estimate` | Instant cost estimate (no API) |
| POST | `/api/comps` | Market comps agent (SSE stream) |
| POST | `/api/fact-check` | Fact-check audit agent (SSE stream) |
| POST | `/api/accurate-estimate` | Accurate cost agent (SSE stream) |
| GET | `/api/download/{file}` | Download Excel file (token auth via query param) |
| GET | `/api/admin/users` | Admin: list all users and usage |
| POST | `/api/admin/toggle-user/{id}` | Admin: enable/disable user |

## Web App Config (`.env`)
```
JWT_SECRET=<random-string-for-signing-tokens>
ADMIN_EMAIL=<admin-user-email>
DAILY_SEARCH_LIMIT=50          # optional, default 50
DATABASE_URL=sqlite:///./storage_tools.db  # or postgresql:// for production
```
Note: No server-side ANTHROPIC_API_KEY needed — users bring their own.

## File Structure
```
Real Estate Project/
├── .gitignore                      # Blocks .env, *.db, __pycache__, generated files
├── CLAUDE.md                       # This file
├── storage_comps_app.py            # Desktop app (tkinter, ~1300 lines)
├── storage_comps_agent.py          # CLI agent script
├── fact_check_agent.py             # Fact-check & audit CLI agent
├── build_comps.py                  # Build helper
├── requirements.txt                # Desktop dependencies
├── Launch Storage Comps App.bat    # Desktop shortcut
├── Install Desktop Shortcut.bat    # Creates desktop shortcut
├── output/                         # Generated Excel files
│   └── gen_comps.py
└── web/                            # Web application
    ├── app.py                      # FastAPI backend (~600 lines)
    ├── database.py                 # SQLAlchemy models (User, UsageLog)
    ├── requirements.txt            # Web dependencies
    ├── start.bat                   # Windows launcher (loads .env)
    ├── .env                        # Environment config (NEVER committed)
    ├── .env.example                # Template for .env
    ├── static/                     # Static assets
    └── templates/
        ├── login.html              # Login/signup page
        └── index.html              # Main dashboard
```

## Tech Stack
- **Language:** Python 3.11+
- **Desktop UI:** tkinter + ttk
- **Web Framework:** FastAPI + uvicorn
- **AI:** Claude Agent SDK (`claude-agent-sdk`)
- **Database:** SQLAlchemy (SQLite / PostgreSQL)
- **Auth:** JWT (python-jose), bcrypt (passlib), Fernet encryption (cryptography)
- **Excel:** openpyxl
- **Geocoding:** geopy
- **Templating:** Jinja2

## Dependencies

### Desktop
```
pip install openpyxl geopy claude-agent-sdk anyio
```

### Web
```
pip install fastapi uvicorn[standard] jinja2 python-multipart python-jose[cryptography] passlib[bcrypt] sqlalchemy openpyxl claude-agent-sdk anyio geopy python-dotenv
```
Note: Use `bcrypt==4.0.1` to avoid compatibility issues with passlib.
