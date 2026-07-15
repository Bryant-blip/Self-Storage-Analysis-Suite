You are helping analyze a self-storage development deal. Follow these steps:

1. **Collect inputs** — Ask the user for: property address, lot size in acres, and land cost ($/acre or total if known).

2. **Classify facility type** based on acreage:
   - >4 acres → single-story (40% yield, $50/sqft build cost, drive-up comps)
   - <2 acres → multi-story (122% yield, $95/sqft build cost, CC comps)
   - 2–4 acres → mixed (dynamic land split targeting 90,000 sqft, uses mixed_proforma_template.xlsx)

3. **Run the comps pipeline**: Execute `python comps_pipeline.py` with the subject address. The pipeline geocodes the address, finds nearby facilities via Google Places, scrapes pricing with Firecrawl, extracts rates with Claude Haiku, and writes a 3-tab Excel report to `output/`.

4. **Review the Excel output** — confirm:
   - Tab 1 (Proforma): Address, acres, yield, rent/sqft, and build cost are auto-filled
   - Tab 2 (Market Comps): Drive-up and/or CC pricing grids populated with $/sqft averages
   - Tab 3 (Facility List): At least 3–5 facilities listed with distances

5. **Report key go/no-go metrics**:
   - H29: Equity Value (primary signal — positive = proceed)
   - H31: Payback Period (target < 12 years)
   - H33: Yield to Cost (target > cap rate of ~7%)

6. **Flag gaps**: Note any facilities with blank pricing (no website, dynamic pricing widgets, homepage-only URLs). These are known limitations — blank cells are expected, not errors.

Key files:
- Core pipeline: `comps_pipeline.py`
- Debug single facility: `test_pipeline.py`
- Templates: `claude excel model template.xlsx` (single/multi), `mixed_proforma_template.xlsx` (mixed)
- Proforma logic reference: `PROFORMA_LOGIC.md`
