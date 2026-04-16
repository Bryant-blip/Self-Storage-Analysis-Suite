# Proforma Logic & Assumptions

This document explains every assumption in the "Initial look proforma" tab,
why each default value was chosen, and what it means for the analysis.

---

## Auto-Filled Inputs (written by Python from Crexi)

| Cell | Label | Source |
|------|-------|--------|
| B3 | Property address | Crexi listing address |
| C5 | Acres | Crexi listing acreage |
| C6 | Cost of Land | Crexi asking price |
| C2 | Crexi URL | Hyperlink to original listing |
| E6 | Rent Per Sqft | Weighted drive-up avg $/sqft from market comps minus $0.05 |

---

## Assumptions (user-editable in template or per-report)

### E5 — Yield of Total Sqft → Rentable Sqft
**Default: 30%**

Of all the raw land square footage, only 30% becomes rentable storage space.
The remaining 70% is consumed by:
- Driveways and access lanes (typically 30–35% of site)
- Building setbacks and perimeter buffers
- Parking / office / retail area
- Stormwater detention or utility easements

A 30% efficiency ratio is conservative and standard for a first-pass feasibility
check. Newer urban infill projects may achieve 32–35%; rural single-story
projects typically land at 28–32%.

**Impact:** Directly drives net rentable sqft (H7), which drives all revenue.

---

### E6 — Rent Per Sqft
**Default: Auto-filled from market comps (weighted drive-up avg − $0.05)**

The weighted average online (discounted) drive-up rate per sqft from the
comparable storage facilities discovered within the search radius.

The $0.05 discount is applied because as a new competitor entering the market
we cannot assume we will achieve the same rates as established operators on
day one. This is a conservative underwriting assumption.

Unit mix weights used for the weighted average:
- 5×5:   12%
- 5×10:  25%
- 10×10: 30%
- 10×15: 15%
- 10×20: 12%
- 10×30:  6%

These weights reflect the typical mix seen in successful suburban self-storage
projects. Smaller units (5×10, 10×10) dominate because they serve the largest
tenant segment — residential movers, students, and apartment dwellers.

**Impact:** Largest single driver of revenue and valuation. A $0.10 change in
rent/sqft on a 50,000 sqft facility changes annual NOI by ~$54,000.

---

### E7 — Occupancy Rate
**Default: 90%**

Stabilized occupancy assumption — the long-run average once the facility has
leased up. Industry stabilized occupancy benchmarks (per CBRE / Marcus &
Millichap self-storage reports) range from 88–92% for well-located facilities.

90% is used because:
- Below 85% typically indicates a weak market or over-supply
- 95%+ is exceptional and not a safe underwriting assumption
- 90% represents a healthy, mid-cycle stabilized asset

Note: This is a stabilized figure. A new facility will ramp from 0% over
12–24 months before reaching this level. This proforma models the stabilized
state only — it does not model the lease-up period.

**Impact:** Directly multiplies rented sqft. Dropping to 85% reduces revenue
by ~5.5%.

---

### E8 — Expense Ratio
**Default: 25%**

Operating expenses as a percentage of gross revenue. Covers:
- Property management (8–10% of revenue if third-party managed)
- Insurance (1–2%)
- Property taxes (3–5%)
- Utilities (2–3%)
- Maintenance and repairs (2–3%)
- Marketing (1–2%)
- Administrative / software (1%)

25% is the standard industry benchmark for a well-run self-storage facility
(per CBRE Self-Storage Investor Survey). Ground-up new construction may run
slightly higher (27–30%) in early years due to lower occupancy spreading fixed
costs over less revenue.

**Impact:** Multiplies gross revenue to produce expenses. A 5-point change
(25% → 30%) reduces NOI by approximately 6.7%.

---

### E9 — Cap Rate
**Default: 7%**

The capitalization rate used to convert annual NOI into a stabilized property
value (Value = NOI / Cap Rate). Reflects the return a buyer would require
to purchase the stabilized asset.

7% is chosen because:
- Self-storage cap rates in secondary markets (where most Crexi deals are)
  typically range from 6.5–8.0% (CBRE 2024 Self-Storage Cap Rate Survey)
- Primary markets (major MSAs) trade at 5.5–6.5%
- Using 7% is conservative for secondary markets but realistic — it avoids
  the optimism of assuming a primary-market buyer
- It also provides a margin of safety vs. rising rate environments

**Impact:** The most sensitive assumption in the model. Changing from 7% to
6.5% increases the valuation by ~7.7%. Changing to 7.5% decreases it by ~6.7%.

---

### E10 — Cost to Build Per Sqft
**Default: $50/sqft**

Hard construction cost per rentable sqft for a standard single-story
drive-up self-storage facility. Includes:
- Site work and grading
- Foundation
- Metal building / shell
- Doors, corridors, partitions
- Electrical, basic HVAC (hallways only)
- General contractor overhead and profit

Does NOT include:
- Soft costs (architecture, engineering, permits): add ~8–12%
- Financing costs during construction
- Climate control premium: add ~$8–12/sqft for full CC facility
- Multi-story premium: add ~$15–25/sqft

$50/sqft reflects current construction costs for simple single-story drive-up
facilities. This is more representative of today's labor and materials
environment compared to the pre-2022 benchmark of $40/sqft. Gulf Coast and
Northeast markets may still run $55–65/sqft.

**Impact:** Directly drives total construction cost (H23) and therefore
total project cost (H25), equity value (H29), and payback period (H31).

---

## Calculated Outputs

| Cell | Label | Formula | What It Means |
|------|-------|---------|---------------|
| H5 | Total Land Sqft | `C5 × 43,560` | Converts acres to square feet |
| H6 | Yield % | `= E5` | Pass-through of the 30% efficiency assumption |
| H7 | Net Rentable Sqft | `H5 × H6` | The actual square footage you can rent out |
| H9 | Rent Per Sqft | `= E6` | Pass-through of the comps-derived rent |
| H10 | Occupancy Rate | `= E7` | Pass-through of occupancy assumption |
| H11 | Rented Sqft | `H10 × H7` | Occupied square footage at stabilization |
| H12 | Monthly Gross Revenue | `H11 × H9` | Top-line monthly income |
| H14 | Expense Ratio | `= E8` | Pass-through of expense assumption |
| H15 | Monthly Expenses | `H12 × H14` | Operating cost per month |
| H16 | Monthly NOI | `H12 − H15` | Net Operating Income per month |
| H18 | Annual NOI | `H16 × 12` | Full-year stabilized NOI |
| H19 | Cap Rate | `= E9` | Pass-through of cap rate assumption |
| H20 | Valuation | `H18 ÷ H19` | What the stabilized asset is worth to a buyer |
| H22 | Cost to Build / Sqft | `= E10` | Pass-through of construction cost |
| H23 | Construction Costs | `H22 × H7` | Total hard cost to build the facility |
| H24 | Cost of Land | `= C6` | Crexi asking price |
| H25 | Total Cost to Build | `H23 + H24` | All-in project cost (land + construction) |
| H27 | Property Value | `= H20` | Stabilized asset value |
| H28 | Less: Cost to Build | `= H25` | Total project cost |
| H29 | Equity Value | `H27 − H28` | Value created above cost — the "profit" |
| H31 | Payback Period | `H25 ÷ H18` | Years for NOI to pay back total cost |
| H33 | Yield to Cost | `H18 ÷ H25` | Annual return on total invested capital |

---

## How to Read the Output

**Equity Value (H29)** is the primary go/no-go signal. A positive number
means the stabilized value exceeds the cost to build — you are creating value.
Negative means the deal destroys value at these assumptions.

**Payback Period (H31)** — industry rule of thumb: under 12 years is
acceptable; under 10 years is good; under 8 years is excellent.

**Yield to Cost (H33)** — target is above the cap rate (E9). If yield to cost
exceeds the cap rate, the project generates a spread above market pricing.
Example: 8% yield to cost vs. 7% cap rate = positive leverage.

---

## Key Risks / Sensitivity

1. **Rent assumption** is the #1 driver — verify comps carefully before
   proceeding past first pass.
2. **Construction costs** can vary ±30% based on site conditions, market,
   and contractor availability. Get a real contractor bid before committing.
3. **Cap rate** will move with interest rates — if rates rise, cap rates
   expand and valuation shrinks.
4. **This model does not underwrite lease-up** — a new facility will lose
   money for 12–24 months before hitting stabilized occupancy. Factor in
   carrying costs separately.

