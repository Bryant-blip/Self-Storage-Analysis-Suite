# Proforma Logic & Assumptions

This document explains every assumption in the proforma tabs,
why each default value was chosen, and what it means for the analysis.

---

## Facility Type Classification

The pipeline auto-detects the facility type based on parcel acreage:

| Type | Acres | Yield | Construction Cost | Rent Source | Template |
|------|-------|-------|-------------------|-------------|----------|
| **single_story** | > 4 | 40% of land | $50/sqft | Drive-up in-store comps weighted avg | `claude excel model template.xlsx` |
| **multi_story** | < 2 | 122% of land | $95/sqft | CC in-store comps weighted avg | `claude excel model template.xlsx` |
| **mixed** | 2–4 | Dynamic split | $95 (MS) + $50 (SS) | Separate CC + DU rates | `mixed_proforma_template.xlsx` |

**Yield derivations:**
- **Single-story 40%**: Industry standard site efficiency for single-story drive-up.
- **Multi-story 122%**: Lehi, UT reference — 1.6 acres -> 85,000 rentable sqft.
  `85,000 / (1.6 x 43,560) = 121.96%`. Exceeds 100% because multi-story stacks
  floors above the land footprint.
- **Mixed**: Dynamically splits the land between multi-story (122% yield) and
  single-story (40% yield) to hit a **90,000 rentable sqft target**.
  Formula: `ms_frac = (90,000 / land_sqft - 0.40) / (1.22 - 0.40)`

| Acres | Multi-Story % of land | Single-Story % of land | MS Sqft | SS Sqft | Total |
|-------|----------------------|----------------------|---------|---------|-------|
| 2.0 | 77% | 23% | 81,774 | 8,226 | 90,000 |
| 3.0 | 35% | 65% | 56,114 | 33,886 | 90,000 |
| 4.0 | 14% | 86% | 30,455 | 59,545 | 90,000 |

---

## Single-Story & Multi-Story Proforma

These use the standard `claude excel model template.xlsx` template with a single
set of assumptions. The only differences are the auto-filled values for yield (E5),
construction cost (E10), and rent source (E6).

### Auto-Filled Inputs (written by Python)

| Cell | Label | Source |
|------|-------|--------|
| B3 | Property address | Crexi listing address |
| C5 | Acres | Crexi listing acreage |
| C6 | Cost of Land | Crexi asking price |
| C2 | Crexi URL | Hyperlink to original listing |
| E5 | Yield of Total Sqft | 0.40 (single-story) or 1.22 (multi-story) |
| E6 | Rent Per Sqft | Weighted avg in-store $/sqft from market comps minus $0.05 |
| E10 | Cost to Build Per Sqft | $50 (single-story) or $95 (multi-story) |
| D3/E3 | Facility Type | "single_story" or "multi_story" |

### Assumptions (user-editable)

#### E5 — Yield of Total Sqft
**Default: 40% (single-story) or 122% (multi-story)**

See Facility Type Classification above for derivation.

**Impact:** Directly drives net rentable sqft (H7), which drives all revenue.

---

#### E6 — Rent Per Sqft
**Default: Auto-filled from market comps (weighted avg - $0.05)**

The weighted average in-store (standard/walk-in) rate per sqft from comparable
storage facilities. For single-story, uses drive-up comps. For multi-story, uses
climate-controlled comps. Falls back to drive-up if CC comps unavailable.

In-store rates are used rather than online/discounted rates because they represent
the established market rate — the price operators charge at the counter.

The $0.05 discount is applied because as a new competitor entering the market
we cannot assume we will achieve the same rates as established operators on
day one.

Unit mix weights used for the weighted average:
- 5x5:   12%
- 5x10:  25%
- 10x10: 30%
- 10x15: 15%
- 10x20: 12%
- 10x30:  6%

**Impact:** Largest single driver of revenue and valuation.

---

#### E7 — Occupancy Rate
**Default: 90%**

Stabilized occupancy assumption. Industry benchmarks (CBRE / Marcus & Millichap)
range from 88-92% for well-located facilities. This proforma models the stabilized
state only — it does not model the lease-up period.

---

#### E8 — Expense Ratio
**Default: 25%**

Operating expenses as a percentage of gross revenue. Covers property management
(8-10%), insurance (1-2%), property taxes (3-5%), utilities (2-3%),
maintenance (2-3%), marketing (1-2%), admin/software (1%).

---

#### E9 — Cap Rate
**Default: 7%**

Capitalization rate used to convert annual NOI into stabilized property value.
Self-storage cap rates in secondary markets typically range from 6.5-8.0%.

**Impact:** Most sensitive assumption. Changing from 7% to 6.5% increases
valuation by ~7.7%.

---

#### E10 — Cost to Build Per Sqft
**Default: $50 (single-story) or $95 (multi-story)**

Hard construction cost per rentable sqft. Single-story covers standard
drive-up facilities. Multi-story covers elevator-served climate-controlled
buildings with higher structural, HVAC, and elevator costs.

Does NOT include soft costs (8-12%), financing costs, or multi-story premium
(already built into the $95 rate for multi-story).

---

### Calculated Outputs

| Cell | Label | Formula |
|------|-------|---------|
| H5 | Total Land Sqft | `C5 x 43,560` |
| H6 | Yield % | `= E5` |
| H7 | Net Rentable Sqft | `H5 x H6` |
| H9 | Rent Per Sqft | `= E6` |
| H10 | Occupancy Rate | `= E7` |
| H11 | Rented Sqft | `H10 x H7` |
| H12 | Monthly Gross Revenue | `H11 x H9` |
| H14 | Expense Ratio | `= E8` |
| H15 | Monthly Expenses | `H12 x H14` |
| H16 | Monthly NOI | `H12 - H15` |
| H18 | Annual NOI | `H16 x 12` |
| H19 | Cap Rate | `= E9` |
| H20 | Valuation | `H18 / H19` |
| H22 | Cost to Build / Sqft | `= E10` |
| H23 | Construction Costs | `H22 x H7` |
| H24 | Cost of Land | `= C6` |
| H25 | Total Cost to Build | `H23 + H24` |
| H27 | Property Value | `= H20` |
| H28 | Less: Cost to Build | `= H25` |
| H29 | Equity Value | `H27 - H28` |
| H31 | Payback Period | `H25 / H18` |
| H33 | Yield to Cost | `H18 / H25` |

---

## Mixed Facility Proforma

Mixed facilities (2-4 acres) use a dedicated template (`mixed_proforma_template.xlsx`)
with two mini-proformas — one for the climate-controlled multi-story portion and
one for the drive-up single-story portion. Each mini-proforma has its own editable
assumptions, and changes flow up to the main summary automatically.

### Layout

**Columns B-C**: Property overview (Acres, Cost of Land, Cap Rate)
**Columns B-G**: Two mini-proformas (CC rows 13-20, DU rows 22-29)
**Columns I-J**: Main output summary (pulls from both mini-proformas)

### Auto-Filled Inputs (written by Python)

| Cell | Label | Source |
|------|-------|--------|
| B3 | Property address | Crexi listing address |
| C5 | Acres | Crexi listing acreage |
| C6 | Cost of Land | Crexi asking price |
| C8 | Cap Rate | 7% default |
| F12 | Land Split | Computed split (e.g. "35% multi-story / 65% single-story") |
| B15 | CC Rentable Sqft | Computed from land split x 122% yield |
| D15 | CC Rent Per Sqft | CC in-store comps weighted avg - $0.05 |
| D18 | CC Cost Per Sqft | $95 |
| B24 | DU Rentable Sqft | Computed from land split x 40% yield |
| D24 | DU Rent Per Sqft | Drive-up in-store comps weighted avg - $0.05 |
| D27 | DU Cost Per Sqft | $50 |

### CC Mini-Proforma (rows 13-20)

| Cell | Label | Default |
|------|-------|---------|
| B15 | Rentable Sqft | Computed (e.g. 56,141 for 3 acres) |
| D15 | Rent Per Sqft | CC in-store comps weighted avg - $0.05 |
| D16 | Occupancy | 90% |
| D17 | Expense Ratio | 25% |
| D18 | Cost Per Sqft | $95 |
| G15 | Rented Sqft | `= B15 x D16` |
| G16 | Monthly Revenue | `= G15 x D15` |
| G17 | Monthly Expenses | `= G16 x D17` |
| G18 | Monthly NOI | `= G16 - G17` |
| G19 | Annual NOI | `= G18 x 12` |
| G20 | Construction Cost | `= B15 x D18` |

### DU Mini-Proforma (rows 22-29)

Same structure as CC, at rows 22-29 with drive-up assumptions.

### Main Output Summary (columns I-J)

| Cell | Label | Formula |
|------|-------|---------|
| J5 | Total Land Sqft | `= C5 x 43560` |
| J6 | Net Rentable Sqft | `= B15 + B24` |
| J7 | Monthly Gross Revenue | `= G16 + G25` |
| J9 | Monthly Expenses | `= G17 + G26` |
| J10 | Monthly NOI | `= J7 - J9` |
| J12 | Annual NOI | `= J10 x 12` |
| J13 | Cap Rate | `= C8` |
| J14 | Valuation | `= J12 / J13` |
| J16 | Construction Costs | `= G20 + G29` |
| J17 | Cost of Land | `= C6` |
| J18 | Total Cost to Build | `= J16 + J17` |
| J20 | Property Value | `= J14` |
| J21 | Less: Cost to Build | `= J18` |
| J22 | Equity Value | `= J20 - J21` |
| J24 | Payback Period | `= J18 / J12` |
| J26 | Yield to Cost | `= J12 / J18` |

---

## How to Read the Output

**Equity Value (H29 or J22)** is the primary go/no-go signal. A positive number
means the stabilized value exceeds the cost to build — you are creating value.

**Payback Period (H31 or J24)** — under 12 years is acceptable; under 10 is good;
under 8 is excellent.

**Yield to Cost (H33 or J26)** — target is above the cap rate. If yield to cost
exceeds the cap rate, the project generates a spread above market pricing.

---

## Key Risks / Sensitivity

1. **Rent assumption** is the #1 driver — verify comps carefully.
2. **Construction costs** can vary +/-30% based on site conditions and market.
3. **Cap rate** will move with interest rates.
4. **This model does not underwrite lease-up** — a new facility will lose
   money for 12-24 months before hitting stabilized occupancy.
5. **Mixed facilities** — the land split is computed to hit 90,000 sqft target;
   actual split depends on site-specific factors (zoning, access, topography).
