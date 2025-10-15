# Redacted Donor Household Model (Originally Built for MBI's Data Warehouse)

This repository contains a **redacted** subset of SQL scripts that mirror the structure of donor and gift analytics tables I created for the Moody Bible Institute's (MBI) data warehouse.
All proprietary schema names, internal table names, and organization-specific literals have been replaced with generic placeholders. The business logic is preserved where possible to demonstrate approach and technique, while protecting confidential implementation details.

## What’s Included
- Analytics-ready tables for householding, donor lifecycle, gift enrichment, and staff assignments.
- **Regular Oracle SQL** only (no platform-specific extensions). The original code was written to run inside Power BI and was later migrated to the data warehouse.
- Redacted table names: created tables assume a `dw` schema; source/operational tables assume a `src` schema.

## Notes on Authorship
- The person summary logic was primarily developed by **Adam Reece**; I made subsequent edits and integrated it with the broader analytics layer.
- All other scripts were written by **Jesse Hanvey**.

## Implementation Notes
- All created tables use descriptive names and are assumed to live in the `dw` schema in this redacted version.
- All source/operational tables are assumed to live in a generic `src` schema.
- Filters referencing the original organization name were normalized to `'ORG'` to avoid exposing institutional identifiers.
- The lifecycle status logic (`LIFECYCLE_STATUS.sql`) is written in plain SQL (originally, this SQL was used within Power BI). It could be refactored in PL/SQL to reduce repetition via loops.

## Files
- `PERSON_SUMMARY.sql` — Person-level rollup used to generate household IDs and enrich constituent attributes.
- `HOUSEHOLD_ASSIGNMENTS.sql` — Links households to assigned giving representatives (active assignments, deduplicated).
- `HOUSEHOLD_INFO.sql` — Aggregates household-level donor category, giving level, and involvement flags with postal/source snapshots.
- `LIFECYCLE_STATUS.sql` — Classifies each household’s lifecycle state by fiscal year using a rolling multi-year lookback and deceased-household logic.
- `GIFT_HOUSEHOLD_LIFECYCLE.sql` — Central gift-level table enriched with household and lifecycle context, including first-time gift flags and designation details.

---

## Donor Household Model — Rationale and Design

### Why householding?
Historically, analytics were person-centric and scattered across many reports. That created several issues: double-counting spouses, inconsistent classification logic, and repeated ad‑hoc transformations across datasets. The household model addresses these gaps by providing a stable, analytics‑ready entity that represents how gifts are **experienced and stewarded** in practice.

### How households are formed
- **Stable Household IDs:** Spouses are combined into a single household identifier to ensure that a married couple’s giving is analyzed together rather than as separate donors. This prevents double‑counting and produces a truer view of acquisition, retention, and gift totals.
- **DAF & Company Reassignment:** When gifts are routed through Donor‑Advised Funds or companies, they are **reassigned** back to the donor behind the gift. This aligns the analytics view with donor relationships (while accounting keeps its own ledgering). The result is a clearer picture of who is actually giving and responding to appeals.
- **First Gift Identification:** The first recorded gift per household is flagged. This enables clean cohorting (e.g., “FY2024 Q1 new donors”), first gift analysis, and downstream retention analysis.
- **Lifecycle Classification:** Each household is placed into a fiscal‑year lifecycle bucket (e.g., *New Donor*, *Key Multi‑Year*, *Recently Lapsed*, *Long Lapsed*). Lifecycle states are computed at the start of the fiscal year to support year‑over‑year tracking and movement analysis.
- **Portfolio Awareness:** Households are linked to their assigned giving representatives, which allows portfolio‑level analysis of upgrade, lapse, and reactivation patterns for higher‑value segments.

### What changed when we shifted reporting to households
1. **Accuracy (no more double‑counting):** Combining spouses as one entity removed inflation in donor counts and clarified conversion and retention rates.
2. **Clarity on “who gave”:** Reassigning DAF and corporate gifts to the underlying donor made campaign response and donor journeys interpretable (vs. seeing only the intermediary entity).
3. **Consistent lifecycle logic:** Lifecycle status was standardized and centralized, eliminating drift between reports and ensuring like‑for‑like comparisons across fiscal years.
4. **Fewer repeated transformations:** Common manipulations (cohorting, fiscal‑year handling, designation joins) moved into warehouse tables, so Power BI models became thinner, faster, and easier to maintain.
5. **Better portfolio management:** With households tied to giving reps, leadership could evaluate pipeline health and outcomes by portfolio, not just by campaign or channel.
6. **New analytics possibilities:** Household‑level features (donor category, giving level, monthly partner flags) unlocked richer segmentation and lifted the ceiling on predictive modeling and retention analyses.
7. **Operational efficiency:** Nightly refreshes and pre‑joined data cut report load times and reduced breakage when source systems changed, while keeping dashboards up‑to‑date for daily decision‑making.

---

## Reporting & Analytics Use Cases (Examples)
- **Acquisition & Retention:** Track first‑year retention, multi‑year donor growth, and reactivation by cohort and portfolio.
- **Lifecycle Movement:** Monitor transitions between *New*, *Key Multi‑Year*, *Recently Lapsed*, and *Lapsed* across fiscal years.
- **Appeal & Channel Performance:** Evaluate gift source, designation, and motivation in the context of household history rather than isolated transactions.
- **Major & Mid‑level Programs:** Identify and follow households appropriate for portfolio assignment, then measure outcomes by representative.
- **Data Reduction in BI Models:** Replace dozens of SQL and data manipulation steps in reports with a single warehouse table import per subject area.

---

## Intended Use
This redacted code is provided **for demonstration purposes only** to illustrate modeling decisions, SQL architecture, and analytics patterns. It is **not** a drop‑in solution and omits environment‑specific dependencies, indexes, and orchestration steps.

## License
Copyright © Jesse Hanvey.
Business logic and data model concepts derived from work performed for Moody Bible Institute are presented here in redacted form.
