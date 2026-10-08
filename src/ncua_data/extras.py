"""Extra earnings ratios on NCUA's own denominators, plus single-quarter annualized versions.

Two families, both computed from the curated call report table:

* ``*_ncua_ytd``: year-to-date flow annualized, over the average of the prior December balance and the current
  quarter-end balance. This is the denominator NCUA uses in its Quarterly Credit Union Data Summary (checked
  against every published quarter, see docs/RECONCILIATION.md).
* ``*_quarterly``: the single quarter's flow times 4, over the average of the prior quarter-end balance and the
  current quarter-end balance. NCUA does not publish this one. It exists because year-to-date annualized ratios
  re-base each January and so step at Q1.

Ratios are fractions (0.05 = 5%). A value is NULL when the needed prior balance or prior quarter is missing.
"""
from __future__ import annotations

# Flow fields used by the ratios (year to date in the curated table).
_FLOWS = {
    "ni": "net_income_ytd", "ii": "interest_income_ytd", "ie": "interest_expense_ytd",
    "iol": "interest_on_loans_ytd", "co": "(chargeoffs_ytd - recoveries_ytd)",
}

EXTRA_COLUMNS = [
    "interest_on_loans_quarter", "net_chargeoffs_quarter",
    "roa_ncua_ytd", "nim_ncua_ytd", "loan_yield_ncua_ytd", "cost_of_funds_ncua_ytd", "net_chargeoff_rate_ncua_ytd",
    "roa_quarterly", "nim_quarterly", "loan_yield_quarterly", "cost_of_funds_quarterly", "net_chargeoff_rate_quarterly",
]


def extras_sql(fact: str) -> str:
    """SQL producing quarter, cu_number and EXTRA_COLUMNS. ``fact`` is the curated call report table or view."""
    return f"""
WITH b AS (
  SELECT f.quarter, f.cu_number, CAST(left(f.quarter, 4) AS INTEGER) AS yr,
         CAST(substr(f.quarter, 6, 2) AS INTEGER) / 3 AS qn,
         f.total_assets AS a, f.loans_and_leases_total AS l,
         f.net_income_ytd AS ni, f.interest_income_ytd AS ii, f.interest_expense_ytd AS ie,
         f.interest_on_loans_ytd AS iol, (f.chargeoffs_ytd - f.recoveries_ytd) AS co
  FROM {fact} f
), j AS (
  SELECT b.*, 4.0 / b.qn AS ann,
         d.a AS a_dec, d.l AS l_dec,
         p.a AS a_pq, p.l AS l_pq, p.ni AS ni_pq, p.ii AS ii_pq, p.ie AS ie_pq, p.iol AS iol_pq, p.co AS co_pq
  FROM b
  LEFT JOIN b d ON d.cu_number = b.cu_number AND d.yr = b.yr - 1 AND d.qn = 4
  LEFT JOIN b p ON p.cu_number = b.cu_number AND p.yr = b.yr AND p.qn = b.qn - 1
), q AS (
  SELECT j.*,
         CASE WHEN qn = 1 THEN ni  WHEN ni_pq  IS NOT NULL THEN ni  - ni_pq  END AS ni_q,
         CASE WHEN qn = 1 THEN ii  WHEN ii_pq  IS NOT NULL THEN ii  - ii_pq  END AS ii_q,
         CASE WHEN qn = 1 THEN ie  WHEN ie_pq  IS NOT NULL THEN ie  - ie_pq  END AS ie_q,
         CASE WHEN qn = 1 THEN iol WHEN iol_pq IS NOT NULL THEN iol - iol_pq END AS iol_q,
         CASE WHEN qn = 1 THEN co  WHEN co_pq  IS NOT NULL THEN co  - co_pq  END AS co_q,
         CASE WHEN qn = 1 THEN a_dec ELSE a_pq END AS a_prev,
         CASE WHEN qn = 1 THEN l_dec ELSE l_pq END AS l_prev
  FROM j
)
SELECT quarter, cu_number,
  iol_q AS interest_on_loans_quarter,
  co_q AS net_chargeoffs_quarter,
  ni * ann / nullif((a + a_dec) / 2, 0) AS roa_ncua_ytd,
  (ii - ie) * ann / nullif((a + a_dec) / 2, 0) AS nim_ncua_ytd,
  iol * ann / nullif((l + l_dec) / 2, 0) AS loan_yield_ncua_ytd,
  ie * ann / nullif((a + a_dec) / 2, 0) AS cost_of_funds_ncua_ytd,
  co * ann / nullif((l + l_dec) / 2, 0) AS net_chargeoff_rate_ncua_ytd,
  ni_q * 4 / nullif((a + a_prev) / 2, 0) AS roa_quarterly,
  (ii_q - ie_q) * 4 / nullif((a + a_prev) / 2, 0) AS nim_quarterly,
  iol_q * 4 / nullif((l + l_prev) / 2, 0) AS loan_yield_quarterly,
  ie_q * 4 / nullif((a + a_prev) / 2, 0) AS cost_of_funds_quarterly,
  co_q * 4 / nullif((l + l_prev) / 2, 0) AS net_chargeoff_rate_quarterly
FROM q
"""
