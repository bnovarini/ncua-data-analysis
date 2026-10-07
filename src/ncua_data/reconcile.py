"""Reconcile this dataset against NCUA's own published aggregate numbers.

The expected values are copied by hand from NCUA's Quarterly Credit Union Data
Summary PDFs (links in the README). Everything is rounded the way NCUA rounds,
so each check carries a tolerance of one unit in the last published digit.
Anything outside tolerance is reported as a mismatch, never adjusted away.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Check:
    quarter: str
    label: str
    expr: str  # SQL over the insured-only aggregate view `agg`
    published: float
    tol: float
    source: str


SRC_2026Q2 = "https://ncua.gov/files/publications/analysis/quarterly-data-summary-2026-Q2.pdf"
SRC_2025Q4 = "https://ncua.gov/files/publications/analysis/quarterly-data-summary-2025-Q4.pdf"

CHECKS = [
    Check("2026-06", "federally insured credit unions", "n", 4214, 0, SRC_2026Q2),
    Check("2026-06", "members (millions)", "members / 1e6", 146.1, 0.05, SRC_2026Q2),
    Check("2026-06", "total loans ($ trillion)", "loans / 1e12", 1.76, 0.005, SRC_2026Q2),
    Check("2026-06", "new auto loans ($B)", "new_auto / 1e9", 160.7, 0.05, SRC_2026Q2),
    Check("2026-06", "used auto loans ($B)", "used_auto / 1e9", 324.7, 0.05, SRC_2026Q2),
    Check("2026-06", "credit card balances ($B)", "credit_card / 1e9", 87.2, 0.05, SRC_2026Q2),
    Check("2026-06", "student loans ($B)", "student / 1e9", 6.2, 0.05, SRC_2026Q2),
    Check("2026-06", "1-4 family residential loans ($B)", "residential / 1e9", 834.1, 0.05, SRC_2026Q2),
    Check("2026-06", "commercial loans ($B)", "commercial / 1e9", 201.7, 0.05, SRC_2026Q2),
    Check("2026-06", "insured shares and deposits ($ trillion)", "insured / 1e12", 1.91, 0.005, SRC_2026Q2),
    Check("2026-06", "total shares and deposits ($ trillion)", "shares / 1e12", 2.13, 0.005, SRC_2026Q2),
    Check("2026-06", "regular shares ($B)", "regular / 1e9", 588.1, 0.05, SRC_2026Q2),
    Check("2026-06", "delinquency rate (bp)", "delinquent / loans * 1e4", 96, 0.5, SRC_2026Q2),
    Check("2026-06", "loan to share ratio (%)", "loans / shares * 100", 82.9, 0.05, SRC_2026Q2),
    Check("2026-06", "net worth ratio (%)", "(net_worth - cecl) / assets * 100", 11.42, 0.005, SRC_2026Q2),
    Check("2026-06", "net income, annualized ($B)", "ni_ytd * 4/2 / 1e9", 22.4, 0.05, SRC_2026Q2),
    Check("2026-06", "interest income, annualized ($B)", "ii_ytd * 4/2 / 1e9", 128.2, 0.05, SRC_2026Q2),
    Check("2026-06", "non-interest income, annualized ($B)", "nii_ytd * 4/2 / 1e9", 27.8, 0.05, SRC_2026Q2),
    Check("2026-06", "interest expense, annualized ($B)", "ie_ytd * 4/2 / 1e9", 42.1, 0.05, SRC_2026Q2),
    Check("2026-06", "non-interest expense, annualized ($B)", "nie_ytd * 4/2 / 1e9", 77.8, 0.05, SRC_2026Q2),
    Check("2026-06", "net interest margin, annualized ($B)", "(ii_ytd - ie_ytd) * 4/2 / 1e9", 86.1, 0.05, SRC_2026Q2),
    Check("2026-06", "cash ($B)", "cash / 1e9", 187.2, 0.05, SRC_2026Q2),
    Check("2025-12", "federally insured credit unions", "n", 4287, 0, SRC_2025Q4),
    Check("2025-12", "members (millions)", "members / 1e6", 144.7, 0.05, SRC_2025Q4),
    Check("2025-12", "total loans ($ trillion)", "loans / 1e12", 1.72, 0.005, SRC_2025Q4),
    Check("2025-12", "delinquency rate (bp)", "delinquent / loans * 1e4", 103, 0.5, SRC_2025Q4),
    Check("2025-12", "loan to share ratio (%)", "loans / shares * 100", 83.2, 0.05, SRC_2025Q4),
    Check("2025-12", "net worth ratio (%)", "(net_worth - cecl) / assets * 100", 11.26, 0.005, SRC_2025Q4),
    Check("2025-12", "net income, full year ($B)", "ni_ytd / 1e9", 18.8, 0.05, SRC_2025Q4),
    Check("2025-12", "net interest margin, full year ($B)", "(ii_ytd - ie_ytd) / 1e9", 80.4, 0.05, SRC_2025Q4),
]

_DEP = {  # NCUA table, $B: share drafts, regular, money market, certificates, IRA/Keogh, non-member
    "2021-12": (367.1, 655.9, 407.3, 247.6, 83.2, 11.3), "2022-12": (382.1, 656.8, 394.6, 296.8, 82.4, 21.5),
    "2023-12": (369.5, 569.1, 331.7, 483.9, 84.8, 27.6), "2024-12": (378.5, 555.5, 338.4, 561.2, 86.8, 26.3),
    "2025-12": (406.3, 569.8, 368.0, 597.3, 87.9, 25.1), "2026-06": (414.2, 588.1, 386.2, 611.4, 87.9, 24.1),
}
for _q, _v in _DEP.items():
    for _lbl, _e, _x in zip(["share drafts", "regular shares", "money market accounts", "share certificates", "IRA/Keogh accounts", "non-member deposits"],
                            ["drafts", "regular", "mm", "certs", "ira", "nonmember"], _v):
        CHECKS.append(Check(_q, f"{_lbl} ($B)", f"{_e} / 1e9", _x, 0.05, SRC_2026Q2))
CHECKS.append(Check("2026-06", "employee compensation, annualized ($B)", "comp_ytd * 4/2 / 1e9", 41.1, 0.05, SRC_2026Q2))

AGG_SQL = """
CREATE OR REPLACE TEMP VIEW agg AS
SELECT f.quarter, count(*) AS n, sum(members) AS members, sum(loans_and_leases_total) AS loans,
  sum(loans_new_vehicle) AS new_auto, sum(loans_used_vehicle) AS used_auto, sum(loans_credit_card) AS credit_card,
  sum(loans_student) AS student,
  sum(loans_first_lien_residential + loans_junior_lien_residential) AS residential,
  sum(loans_commercial_total) AS commercial, sum(insured_shares) AS insured,
  sum(total_shares_and_deposits) AS shares, sum(shares_regular) AS regular,
  sum(delinquent_2m_plus) AS delinquent, sum(coalesce(cecl_transition_provision, 0)) AS cecl,
  sum(shares_share_drafts) AS drafts, sum(shares_money_market) AS mm, sum(shares_certificates) AS certs,
  sum(shares_ira_keogh) AS ira, sum(deposits_non_member) AS nonmember, sum(employee_compensation_ytd) AS comp_ytd, sum(net_worth) AS net_worth, sum(total_assets) AS assets,
  sum(net_income_ytd) AS ni_ytd, sum(interest_income_ytd) AS ii_ytd, sum(non_interest_income_ytd) AS nii_ytd,
  sum(interest_expense_ytd) AS ie_ytd, sum(non_interest_expense_ytd) AS nie_ytd,
  sum(cash_on_hand + cash_on_deposit) AS cash
FROM fact_call_report_curated f JOIN dim_credit_union d USING (quarter, cu_number)
WHERE d.is_federally_insured GROUP BY f.quarter
"""


def run(con) -> list[dict]:
    con.execute(AGG_SQL)
    out = []
    for c in CHECKS:
        v = con.execute(f"SELECT {c.expr} FROM agg WHERE quarter = '{c.quarter}'").fetchone()[0]
        out.append({"quarter": c.quarter, "check": c.label, "dataset": round(float(v), 3),
                    "published": c.published, "tolerance": c.tol,
                    "ok": abs(float(v) - c.published) <= c.tol + 1e-9, "source": c.source})
    return out
