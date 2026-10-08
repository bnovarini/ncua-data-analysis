"""Build the data dictionary table: one row per column in every published table."""
from __future__ import annotations

from .spec import FIELDS

METRIC_DOCS = {
    "asset_growth_yoy": ("Total assets versus the same quarter one year earlier.", "ratio"),
    "loan_growth_yoy": ("Total loans versus the same quarter one year earlier.", "ratio"),
    "share_growth_yoy": ("Total shares and deposits versus one year earlier.", "ratio"),
    "member_growth_yoy": ("Members versus one year earlier.", "ratio"),
    "auto_loan_growth_yoy": ("New plus used vehicle loans versus one year earlier.", "ratio"),
    "first_lien_growth_yoy": ("First-lien 1-4 family loans versus one year earlier.", "ratio"),
    "loan_to_share": ("Loans divided by total shares and deposits. NCUA's headline liquidity ratio.", "ratio"),
    "loans_to_assets": ("Loans divided by total assets.", "ratio"),
    "net_worth_to_assets": ("Net worth divided by total assets (1.0 = 100%).", "ratio"),
    "allowance_to_loans": ("Allowance for credit losses divided by total loans.", "ratio"),
    "net_worth_ratio_ex_cecl": ("Net worth minus the CECL transition provision, over assets. This is how NCUA publishes its net worth ratio from 2023 on.", "ratio"),
    "deposit_mix_share_drafts": ("Share draft (checking) accounts as a share of total shares and deposits.", "ratio"),
    "deposit_mix_regular": ("Regular shares as a share of total shares and deposits.", "ratio"),
    "deposit_mix_money_market": ("Money market shares as a share of total shares and deposits.", "ratio"),
    "deposit_mix_certificates": ("Share certificates as a share of total shares and deposits.", "ratio"),
    "deposit_mix_ira_keogh": ("IRA and Keogh accounts as a share of total shares and deposits.", "ratio"),
    "deposit_mix_non_member": ("Non-member deposits as a share of total shares and deposits.", "ratio"),
    "avg_balance_per_listed_account": ("Total shares and deposits over the count of share draft, certificate, money market, IRA and non-member accounts. Rough; regular share account counts are not collected.", "dollars"),
    "employees_fte_estimate": ("Full-time employees plus half of part-time employees. An estimate, not an NCUA definition.", "count"),
    "members_per_fte": ("Members per estimated full-time-equivalent employee.", "count"),
    "assets_per_fte": ("Assets per estimated full-time-equivalent employee.", "dollars"),
    "compensation_per_fte": ("Annualized employee compensation and benefits per estimated FTE.", "dollars"),
    "operating_expense_per_fte": ("Annualized non-interest expense per estimated FTE.", "dollars"),
    "assets_per_branch": ("Assets per branch.", "dollars"),
    "members_per_branch": ("Members per branch.", "count"),
    "compensation_share_of_opex": ("Employee compensation and benefits as a share of non-interest expense.", "ratio"),
    "loans_per_member": ("Average loan dollars per member.", "dollars"),
    "shares_per_member": ("Average shares and deposits per member.", "dollars"),
    "mix_auto": ("New plus used vehicle loans as a share of total loans.", "ratio"),
    "mix_residential_real_estate": ("First lien, junior lien and other real estate loans as a share of total loans.", "ratio"),
    "mix_credit_card": ("Credit card loans as a share of total loans.", "ratio"),
    "mix_commercial": ("Commercial loans as a share of total loans.", "ratio"),
    "delinquency_rate": ("Loans delinquent two or more months divided by total loans. Matches NCUA's published rate.", "ratio"),
    "auto_delinquency_rate": ("Reportable delinquent vehicle loans divided by vehicle loans.", "ratio"),
    "credit_card_delinquency_rate": ("Delinquent credit card loans divided by credit card loans.", "ratio"),
    "net_chargeoff_rate": ("(Charge-offs minus recoveries), annualized from year to date, over year-end loans.", "ratio"),
    "net_chargeoff_rate_avg_loans_4q": ("Net charge-offs annualized from year to date, over the average of the last four quarter-end loans. NCUA publishes the ratio on a different average; see net_chargeoff_rate_ncua_ytd.", "ratio"),
    "provision_to_loans": ("Provision (credit loss expense) annualized over year-end loans.", "ratio"),
    "roa_year_end_assets": ("Return on assets: net income annualized from year to date, over year-end assets.", "ratio"),
    "roa_avg_assets_4q": ("Return on assets: net income annualized from year to date, over the average of the last four quarter-end assets. Prefer roa_ncua_ytd, which uses NCUA's own average.", "ratio"),
    "nim_year_end_assets": ("Net interest margin: (interest income minus interest expense) annualized from year to date, over year-end assets.", "ratio"),
    "nim_avg_assets_4q": ("Net interest margin (NIM): same, over the four-quarter average of assets. Prefer nim_ncua_ytd, which matches NCUA's published 3.49% for June 2026 exactly in aggregate.", "ratio"),
    "loan_yield": ("Yield on loans: interest on loans annualized from year to date, over year-end loans. Lags in rising-rate years; loan_yield_ncua_ytd uses NCUA's average balance.", "ratio"),
    "cost_of_shares": ("Cost of shares: total interest expense annualized from year to date, over shares and deposits. Not NCUA's cost of funds; see cost_of_funds_ncua_ytd.", "ratio"),
    "efficiency_ratio": ("Non-interest expense divided by (net interest income plus non-interest income).", "ratio"),
    "opex_to_assets": ("Non-interest expense annualized over assets.", "ratio"),
    "fee_share_of_non_interest_income": ("Fee income divided by non-interest income.", "ratio"),
    "net_income_quarter": ("Net income for the single quarter (year-to-date minus prior quarter).", "dollars"),
    "interest_income_quarter": ("Interest income for the single quarter.", "dollars"),
    "interest_expense_quarter": ("Interest expense for the single quarter.", "dollars"),
    "non_interest_expense_quarter": ("Non-interest expense for the single quarter.", "dollars"),
    "roa_ncua_ytd": ("Return on average assets on NCUA's basis: year-to-date net income annualized, over the average of prior-December and current quarter-end assets. Pooled across all insured credit unions it reproduces NCUA's published ROA to within a basis point for every quarter 2019-2026.", "ratio"),
    "nim_ncua_ytd": ("Net interest margin (NIM) on NCUA's basis: (interest income minus interest expense) annualized from year to date, over the average of prior-December and current assets. Pooled, it reproduces NCUA's published NIM percentage.", "ratio"),
    "loan_yield_ncua_ytd": ("Yield on loans on NCUA's basis: interest on loans annualized from year to date, over the average of prior-December and current loans. The median across credit unions reproduces NCUA's published median yield on loans.", "ratio"),
    "cost_of_funds_ncua_ytd": ("Cost of funds on NCUA's basis: total interest expense annualized from year to date, over the average of prior-December and current assets. The median reproduces NCUA's published median cost of funds.", "ratio"),
    "net_chargeoff_rate_ncua_ytd": ("Net charge-off rate on NCUA's basis: (charge-offs minus recoveries) annualized from year to date, over the average of prior-December and current loans. Pooled, it matches NCUA's published ratio within a basis point.", "ratio"),
    "roa_quarterly": ("Return on assets for the single quarter: the quarter's net income times 4, over the average of prior and current quarter-end assets. Derived here; NCUA does not publish quarterly-only ratios. No January step.", "ratio"),
    "nim_quarterly": ("Net interest margin (NIM) for the single quarter: (interest income minus expense) for the quarter times 4, over the average of prior and current quarter-end assets. Derived here; not an NCUA published ratio. No January step.", "ratio"),
    "loan_yield_quarterly": ("Yield on loans for the single quarter: the quarter's interest on loans times 4, over the average of prior and current quarter-end loans. Derived here; not an NCUA published ratio.", "ratio"),
    "cost_of_funds_quarterly": ("Cost of funds for the single quarter: the quarter's interest expense times 4, over the average of prior and current quarter-end assets. Derived here; not an NCUA published ratio.", "ratio"),
    "net_chargeoff_rate_quarterly": ("Net charge-off rate for the single quarter: the quarter's (charge-offs minus recoveries) times 4, over the average of prior and current quarter-end loans. Derived here; not an NCUA published ratio.", "ratio"),
    "interest_on_loans_quarter": ("Interest on loans for the single quarter (year-to-date minus prior quarter).", "dollars"),
    "net_chargeoffs_quarter": ("Charge-offs minus recoveries for the single quarter.", "dollars"),
    "provision_quarter": ("Provision for loan losses for the single quarter.", "dollars"),
}

DIM_DOCS = {
    "quarter": "Report quarter, YYYY-MM of the quarter end (03, 06, 09, 12).",
    "cu_number": "NCUA charter number. Stable for a credit union over time; disappears when it merges or closes.",
    "rssd": "Federal Reserve RSSD identifier.",
    "name": "Credit union name.", "city": "Mailing city.", "state": "Mailing state.",
    "charter_state": "State of charter (state-chartered only).", "zip_code": "Mailing ZIP.",
    "county_code": "County code.",
    "charter_type": "federal, state_federally_insured, or state_not_federally_insured.",
    "is_federally_insured": "False for the ~85 state-chartered credit unions NCUA does not insure. NCUA's published totals exclude them.",
    "peer_group": "NCUA asset peer group 1-6.", "peer_group_label": "Peer group asset range.",
    "is_minority_depository": "Minority depository institution flag.",
    "is_low_income": "Low-income designation.", "year_opened": "Year the credit union was organized.",
    "field_of_membership_code": "Type-of-membership code.", "ncua_region": "NCUA region code.",
}


def build_dictionary(con) -> None:
    rows = []
    for col, desc in DIM_DOCS.items():
        rows.append(("dim_credit_union", col, desc, "attribute", None, None, "FOICU.txt", None, None))
    span = {}
    for f in FIELDS:
        r = con.execute(
            f"SELECT min(quarter), max(quarter) FROM fact_call_report_curated WHERE {f.name} IS NOT NULL AND {f.name} <> 0"
        ).fetchone()
        span[f.name] = r
        rows.append(("fact_call_report_curated", f.name, f.description, f.kind, f.category,
                     ", ".join(c.replace("ACCT_", "Acct_") for c in f.codes), "FS220* tables", r[0], r[1]))
    for name, (desc, unit) in METRIC_DOCS.items():
        rows.append(("metrics", name, desc, unit, "derived", None, "computed from fact_call_report_curated", None, None))
    con.execute("""CREATE OR REPLACE TABLE dictionary(table_name VARCHAR, column_name VARCHAR, description VARCHAR,
        kind VARCHAR, category VARCHAR, ncua_account_codes VARCHAR, source VARCHAR, first_quarter_nonzero VARCHAR, last_quarter_nonzero VARCHAR)""")
    con.executemany("INSERT INTO dictionary VALUES (?,?,?,?,?,?,?,?,?)", rows)
