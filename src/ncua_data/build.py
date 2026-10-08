"""Build the curated tables (dim, fact, metrics, dictionary) from long Parquet."""
from __future__ import annotations

from pathlib import Path

from .extras import extras_sql

import duckdb

from .spec import FIELDS


def fact_sql(long_glob: str) -> str:
    cols = []
    for f in FIELDS:
        codes = ", ".join(f"'{c}'" for c in f.codes)
        agg = f"sum(l.value) FILTER (WHERE l.acct IN ({codes}))"
        # No row in the long table means the credit union reported zero (or left it blank).
        expr = f"coalesce({agg}, 0)"
        if f.first_quarter:
            expr = f"CASE WHEN q.quarter >= '{f.first_quarter}' THEN {expr} END"
        cols.append(f"{expr} AS {f.name}")
    return f"""
        SELECT q.quarter, q.cu_number, {', '.join(cols)}
        FROM quarter_cus q
        LEFT JOIN (SELECT * FROM read_parquet('{long_glob}')) l
          ON l.quarter = q.quarter AND l.cu_number = q.cu_number
        GROUP BY q.quarter, q.cu_number"""


def build(data_dir: Path, out_dir: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET memory_limit='1400MB'; SET threads=2")
    long_glob = str(data_dir / "long" / "*_long.parquet")
    foicu_glob = str(data_dir / "long" / "*_foicu.parquet")
    con.execute(
        f"""CREATE TABLE quarter_cus AS
            SELECT quarter, CU_NUMBER::INTEGER AS cu_number
            FROM read_parquet('{foicu_glob}', union_by_name=true)"""
    )
    # One quarter at a time keeps memory low enough for a 2 GB machine.
    first = True
    for lf in sorted((data_dir / "long").glob("*_long.parquet")):
        q = lf.name.split("_")[0]
        sql = fact_sql(str(lf)).replace("FROM quarter_cus q", f"FROM (SELECT * FROM quarter_cus WHERE quarter = '{q}') q")
        if first:
            con.execute(f"CREATE TABLE fact_call_report_curated AS {sql}")
            first = False
        else:
            con.execute(f"INSERT INTO fact_call_report_curated {sql}")
    return con


DIM_SQL = """
CREATE TABLE dim_credit_union AS
SELECT quarter,
       CU_NUMBER::INTEGER AS cu_number,
       try_cast(RSSD AS BIGINT) AS rssd,
       trim(CU_NAME) AS name,
       trim(CITY) AS city,
       trim(STATE) AS state,
       trim(CharterState) AS charter_state,
       trim(ZIP_CODE) AS zip_code,
       try_cast(COUNTY_CODE AS INTEGER) AS county_code,
       CASE CU_TYPE WHEN '1' THEN 'federal' WHEN '2' THEN 'state_federally_insured'
                    WHEN '3' THEN 'state_not_federally_insured' END AS charter_type,
       CU_TYPE <> '3' AS is_federally_insured,
       try_cast(Peer_Group AS INTEGER) AS peer_group,
       CASE try_cast(Peer_Group AS INTEGER)
            WHEN 1 THEN 'under $2M' WHEN 2 THEN '$2M-$10M' WHEN 3 THEN '$10M-$50M'
            WHEN 4 THEN '$50M-$100M' WHEN 5 THEN '$100M-$500M' WHEN 6 THEN '$500M+' END AS peer_group_label,
       lower(IsMDI) IN ('true', '1') AS is_minority_depository,
       try_cast(LIMITED_INC AS INTEGER) = 1 AS is_low_income,
       try_cast(YEAR_OPENED AS INTEGER) AS year_opened,
       trim(TOM_CODE) AS field_of_membership_code,
       trim(REGION) AS ncua_region
FROM read_parquet('{foicu}', union_by_name=true)
"""

# Metrics. Flow fields are year-to-date, so annualise by 4 / quarter number.
METRICS_SQL = """
CREATE TABLE metrics AS
WITH f AS (
  SELECT f.*, d.is_federally_insured,
         CAST(substr(f.quarter, 6, 2) AS INTEGER) / 3 AS qn,
         CAST(substr(f.quarter, 1, 4) AS INTEGER) AS yr
  FROM fact_call_report_curated f JOIN dim_credit_union d USING (quarter, cu_number)
), g AS (
  SELECT f.*, 4.0 / qn AS ann,
         lag(total_assets, 4) OVER w AS assets_1y_ago,
         lag(loans_and_leases_total, 4) OVER w AS loans_1y_ago,
         lag(total_shares_and_deposits, 4) OVER w AS shares_1y_ago,
         lag(members, 4) OVER w AS members_1y_ago,
         lag(loans_new_vehicle + loans_used_vehicle, 4) OVER w AS auto_1y_ago,
         lag(loans_first_lien_residential, 4) OVER w AS first_lien_1y_ago,
         CASE WHEN count(*) OVER w4 = 4 THEN avg(total_assets) OVER w4 END AS avg_assets_4q,
         CASE WHEN count(*) OVER w4 = 4 THEN avg(loans_and_leases_total) OVER w4 END AS avg_loans_4q,
         lag(net_income_ytd, 1) OVER w AS ni_prev, lag(interest_income_ytd, 1) OVER w AS ii_prev,
         lag(interest_expense_ytd, 1) OVER w AS ie_prev, lag(non_interest_expense_ytd, 1) OVER w AS nie_prev,
         lag(provision_for_loan_losses_ytd, 1) OVER w AS prov_prev
  FROM f WINDOW w AS (PARTITION BY cu_number ORDER BY quarter),
  w4 AS (PARTITION BY cu_number ORDER BY quarter ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
)
SELECT quarter, cu_number,
  -- size and growth
  CASE WHEN assets_1y_ago > 0 THEN total_assets / assets_1y_ago - 1 END AS asset_growth_yoy,
  CASE WHEN loans_1y_ago > 0 THEN loans_and_leases_total / loans_1y_ago - 1 END AS loan_growth_yoy,
  CASE WHEN shares_1y_ago > 0 THEN total_shares_and_deposits / shares_1y_ago - 1 END AS share_growth_yoy,
  CASE WHEN members_1y_ago > 0 THEN members / members_1y_ago - 1 END AS member_growth_yoy,
  CASE WHEN auto_1y_ago > 0 THEN (loans_new_vehicle + loans_used_vehicle) / auto_1y_ago - 1 END AS auto_loan_growth_yoy,
  CASE WHEN first_lien_1y_ago > 0 THEN loans_first_lien_residential / first_lien_1y_ago - 1 END AS first_lien_growth_yoy,
  -- balance sheet ratios
  loans_and_leases_total / nullif(total_shares_and_deposits, 0) AS loan_to_share,
  loans_and_leases_total / nullif(total_assets, 0) AS loans_to_assets,
  net_worth / nullif(total_assets, 0) AS net_worth_to_assets,
  (net_worth - coalesce(cecl_transition_provision, 0)) / nullif(total_assets, 0) AS net_worth_ratio_ex_cecl,
  allowance_for_credit_losses / nullif(loans_and_leases_total, 0) AS allowance_to_loans,
  loans_and_leases_total / nullif(members, 0) AS loans_per_member,
  total_shares_and_deposits / nullif(members, 0) AS shares_per_member,
  -- loan mix (share of total loans)
  (loans_new_vehicle + loans_used_vehicle) / nullif(loans_and_leases_total, 0) AS mix_auto,
  (loans_first_lien_residential + loans_junior_lien_residential + loans_other_real_estate) / nullif(loans_and_leases_total, 0) AS mix_residential_real_estate,
  loans_credit_card / nullif(loans_and_leases_total, 0) AS mix_credit_card,
  loans_commercial_total / nullif(loans_and_leases_total, 0) AS mix_commercial,
  -- share and deposit mix (share of total shares and deposits)
  shares_share_drafts / nullif(total_shares_and_deposits, 0) AS deposit_mix_share_drafts,
  shares_regular / nullif(total_shares_and_deposits, 0) AS deposit_mix_regular,
  shares_money_market / nullif(total_shares_and_deposits, 0) AS deposit_mix_money_market,
  shares_certificates / nullif(total_shares_and_deposits, 0) AS deposit_mix_certificates,
  shares_ira_keogh / nullif(total_shares_and_deposits, 0) AS deposit_mix_ira_keogh,
  deposits_non_member / nullif(total_shares_and_deposits, 0) AS deposit_mix_non_member,
  total_shares_and_deposits / nullif(accounts_share_drafts + accounts_certificates + accounts_money_market + accounts_ira_keogh + accounts_non_member, 0) AS avg_balance_per_listed_account,
  -- staffing and branches
  employees_full_time + employees_part_time / 2.0 AS employees_fte_estimate,
  members / nullif(employees_full_time + employees_part_time / 2.0, 0) AS members_per_fte,
  total_assets / nullif(employees_full_time + employees_part_time / 2.0, 0) AS assets_per_fte,
  employee_compensation_ytd * ann / nullif(employees_full_time + employees_part_time / 2.0, 0) AS compensation_per_fte,
  non_interest_expense_ytd * ann / nullif(employees_full_time + employees_part_time / 2.0, 0) AS operating_expense_per_fte,
  total_assets / nullif(branches, 0) AS assets_per_branch,
  members / nullif(branches, 0) AS members_per_branch,
  employee_compensation_ytd / nullif(non_interest_expense_ytd, 0) AS compensation_share_of_opex,
  -- asset quality
  delinquent_2m_plus / nullif(loans_and_leases_total, 0) AS delinquency_rate,
  (delinquent_new_vehicle + delinquent_used_vehicle) / nullif(loans_new_vehicle + loans_used_vehicle, 0) AS auto_delinquency_rate,
  delinquent_credit_card / nullif(loans_credit_card, 0) AS credit_card_delinquency_rate,
  (chargeoffs_ytd - recoveries_ytd) * ann / nullif(loans_and_leases_total, 0) AS net_chargeoff_rate,
  (chargeoffs_ytd - recoveries_ytd) * ann / nullif(avg_loans_4q, 0) AS net_chargeoff_rate_avg_loans_4q,
  provision_for_loan_losses_ytd * ann / nullif(loans_and_leases_total, 0) AS provision_to_loans,
  -- earnings (annualised from year-to-date)
  net_income_ytd * ann / nullif(total_assets, 0) AS roa_year_end_assets,
  net_income_ytd * ann / nullif(avg_assets_4q, 0) AS roa_avg_assets_4q,
  (interest_income_ytd - interest_expense_ytd) * ann / nullif(total_assets, 0) AS nim_year_end_assets,
  (interest_income_ytd - interest_expense_ytd) * ann / nullif(avg_assets_4q, 0) AS nim_avg_assets_4q,
  interest_on_loans_ytd * ann / nullif(loans_and_leases_total, 0) AS loan_yield,
  interest_expense_ytd * ann / nullif(total_shares_and_deposits, 0) AS cost_of_shares,
  non_interest_expense_ytd / nullif((interest_income_ytd - interest_expense_ytd) + non_interest_income_ytd, 0) AS efficiency_ratio,
  non_interest_expense_ytd * ann / nullif(total_assets, 0) AS opex_to_assets,
  fee_income_ytd / nullif(non_interest_income_ytd, 0) AS fee_share_of_non_interest_income,
  -- de-cumulated quarterly flows
  CASE WHEN qn = 1 THEN net_income_ytd ELSE net_income_ytd - ni_prev END AS net_income_quarter,
  CASE WHEN qn = 1 THEN interest_income_ytd ELSE interest_income_ytd - ii_prev END AS interest_income_quarter,
  CASE WHEN qn = 1 THEN interest_expense_ytd ELSE interest_expense_ytd - ie_prev END AS interest_expense_quarter,
  CASE WHEN qn = 1 THEN non_interest_expense_ytd ELSE non_interest_expense_ytd - nie_prev END AS non_interest_expense_quarter,
  CASE WHEN qn = 1 THEN provision_for_loan_losses_ytd ELSE provision_for_loan_losses_ytd - prov_prev END AS provision_quarter
FROM g
"""


def build_all(data_dir: Path) -> duckdb.DuckDBPyConnection:
    con = build(data_dir, data_dir)
    foicu = str(data_dir / "long" / "*_foicu.parquet")
    con.execute(DIM_SQL.format(foicu=foicu))
    con.execute(METRICS_SQL)
    con.execute("CREATE TABLE metrics_x AS " + extras_sql("fact_call_report_curated"))
    con.execute("CREATE TABLE metrics2 AS SELECT m.*, x.* EXCLUDE (quarter, cu_number) FROM metrics m "
                "LEFT JOIN metrics_x x USING (quarter, cu_number)")
    con.execute("DROP TABLE metrics")
    con.execute("ALTER TABLE metrics2 RENAME TO metrics")
    con.execute("DROP TABLE metrics_x")
    return con
