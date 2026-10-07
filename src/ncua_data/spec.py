"""The curated field list.

Each field maps one clean snake_case name to one or more NCUA account codes.
When several codes are listed, they are *eras of the same concept* (for example
the pre-CECL allowance Acct_719 and the CECL allowance Acct_AS0048); a credit
union reports in only one of them in a given quarter, so the values are summed.

kind:
  stock  point-in-time balance at quarter end
  ytd    year-to-date flow (resets each January; Q4 is the full year)
  count  a count of loans, members or accounts
  pct    a ratio reported by the credit union itself
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Field:
    name: str
    category: str
    kind: str
    codes: tuple[str, ...]
    description: str
    first_quarter: str | None = None  # None = available from the start of the dataset


def F(name, category, kind, codes, description, first=None):
    codes = (codes,) if isinstance(codes, str) else tuple(codes)
    return Field(name, category, kind, tuple("ACCT_" + c for c in codes), description, first)


FIELDS: list[Field] = [
    # ---- Balance sheet -------------------------------------------------
    F("total_assets", "balance_sheet", "stock", "010", "Total assets, in dollars."),
    F("cash_on_hand", "balance_sheet", "stock", "730A", "Coin and currency held."),
    F("cash_on_deposit", "balance_sheet", "stock", "730B", "Cash deposited at other financial institutions, including corporate credit unions and the Federal Reserve."),
    F("loans_held_for_sale", "balance_sheet", "stock", "003", "Loans the credit union intends to sell."),
    F("land_and_building", "balance_sheet", "stock", "007", "Land and buildings, net."),
    F("other_fixed_assets", "balance_sheet", "stock", "008", "Other fixed assets, net."),
    F("accrued_interest_on_loans", "balance_sheet", "stock", "009A", "Interest earned on loans but not yet received."),
    F("goodwill", "balance_sheet", "stock", "009D2", "Goodwill from acquisitions."),
    F("ncua_insurance_deposit", "balance_sheet", "stock", "794", "Capitalization deposit held at the NCUA share insurance fund."),
    F("total_liabilities_shares_equity", "balance_sheet", "stock", "014", "Total liabilities, shares and equity. Equals total assets."),
    F("allowance_for_credit_losses", "balance_sheet", "stock", ("719", "AS0048"), "Allowance for loan and lease losses. Reported as Acct_719 before CECL and as Acct_AS0048 after."),
    F("borrowings_repurchase", "balance_sheet", "stock", "058C", "Borrowings through repurchase transactions."),
    # ---- Shares and capital --------------------------------------------
    F("total_shares", "shares_capital", "stock", "013", "Total member shares."),
    F("total_shares_and_deposits", "shares_capital", "stock", "018", "Total shares and deposits, including non-member deposits."),
    F("shares_under_1yr", "shares_capital", "stock", "013A", "Shares maturing in under one year, including share drafts and regular shares."),
    F("shares_1_to_3yr", "shares_capital", "stock", "013B1", "Shares maturing in one to three years."),
    F("shares_over_3yr", "shares_capital", "stock", "013B2", "Shares maturing in more than three years."),
    F("commercial_share_accounts", "shares_capital", "stock", "643", "Share accounts held by business members."),
    F("brokered_certificates", "shares_capital", "stock", "788", "Brokered certificates of deposit."),
    F("uninsured_shares", "shares_capital", "stock", "068A", "Shares and deposits above the insurance limit."),
    F("insured_shares", "shares_capital", "stock", "069A", "Shares and deposits covered by NCUA insurance."),
    F("net_worth", "shares_capital", "stock", "997", "Total net worth (regulatory capital)."),
    F("cecl_transition_provision", "shares_capital", "stock", "NW0004", "CECL transition provision included in net worth. NCUA's published net worth ratio excludes it. Available from 2023.", "2023-03"),
    F("net_worth_ratio_pct_x100", "shares_capital", "pct", "998", "Net worth ratio as reported, in hundredths of a percent (1455 = 14.55%)."),
    F("other_reserves", "shares_capital", "stock", "658", "Other reserves appropriated from undivided earnings."),
    F("equity_acquired_in_merger", "shares_capital", "stock", "658A", "Equity acquired through mergers."),
    # ---- Share and deposit account composition --------------------------
    F("shares_share_drafts", "account_composition", "stock", "902", "Share draft (checking) accounts."),
    F("shares_regular", "account_composition", "stock", "657", "Regular share (savings) accounts."),
    F("shares_money_market", "account_composition", "stock", "911", "Money market shares."),
    F("shares_certificates", "account_composition", "stock", "908C", "Share certificates, all maturities."),
    F("shares_certificates_under_1yr", "account_composition", "stock", "908A", "Share certificates maturing in under one year."),
    F("shares_certificates_1_to_3yr", "account_composition", "stock", "908B1", "Share certificates maturing in one to three years."),
    F("shares_certificates_over_3yr", "account_composition", "stock", "908B2", "Share certificates maturing in more than three years."),
    F("shares_ira_keogh", "account_composition", "stock", "906C", "IRA and Keogh retirement accounts."),
    F("shares_other", "account_composition", "stock", "630", "All other share accounts."),
    F("deposits_non_member", "account_composition", "stock", "880", "Deposits from non-members (for example government and other credit unions)."),
    F("accounts_share_drafts", "account_composition", "count", "452", "Number of share draft accounts."),
    F("accounts_certificates", "account_composition", "count", "451", "Number of share certificate accounts."),
    F("accounts_money_market", "account_composition", "count", "458", "Number of money market accounts."),
    F("accounts_ira_keogh", "account_composition", "count", "453", "Number of IRA and Keogh accounts."),
    F("accounts_non_member", "account_composition", "count", "457", "Number of non-member deposit accounts."),
    # ---- Staffing and branches ------------------------------------------
    F("employees_full_time", "operations", "count", "564A", "Full-time employees (26 or more hours a week)."),
    F("employees_part_time", "operations", "count", "564B", "Part-time employees (25 hours or less a week)."),
    F("branches", "operations", "count", "566", "Number of branches, counting the main office when it serves members."),
    # ---- Membership ----------------------------------------------------
    F("members", "membership", "count", "083", "Current members (people, not accounts)."),
    F("potential_members", "membership", "count", "084", "Potential members within the field of membership."),
    # ---- Loan portfolio (balances) -------------------------------------
    F("loans_and_leases_total", "loan_portfolio", "stock", "025B", "Total loans and leases outstanding."),
    F("loans_and_leases_count", "loan_portfolio", "count", "025A", "Number of loans and leases outstanding."),
    F("leases_receivable", "loan_portfolio", "stock", "002", "Leases receivable."),
    F("loans_new_vehicle", "loan_portfolio", "stock", "385", "New vehicle loans."),
    F("loans_used_vehicle", "loan_portfolio", "stock", "370", "Used vehicle loans."),
    F("loans_credit_card", "loan_portfolio", "stock", "396", "Unsecured credit card loans."),
    F("loans_other_unsecured", "loan_portfolio", "stock", "397", "All other unsecured loans and lines of credit."),
    F("loans_payday_alternative", "loan_portfolio", "stock", "397A", "Payday alternative loans (PAL), federal credit unions only."),
    F("loans_student", "loan_portfolio", "stock", "698A", "Non-federally guaranteed student loans."),
    F("loans_other_secured_non_re", "loan_portfolio", "stock", "698C", "All other secured loans that are not real estate."),
    F("loans_first_lien_residential", "loan_portfolio", "stock", "703A", "Loans and lines secured by a first lien on 1-4 family residential property."),
    F("loans_junior_lien_residential", "loan_portfolio", "stock", "386A", "Loans and lines secured by a junior lien on 1-4 family residential property (mostly HELOCs)."),
    F("loans_other_real_estate", "loan_portfolio", "stock", "386B", "Other real estate loans and lines of credit."),
    F("loans_commercial_total", "loan_portfolio", "stock", "400T1", "All member and non-member commercial loans."),
    F("loans_commercial_member", "loan_portfolio", "stock", "400A1", "Commercial loans to members."),
    F("loans_commercial_nonmember", "loan_portfolio", "stock", "400B1", "Commercial loans purchased or participated from non-members."),
    F("loans_commercial_not_re", "loan_portfolio", "stock", "400P", "Commercial loans not secured by real estate."),
    F("loans_commercial_real_estate", "loan_portfolio", "stock", "718A5", "Commercial loans and lines that are real estate secured."),
    F("loans_member_business_net", "loan_portfolio", "stock", "400A", "Net member business loan balance (regulatory definition)."),
    F("loans_construction_member", "loan_portfolio", "stock", "143B3", "Member construction and development loans."),
    F("loans_agricultural_commercial", "loan_portfolio", "stock", "042A9", "Agricultural commercial loans."),
    F("loans_indirect_total", "loan_portfolio", "stock", "618A", "Indirect loans (originated through dealers or third parties)."),
    F("loans_interest_only_first_mortgage", "loan_portfolio", "stock", "704C1", "Interest-only and payment-option first mortgages."),
    F("loans_troubled_debt_restructured", "loan_portfolio", "stock", "1001F", "Loans classed as troubled debt restructurings."),
    F("real_estate_loans_sold_serviced", "loan_portfolio", "stock", "779A", "Real estate loans sold but still serviced by the credit union."),
    # ---- Originations and commitments ----------------------------------
    F("loans_granted_ytd", "originations", "ytd", "031B", "Dollar amount of loans granted year to date."),
    F("loans_granted_count_ytd", "originations", "ytd", "031A", "Number of loans granted year to date."),
    F("participations_purchased_ytd", "originations", "ytd", "690", "Participation loans purchased year to date."),
    F("participations_sold_ytd", "originations", "ytd", "691", "Participation loans sold year to date."),
    F("unfunded_credit_card_lines", "originations", "stock", "812C", "Unfunded credit card commitments."),
    F("unfunded_revolving_real_estate", "originations", "stock", "811D", "Unfunded revolving open-end real estate commitments."),
    F("unfunded_commercial", "originations", "stock", "814K", "Unfunded commercial loan commitments."),
    # ---- Delinquency ---------------------------------------------------
    F("delinquent_1_to_2m", "delinquency", "stock", "020B", "All loans and leases delinquent 1 to under 2 months."),
    F("delinquent_2_to_6m", "delinquency", "stock", "021B", "All loans and leases delinquent 2 to under 6 months."),
    F("delinquent_6_to_12m", "delinquency", "stock", "022B", "All loans and leases delinquent 6 to under 12 months."),
    F("delinquent_12m_plus", "delinquency", "stock", "023B", "All loans and leases delinquent 12 months or more."),
    F("delinquent_2m_plus", "delinquency", "stock", "041B", "Total delinquent loans and leases, two or more months. The headline NCUA delinquency figure."),
    F("delinquent_new_vehicle", "delinquency", "stock", "041C1", "Reportable delinquent new vehicle loans."),
    F("delinquent_used_vehicle", "delinquency", "stock", "041C2", "Reportable delinquent used vehicle loans."),
    F("delinquent_credit_card", "delinquency", "stock", "045B", "Delinquent unsecured credit card loans."),
    F("delinquent_indirect", "delinquency", "stock", "041E", "Delinquent indirect loans."),
    F("delinquent_student", "delinquency", "stock", "041T", "Delinquent student loans."),
    F("delinquent_first_lien_residential", "delinquency", "stock", "DL0062", "Reportable delinquent first-lien 1-4 family loans. Available from 2022 only.", "2022-03"),
    F("delinquent_junior_lien_residential", "delinquency", "stock", "DL0069", "Reportable delinquent junior-lien 1-4 family loans. Available from 2022 only.", "2022-03"),
    F("consumer_loans_nonaccrual", "delinquency", "stock", "DL0145", "Consumer loans in non-accrual status. Available from 2022 only.", "2022-03"),
    # ---- Charge-offs and credit losses ---------------------------------
    F("chargeoffs_ytd", "credit_losses", "ytd", "550", "Total loans charged off year to date."),
    F("recoveries_ytd", "credit_losses", "ytd", "551", "Total recoveries on charged-off loans year to date."),
    F("chargeoffs_credit_card_ytd", "credit_losses", "ytd", "680", "Credit card charge-offs year to date."),
    F("chargeoffs_used_vehicle_ytd", "credit_losses", "ytd", "550C2", "Used vehicle charge-offs year to date."),
    F("chargeoffs_indirect_ytd", "credit_losses", "ytd", "550E", "Indirect loan charge-offs year to date."),
    F("chargeoffs_other_unsecured_ytd", "credit_losses", "ytd", "CH0007", "Other unsecured charge-offs year to date. Available from 2022 only.", "2022-03"),
    F("provision_for_loan_losses_ytd", "credit_losses", "ytd", ("300", "IS0011"), "Provision (pre-CECL) or credit loss expense (CECL) on loans, year to date."),
    # ---- Income statement (year to date) -------------------------------
    F("interest_income_ytd", "income_statement", "ytd", "115", "Total interest income, year to date."),
    F("interest_on_loans_ytd", "income_statement", "ytd", "110", "Interest on loans before refunds, year to date."),
    F("interest_refunded_ytd", "income_statement", "ytd", "119", "Interest refunded to borrowers, year to date."),
    F("investment_income_ytd", "income_statement", "ytd", "120", "Income from investments, year to date."),
    F("fee_income_ytd", "income_statement", "ytd", "131", "Fee income, year to date."),
    F("non_interest_income_ytd", "income_statement", "ytd", "117", "Total non-interest income, year to date."),
    F("interest_expense_ytd", "income_statement", "ytd", "350", "Total interest expense (dividends, deposit interest and borrowing interest), year to date."),
    F("dividends_on_shares_ytd", "income_statement", "ytd", "380", "Dividends paid on shares, year to date."),
    F("interest_on_deposits_ytd", "income_statement", "ytd", "381", "Interest on deposits, year to date."),
    F("interest_on_borrowings_ytd", "income_statement", "ytd", "340", "Interest on borrowed money, year to date."),
    F("employee_compensation_ytd", "income_statement", "ytd", "210", "Employee compensation and benefits, year to date."),
    F("occupancy_expense_ytd", "income_statement", "ytd", "250", "Office occupancy expense, year to date."),
    F("operations_expense_ytd", "income_statement", "ytd", "260", "Office operations expense, year to date."),
    F("loan_servicing_expense_ytd", "income_statement", "ytd", "280", "Loan servicing expense, year to date."),
    F("professional_services_expense_ytd", "income_statement", "ytd", "290", "Professional and outside services expense, year to date."),
    F("non_interest_expense_ytd", "income_statement", "ytd", "671", "Total non-interest expense, year to date."),
    F("net_income_ytd", "income_statement", "ytd", "661A", "Net income, year to date."),
]

FIELD_BY_NAME = {f.name: f for f in FIELDS}
assert len(FIELD_BY_NAME) == len(FIELDS), "duplicate field name"
