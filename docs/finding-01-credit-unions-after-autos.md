# Finding 01: credit unions are drifting away from auto loans

Federally insured credit unions, year-end balances (dollars from the `fact_call_report_curated` table, ratios from the same totals):

| | 2018 | 2021 | 2023 | 2025 | Jun 2026 |
|---|---|---|---|---|---|
| Total loans ($B) | 1,044 | 1,255 | 1,603 | 1,721 | 1,763 |
| Auto loans, new + used ($B) | 366.0 | 404.5 | 498.2 | 480.1 | 485.4 |
| Auto share of all loans | 35.1% | 32.2% | 31.1% | 27.9% | 27.5% |
| Junior-lien 1-4 family (HELOC, second mortgage) ($B) | 79.4 | 75.6 | 131.1 | 178.2 | 187.8 |
| Commercial loans ($B) | 71.2 | 111.7 | 157.2 | 192.9 | 201.7 |
| Delinquency rate (2+ months) | 0.71% | 0.49% | 0.83% | 1.03% | 0.96% |

What the numbers say:

- Auto loan balances peaked at $498B at the end of 2023 and have since slipped to $485B, while total loans grew about 10%. Auto's share of the loan book is down from 35% to 27.5%.
- Junior-lien real estate loans, mostly home equity, rose from $76B at the end of 2021 to $188B, about 2.5x.
- Commercial loans are 2.8x their 2018 level ($71B to $202B).
- Delinquency bottomed at 49 basis points at the end of 2021 and is 96 basis points in June 2026, above the 71 basis points of 2018. It peaked at 103 basis points in December 2025.

Checks done: all totals above reconcile to NCUA's published aggregates for June 2026 and December 2025 (see RECONCILIATION.md). The junior-lien series is continuous across NCUA's 2022 form change (Acct_386A: $76.5B Dec 2021, $79.0B Mar 2022).

Caveats: this is a system-wide aggregate, so it mixes very different institutions. A merger moves loans from one charter to another but not out of the system. "Commercial" combines member and non-member commercial loans as NCUA defines them.

Reproduce:

```sql
SELECT quarter,
       sum(loans_new_vehicle + loans_used_vehicle) / sum(loans_and_leases_total) AS auto_share
FROM fact_call_report_curated JOIN dim_credit_union USING (quarter, cu_number)
WHERE is_federally_insured AND quarter LIKE '%-12'
GROUP BY quarter ORDER BY quarter;
```
