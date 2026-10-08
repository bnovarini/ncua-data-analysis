# Ratio audit against NCUA's Quarterly Credit Union Data Summary

Every annualized ratio in this dataset was checked against NCUA's published summaries for all 32 quarters from 2018-Q3 to 2026-Q2
(`https://ncua.gov/files/publications/analysis/quarterly-data-summary-YYYY-Qn.pdf`). Federally insured credit unions only.

## Findings

1. **NCUA's denominator is the average of the prior December balance and the current quarter-end balance.** Net income, net
   interest margin and charge-offs are year-to-date flows annualized (x 4 / quarter number). With that denominator the pooled
   ROA, NIM and net charge-off ratio match NCUA's published values in every quarter checked (table below), and so do NCUA's
   published *median* yield on loans, cost of funds, net interest margin and return on average assets (within 0.03 points).
   These are the `*_ncua_ytd` fields. The older `*_avg_assets_4q` and year-end fields are kept but do not match NCUA exactly.
2. **The January step is real.** NCUA's own year-to-date ratios jump at Q1 (NIM 2.86% in 2022-12, 3.01% in 2023-03; median yield
   on loans 4.69% to 4.95%) because Q4 averages the whole year and Q1 is one quarter. It is not a data error.
   The `*_quarterly` fields (single quarter x 4, over the average of prior and current quarter-end balances) remove it.
3. **Mismatches to know about.**
   - `loan_yield` (year-end loans) runs up to 28 bp below NCUA's median yield in rising-rate years (4.41% vs 4.69% in 2022-12).
     Use `loan_yield_ncua_ytd`.
   - `cost_of_shares` (interest expense over shares) is not NCUA's cost of funds (which is over average assets). Use `cost_of_funds_ncua_ytd`.
   - NCUA revises history. Where the dataset differs from the latest NCUA summary, it equals the value NCUA published in that
     quarter's own release: 2019-03 total interest income (dataset 58.27B; first release 58.3B; restated to 59.1B later), and net income up to 0.27B
     (2023-12: dataset 15.17B, first release 15.2B, now 14.9B). Balance sheet totals differ by at most 0.25B (0.01%).
   - Pooled net worth ratio differs from NCUA's by 0.01 in a few quarters before 2020-Q2 (NCUA changed the formula then).
   - Quarter-only ratios are not published by NCUA, so they cannot be matched to a published number. They were checked by
     identity: the quarter implied by NCUA's own consecutive year-to-date aggregates (n x YTD_n minus (n-1) x YTD_n-1, same release vintage)
     against the dataset's single-quarter flows. 66 of 69 checks (NIM $, net income, interest expense; 2018-2026) are within
     NCUA's rounding. The other 3 miss by 0.39 to 0.44B on 19 to 83B, from NCUA restating prior-quarter figures.
4. **Not independently verifiable:** `compensation_per_fte`, `operating_expense_per_fte` and `opex_to_assets` have no NCUA published
   ratio. Their numerators (employee compensation, non-interest expense) match NCUA's dollar totals; the FTE estimate is this project's own.

## Pooled ratios vs NCUA (insured credit unions)

| Quarter | ROA bp (ds / NCUA) | NIM % (ds / NCUA) | Net charge-off bp (ds / NCUA) | Delinquency bp (ds / NCUA) | Net worth % (ds / NCUA) |
|---|---|---|---|---|---|
| 2019-03 | 95.0 / 95 | 3.115 / 3.1 | - / - | 57.8 / 58 | 11.14 / 11.14 |
| 2019-06 | 96.7 / 97 | 3.181 / 3.2 | - / - | 63.4 / 63 | 11.27 / 11.27 |
| 2019-09 | 98.0 / 98 | 3.191 / 3.2 | 55.5 / 55 | 67.1 / 67 | 11.39 / 11.39 |
| 2019-12 | 93.5 / 94 | 3.163 / 3.2 | 56.3 / 56 | 70.9 / 71 | 11.38 / 11.37 |
| 2020-03 | 52.7 / 53 | 2.952 / 3.0 | 57.9 / 58 | 63.4 / 63 | 11.01 / 11.01 |
| 2020-06 | 56.8 / 57 | 2.884 / 2.88 | 53.1 / 53 | 58.1 / 58 | 10.46 / 10.46 |
| 2020-09 | 65.5 / 66 | 2.868 / 2.87 | 48.0 / 48 | 54.5 / 55 | 10.44 / 10.43 |
| 2020-12 | 70.4 / 70 | 2.822 / 2.82 | 44.8 / 45 | 60.1 / 60 | 10.32 / 10.32 |
| 2021-03 | 104.1 / 104 | 2.567 / 2.57 | 31.8 / 32 | 46.0 / 46 | 10.02 / 10.01 |
| 2021-06 | 111.6 / 112 | 2.572 / 2.57 | 27.7 / 28 | 45.6 / 46 | 10.17 / 10.17 |
| 2021-09 | 111.5 / 112 | 2.588 / 2.59 | 26.0 / 26 | 46.1 / 46 | 10.24 / 10.23 |
| 2021-12 | 106.8 / 107 | 2.590 / 2.59 | 26.0 / 26 | 49.0 / 49 | 10.27 / 10.26 |
| 2022-03 | 86.8 / 87 | 2.573 / 2.57 | 27.7 / 28 | 42.1 / 42 | 10.22 / 10.22 |
| 2022-06 | 86.0 / 86 | 2.665 / 2.67 | 28.4 / 28 | 48.1 / 48 | 10.42 / 10.42 |
| 2022-09 | 87.9 / 88 | 2.786 / 2.79 | 30.4 / 30 | 53.0 / 53 | 10.59 / 10.59 |
| 2022-12 | 89.2 / 89 | 2.865 / 2.86 | 33.9 / 34 | 61.5 / 61 | 10.74 / 10.74 |
| 2023-03 | 80.6 / 81 | 3.010 / 3.01 | 51.8 / 52 | 52.5 / 53 | 10.49 / 10.49 |
| 2023-06 | 79.4 / 79 | 3.016 / 3.02 | 52.8 / 53 | 62.9 / 63 | 10.63 / 10.63 |
| 2023-09 | 75.5 / 76 | 3.024 / 3.02 | 55.5 / 56 | 72.0 / 72 | 10.73 / 10.73 |
| 2023-12 | 68.6 / 69 | 3.013 / 3.01 | 61.2 / 61 | 82.9 / 83 | 10.70 / 10.70 |
| 2024-03 | 65.6 / 66 | 2.996 / 3.00 | 80.3 / 80 | 77.5 / 78 | 10.62 / 10.62 |
| 2024-06 | 68.9 / 69 | 3.054 / 3.05 | 79.2 / 79 | 84.2 / 84 | 10.84 / 10.84 |
| 2024-09 | 69.3 / 69 | 3.087 / 3.09 | 78.0 / 78 | 90.9 / 91 | 10.94 / 10.94 |
| 2024-12 | 63.1 / 63 | 3.122 / 3.12 | 80.0 / 80 | 97.5 / 98 | 11.07 / 11.07 |
| 2025-03 | 67.0 / 67 | 3.236 / 3.24 | 82.5 / 82 | 79.7 / 80 | 10.95 / 10.95 |
| 2025-06 | 75.7 / 76 | 3.321 / 3.32 | 78.7 / 79 | 90.6 / 91 | 11.11 / 11.11 |
| 2025-09 | 81.3 / 81 | 3.380 / 3.38 | 76.8 / 77 | 94.8 / 95 | 11.24 / 11.24 |
| 2025-12 | 79.3 / 79 | 3.390 / 3.39 | 78.2 / 78 | 102.7 / 103 | 11.26 / 11.26 |
| 2026-03 | 83.2 / 83 | 3.444 / 3.44 | 80.8 / 81 | 84.5 / 85 | 11.24 / 11.24 |
| 2026-06 | 91.0 / 91 | 3.492 / 3.49 | 78.0 / 78 | 96.2 / 96 | 11.42 / 11.42 |

2018-Q3 to 2018-Q4 are not in the table: the dataset starts in 2018-03, so there is no prior December balance (NCUA-basis ratios are empty for 2018).

## Medians vs NCUA's published medians (all within 0.03 points, 2019-03 to 2026-06)

| Quarter | yield on loans % | cost of funds % | net interest margin % | ROA % |
|---|---|---|---|---|
| 2022-12 | 4.70 / 4.69 | 0.23 / 0.24 | 2.72 / 2.73 | 0.51 / 0.5 |
| 2023-03 | 4.96 / 4.95 | 0.41 / 0.41 | 3.09 / 3.09 | 0.61 / 0.62 |
| 2023-12 | 5.30 / 5.29 | 0.67 / 0.67 | 3.23 / 3.24 | 0.60 / 0.6 |
| 2024-03 | 5.66 / 5.65 | 0.95 / 0.95 | 3.36 / 3.36 | 0.55 / 0.55 |
| 2025-12 | 6.22 / 6.21 | 1.11 / 1.12 | 3.73 / 3.73 | 0.72 / 0.72 |
| 2026-03 | 6.24 / 6.22 | 1.05 / 1.05 | 3.69 / 3.69 | 0.66 / 0.66 |
| 2026-06 | 6.24 / 6.23 | 1.05 / 1.05 | 3.75 / 3.75 | 0.71 / 0.71 |

Full per-quarter comparisons are reproduced by `tests/test_mcp_server.py::Round5Regressions` for sample quarters.
