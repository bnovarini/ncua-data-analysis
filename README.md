# ncua-data-analysis

Clean, documented, quarterly credit union data from NCUA call reports, 2018 to today: lending, deposit mix, earnings, staffing and efficiency.

NCUA publishes every federally insured credit union's quarterly 5300 call report as free bulk files. They are hard to use: 3,300+ account columns split across 17 wide tables, form changes that move accounts around, year-to-date income, and a data dictionary written as form instructions. This project turns that into four tidy tables you can query with DuckDB, pandas or anything that reads Parquet.

Status: v0. 115 curated fields and 52 computed metrics, 34 quarters (2018 Q1 to 2026 Q2), checked against NCUA's own published totals. See [what is not done yet](#not-done-yet).

## Tables

| Table | Grain | What it is |
|---|---|---|
| `dim_credit_union` | credit union x quarter | Name, location, charter type, peer group, low-income and MDI flags, and `is_federally_insured`. |
| `fact_call_report_curated` | credit union x quarter | 115 curated account values with plain names: balance sheet, shares and capital, deposit composition (share drafts, regular, money market, certificates, IRA, non-member), loan balances by type, originations, delinquency, charge-offs, income statement, employees and branches. |
| `metrics` | credit union x quarter | 52 computed metrics: delinquency, loan-to-share, ROA, NIM, loan and deposit mix, growth, efficiency ratio, members and assets per employee, per-branch figures, de-cumulated quarterly income. |
| `dictionary` | column | Description, unit, NCUA account codes, and first and last quarter each field has data. |

Join on `quarter` + `cu_number`. Dollar fields are dollars. Fields ending in `_ytd` are year to date and reset each January (the Q4 value is the full year); `metrics` annualizes them.

```sql
-- Which large credit unions grew auto lending fastest last year?
SELECT d.name, d.state, m.auto_loan_growth_yoy, f.loans_new_vehicle + f.loans_used_vehicle AS auto_loans
FROM fact_call_report_curated f
JOIN dim_credit_union d USING (quarter, cu_number)
JOIN metrics m USING (quarter, cu_number)
WHERE f.quarter = '2026-06' AND d.is_federally_insured AND d.peer_group = 6
ORDER BY m.auto_loan_growth_yoy DESC LIMIT 10;
```

## Run it

```bash
pip install -e .
ncua-data all          # download 34 quarters (~270 MB), build tables, reconcile
python -m unittest discover -s tests
```

Output lands in `data/out/` as Parquet. Raw ZIPs are never committed. Links are scraped from NCUA's [quarterly data page](https://ncua.gov/analysis/credit-union-corporate-call-report-data/quarterly-data).

## Numbers you can trust

For June 2026 and December 2025 the totals reconcile to the figures in NCUA's Quarterly Credit Union Data Summary: credit union count, members, loans by type, shares, net worth ratio, delinquency, income and expense. 60 of 67 checks match across six year-ends (2021 to June 2026). The 7 that do not are historical deposit lines that differ by under $0.5B (under 0.1%), for example Dec 2025 money market $367.5B versus $368.0B published. All June 2026 lines match. The likely cause is restated history in NCUA's table, which has not been confirmed. Full table: [docs/RECONCILIATION.md](docs/RECONCILIATION.md).

Things to know before using it:

- **Filter to `is_federally_insured`.** NCUA's raw files include about 85 state-chartered credit unions it does not insure. Its published totals leave them out.
- **The form changed in 2022 and 2023.** NCUA redesigned the call report and adopted CECL, so some accounts disappear and new ones appear. Fields that span the change map both account codes (for example the allowance is Acct_719 before CECL and Acct_AS0048 after). Fields that only exist after the change say so in the dictionary.
- **Net worth ratio.** NCUA's published ratio excludes the CECL transition provision from 2023 on. This dataset carries that provision (`cecl_transition_provision`) and `metrics.net_worth_ratio_ex_cecl` matches NCUA.
- **Employees are estimated.** `employees_fte_estimate` is full-time plus half of part-time. It is not an NCUA definition and is not reconciled to a published figure.
- **Net charge-off ratio, ROA and NIM** use NCUA's own average balances, which are not public. Metrics here use a four-quarter average and land within a few basis points of NCUA's published values, not on them.
- **Mergers.** Credit union counts fell from 5,375 to 4,214 since 2018. A merged credit union's history stays under its old charter number, so per-institution growth across a merger is not meaningful.
- Reports are self-reported and occasionally reposted as "Revised". The ZIPs are used as NCUA publishes them today.

## Not done yet

- Commercial-loan delinquency by type (NCUA moved these to new account codes in 2022).
- More fields. The target is 150 to 200; 115 are in and verified.
- Years before 2018 (the download links use two other naming patterns).
- A static analytics site and natural-language querying. The dictionary is built to be the semantic layer for that.

## MCP server (v1)

A local MCP server lets an AI assistant query this dataset in plain language. It runs over stdio, reads the release Parquet files with DuckDB (downloaded once to `~/.cache/ncua-data-analysis`), and builds its tool descriptions from the dictionary table.

```json
{
  "mcpServers": {
    "ncua-data": {
      "command": "uvx",
      "args": ["--from", "ncua-data-analysis", "ncua-data-mcp"]
    }
  }
}
```

The package is on PyPI: https://pypi.org/project/ncua-data-analysis/. To run the latest development version instead, use `"args": ["--from", "git+https://github.com/bnovarini/ncua-data-analysis", "ncua-data-mcp"]`.

**Hosted, nothing to install:** `https://ncua-data-analysis.fly.dev/mcp` (streamable HTTP, read-only, rate limited to 60 requests a minute per client). Add it as a remote MCP server in any client that supports one, for example `{"mcpServers": {"ncua-data": {"url": "https://ncua-data-analysis.fly.dev/mcp"}}}`.

[![Install in Cursor](https://cursor.com/deeplink/mcp-install-dark.svg)](https://cursor.com/en/install-mcp?name=ncua-data&config=eyJ1cmwiOiJodHRwczovL25jdWEtZGF0YS1hbmFseXNpcy5mbHkuZGV2L21jcCJ9) [![Install in VS Code](https://img.shields.io/badge/VS_Code-Install_MCP-0098FF?logo=visualstudiocode&logoColor=white)](https://vscode.dev/redirect/mcp/install?name=ncua-data&config=%7B%22type%22%3A%22http%22%2C%22url%22%3A%22https%3A%2F%2Fncua-data-analysis.fly.dev%2Fmcp%22%7D) [![Install in VS Code Insiders](https://img.shields.io/badge/VS_Code_Insiders-Install_MCP-24bfa5?logo=visualstudiocode&logoColor=white)](https://insiders.vscode.dev/redirect/mcp/install?name=ncua-data&config=%7B%22type%22%3A%22http%22%2C%22url%22%3A%22https%3A%2F%2Fncua-data-analysis.fly.dev%2Fmcp%22%7D)

Claude Code: `claude mcp add --transport http ncua-data https://ncua-data-analysis.fly.dev/mcp`

Tools: `list_fields`, `find_credit_union`, `credit_union_profile`, `metric_series` (one credit union or an aggregate across all), `peer_compare` (by asset group, state or charter), and `query_metrics` (filters, ordering and limits; no raw SQL). Set `NCUA_DATA_DIR` to use a folder of already-downloaded files. Status: first working version, tested over stdio with a real MCP client; not yet listed in the MCP registry.

## Download

Ready-made Parquet files are attached to the [v0.1 release](https://github.com/bnovarini/ncua-data-analysis/releases/tag/v0.1). GitHub caps release files at 25 MB, so `fact_call_report_curated` and `metrics` come in three parts by year (2018-2020, 2021-2023, 2024-2026) with identical columns:

```python
import duckdb
base = "https://github.com/bnovarini/ncua-data-analysis/releases/download/v0.1/"
parts = [base + f"metrics_{y}.parquet" for y in ("2018_2020", "2021_2023", "2024_2026")]
duckdb.sql(f"SELECT quarter, count(*) FROM read_parquet({parts}) GROUP BY 1 ORDER BY 1").show()
```

## Data source and license

Data: National Credit Union Administration, 5300 Call Report Quarterly Data. NCUA does not state a license on the download page. As a US federal agency's work it is assumed to be public domain, but that assumption has not been confirmed. Credit NCUA when you use it.

Code: MIT, see LICENSE.
