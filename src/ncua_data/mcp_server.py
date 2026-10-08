"""Local stdio MCP server over the published NCUA credit union dataset.

Data comes from the GitHub release parquet files, cached on first use. DuckDB does
the querying. Tool descriptions are generated from the dictionary table.
"""
from __future__ import annotations

import difflib
import json
import os
import re
import unicodedata
import sys
import threading
import time
import urllib.request
from pathlib import Path
from typing import Any, Optional

import duckdb
from mcp.server.fastmcp import FastMCP

from .extras import EXTRA_COLUMNS, extras_sql

RELEASE = "v0.1.1"  # data release; unchanged in 0.1.2
BASE_URL = f"https://github.com/bnovarini/ncua-data-analysis/releases/download/{RELEASE}/"
FILES = [
    "dim_credit_union.parquet",
    *[f"fact_call_report_curated_{y}.parquet" for y in ("2018_2020", "2021_2023", "2024_2026")],
    *[f"metrics_{y}.parquet" for y in ("2018_2020", "2021_2023", "2024_2026")],
]
DICT_PATH = Path(__file__).parent / "mcp_data" / "dictionary.json"
MAX_ROWS = 200
QUERY_TIMEOUT_S = float(os.environ.get("NCUA_QUERY_TIMEOUT", "15"))
KEYS = ("quarter", "cu_number")


def data_dir() -> Path:
    env = os.environ.get("NCUA_DATA_DIR")
    d = Path(env) if env else Path.home() / ".cache" / "ncua-data-analysis" / RELEASE
    d.mkdir(parents=True, exist_ok=True)
    return d


def ensure_files() -> Path:
    d = data_dir()
    for f in FILES:
        p = d / f
        if not p.exists():
            tmp = p.with_suffix(".part")
            urllib.request.urlretrieve(BASE_URL + f, tmp)
            tmp.rename(p)
    return d


# ---- dictionary (bundled, so tool descriptions need no download) ----
_DICT = [(r["table_name"], r["column_name"], r["description"], r["kind"], r["category"]) for r in json.loads(DICT_PATH.read_text())]
DESC = {(t, c): d for t, c, d, _, _ in _DICT}
METRIC_NAMES = [c for t, c, *_ in _DICT if t == "metrics" and c not in KEYS]
FACT_NAMES = [c for t, c, *_ in _DICT if t == "fact_call_report_curated" and c not in KEYS]
DIM_NAMES = [c for t, c, *_ in _DICT if t == "dim_credit_union" and c not in KEYS]
ALL_FIELDS = set(METRIC_NAMES) | set(FACT_NAMES) | set(DIM_NAMES) | set(KEYS)
KIND = {c: k for t, c, _, k, _ in _DICT}


def _first_sentence(s: str, n: int = 90) -> str:
    s = (s or "").split(". ")[0].strip().rstrip(".")
    return s if len(s) <= n else s[: n - 1] + "…"


def _catalog(names: list[str], table: str) -> str:
    return "; ".join(f"{c} ({_first_sentence(DESC.get((table, c), ''))})" for c in names)


METRIC_CATALOG = _catalog(METRIC_NAMES, "metrics")

# ---- database ----
_con: Optional[duckdb.DuckDBPyConnection] = None


def con() -> duckdb.DuckDBPyConnection:
    global _con
    if _con is None:
        d = ensure_files()
        c = duckdb.connect()
        c.execute(f"SET memory_limit='{os.environ.get('NCUA_MEMORY_LIMIT', '1GB')}'")
        c.execute(f"SET threads={int(os.environ.get('NCUA_THREADS', '2'))}")
        g = lambda p: str(d / p)
        c.execute(f"CREATE VIEW dim AS SELECT * FROM read_parquet('{g('dim_credit_union.parquet')}')")
        c.execute(f"CREATE VIEW fact AS SELECT * FROM read_parquet('{g('fact_call_report_curated_*.parquet')}')")
        c.execute(f"CREATE VIEW met_base AS SELECT * FROM read_parquet('{g('metrics_*.parquet')}')")
        have = {r[0] for r in c.execute("DESCRIBE met_base").fetchall()}
        missing = [x for x in EXTRA_COLUMNS if x not in have]
        if missing:
            # Earnings ratios on NCUA's denominators and single-quarter versions are computed here from the fact table.
            c.execute("CREATE TABLE met_x AS SELECT quarter, cu_number, " + ", ".join(f'"{x}"' for x in missing)
                      + " FROM (" + extras_sql("fact") + ")")
            c.execute("CREATE VIEW met AS SELECT b.*, x.* EXCLUDE (quarter, cu_number) FROM met_base b "
                      "LEFT JOIN met_x x USING (quarter, cu_number)")
        else:
            c.execute("CREATE VIEW met AS SELECT * FROM met_base")
        c.execute(
            "CREATE VIEW cu AS SELECT dim.*, fact.* EXCLUDE (quarter, cu_number), met.* EXCLUDE (quarter, cu_number) "
            "FROM dim JOIN fact USING (quarter, cu_number) JOIN met USING (quarter, cu_number)"
        )
        _con = c
    return _con


def run(sql: str, params: list[Any] | None = None) -> list[dict]:
    cur = con().cursor()
    timer = threading.Timer(QUERY_TIMEOUT_S, cur.interrupt)
    timer.start()
    try:
        cur.execute(sql, params or [])
        rows = cur.fetchall()
    except duckdb.InterruptException:
        raise RuntimeError(f"Query took longer than {QUERY_TIMEOUT_S:.0f}s and was stopped. Narrow the filters or quarter range.")
    finally:
        timer.cancel()
    cols = [x[0] for x in cur.description]
    out = []
    for row in rows:
        out.append({k: (round(v, 6) if isinstance(v, float) else v) for k, v in zip(cols, row)})
    return out


_LATEST: Optional[str] = None
FIRST_QUARTER = "2018-03"


def latest_quarter() -> str:
    global _LATEST
    if _LATEST is None:
        _LATEST = run("SELECT max(quarter) q FROM dim")[0]["q"]
    return _LATEST


def _unknown_field_message(name: str) -> str:
    low = str(name).lower()
    key = re.sub(r"[^a-z0-9]+", "_", low).strip("_")
    msg = f"Unknown field '{name}': it is not in this dataset."
    if any(k in low for k in ("originat", "funded", "produc")) or (("auto" in low or "vehicle" in low) and "granted" in low):
        msg += (" Loan originations by loan type are not in the NCUA call report. What exists: loans_granted_ytd and "
                "loans_granted_count_ytd (all loan types combined, year to date), and balances and counts of vehicle loans "
                "outstanding (loans_new_vehicle, loans_used_vehicle, loans_new_vehicle_count, loans_used_vehicle_count).")
    elif key in ("nim", "net_interest_margin", "interest_margin", "net_interest_margin_pct", "nim_pct", "net_interest_margin_percent"):
        msg += (" Net interest margin is in this dataset under another name: nim_ncua_ytd (NCUA's published basis: "
                "year-to-date annualized over the average of prior-December and current assets), nim_avg_assets_4q, "
                "nim_year_end_assets, or nim_quarterly (single quarter). Say which one you used.")
    elif key in ("capital_ratio", "capital_adequacy", "capital_adequacy_ratio", "capital", "tier_1", "tier1", "tier_1_capital_ratio",
                 "leverage_ratio", "regulatory_capital", "capital_to_assets"):
        msg += (" Credit unions report net worth, not bank-style capital ratios. The capital ratio NCUA publishes is "
                "net_worth_ratio_ex_cecl (net worth over assets, fraction). net_worth_to_assets includes the CECL "
                "transition provision from 2023.")
    elif key in ("roe", "return_on_equity", "return_on_equity_pct", "return_on_capital"):
        msg += (" Return on equity is not a call report metric: credit unions have no shareholder equity. Closest: "
                "roa_ncua_ytd (matches NCUA's published return on average assets) for earnings, net_worth_ratio_ex_cecl "
                "for capital. Say which one you used.")
    elif "profit" in low or ("margin" in low and "interest" not in low):
        msg += (" 'Profit margin' is not a call report metric. Closest: roa_ncua_ytd or roa_avg_assets_4q "
                "(net income / assets), net_income_ytd, efficiency_ratio, nim_ncua_ytd (net interest margin). Say which one you used.")
    elif any(k in low for k in ("best", "worst", "rank", "score", "rating", "top")):
        msg += (" There is no single 'best' measure. Pick explicit metrics (for example roa_avg_assets_4q, "
                "delinquency_rate, efficiency_ratio, net_worth_to_assets) and say which one you ranked on.")
    elif any(k in low for k in ("apr", "interest_rate", "rate_on", "pricing", "_rate")) and "delinq" not in low and "chargeoff" not in low:
        msg += (" Loan and share interest rates by product are not in the NCUA call report. Closest: loan_yield "
                "(interest on loans / loans, an average yield), cost_of_shares (interest expense / shares), nim_avg_assets_4q.")
    elif any(k in low for k in ("salary", "ceo", "executive", "bonus", "wage", "pay")):
        msg += (" Individual or executive pay is not in the call report. Closest: employee_compensation_ytd (total "
                "compensation, year to date), compensation_per_fte, compensation_share_of_opex.")
    elif any(k in low for k in ("age", "gender", "race", "income_of", "demograph", "fico", "credit_score", "zip_of_members")):
        msg += (" Member demographics and credit scores are not in the call report. Closest: members, potential_members, "
                "members_per_branch, is_low_income, is_minority_depository, field_of_membership_code.")
    elif any(k in low for k in ("complaint", "review", "customer_satisfaction", "closed", "opened", "closure")):
        msg += (" Complaints, reviews and branch openings or closings are not in the call report. Only the current "
                "branch count exists (branches), so changes can be read from its series.")
    toks = [t for t in re.split(r"[^a-z0-9]+", low) if len(t) > 3]
    close = difflib.get_close_matches(str(name), sorted(ALL_FIELDS), n=5, cutoff=0.6)
    close += [f for f in sorted(ALL_FIELDS) if any(t in f for t in toks) and f not in close][:5]
    if close:
        msg += " Closest field names: " + ", ".join(close[:8]) + "."
    return msg + " Use list_fields with a search term to find the right field."


def q(name: str) -> str:
    if name not in ALL_FIELDS:
        raise ValueError(_unknown_field_message(name))
    return f'"{name}"'


STATE_NAMES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA", "colorado": "CO",
    "connecticut": "CT", "delaware": "DE", "district of columbia": "DC", "florida": "FL", "georgia": "GA",
    "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD", "massachusetts": "MA",
    "michigan": "MI", "minnesota": "MN", "mississippi": "MS", "missouri": "MO", "montana": "MT",
    "nebraska": "NE", "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM",
    "new york": "NY", "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC", "south dakota": "SD",
    "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY", "puerto rico": "PR", "guam": "GU",
}


def check_state(state: Optional[str]) -> Optional[str]:
    if state is None:
        return None
    st = str(state).strip().upper()
    if str(state).strip().lower() in STATE_NAMES:
        return STATE_NAMES[str(state).strip().lower()]
    if len(st) != 2 or not st.isalpha():
        raise ValueError(f"state must be a 2-letter postal code such as TX or NJ, got '{state}'. Full state names such as Texas also work.")
    return st


PEER_LABELS = {1: "under $2M", 2: "$2M-$10M", 3: "$10M-$50M", 4: "$50M-$100M", 5: "$100M-$500M", 6: "$500M+"}
_PEER_HELP = "peer_group is 1 to 6: " + ", ".join(f"{k} = {v}" for k, v in PEER_LABELS.items())


def check_peer_group(pg: Any) -> Optional[int]:
    """Accept the code (1-6) or its asset-size label ('$100M-$500M')."""
    if pg is None:
        return None
    if isinstance(pg, str):
        t = pg.strip().lower().replace(" ", "")
        for k, v in PEER_LABELS.items():
            if t in (v.lower().replace(" ", ""), str(k)):
                return k
        raise ValueError(f"peer_group '{pg}' not recognised. {_PEER_HELP}.")
    if isinstance(pg, bool) or pg not in PEER_LABELS:
        raise ValueError(f"peer_group must be one of the codes or labels. {_PEER_HELP}; got {pg}.")
    return int(pg)


def _fold(text: str) -> str:
    """Strip accents so 'Señor' and 'Económica' match the ASCII forms NCUA data uses."""
    return "".join(c for c in unicodedata.normalize("NFKD", text) if not unicodedata.combining(c))


def last_reported(cu_number: int) -> Optional[dict]:
    r = run("SELECT name, state, max(quarter) AS last_quarter FROM dim WHERE cu_number = ? "
            "GROUP BY name, state ORDER BY max(quarter) DESC LIMIT 1", [cu_number])
    return r[0] if r else None


def no_data_error(cu_number: int, qt: str) -> dict:
    lr = last_reported(cu_number)
    if lr is None:
        return {"error": f"cu_number {cu_number} is not in the dataset (federally insured credit unions, 2018-2026). "
                         "Use find_credit_union to look up the number."}
    return {"error": f"No data for cu_number {cu_number} ({lr['name']}, {lr['state']}) in {qt}. Its last reported quarter is "
                     f"{lr['last_quarter']}: it may have merged, closed, or left federal insurance. Retry with quarter='{lr['last_quarter']}'."}


def _quarter(quarter: Optional[str]) -> str:
    if not quarter:
        return latest_quarter()
    if not (len(quarter) == 7 and quarter[4] == "-" and quarter[5:] in ("03", "06", "09", "12")):
        raise ValueError("quarter must look like 2025-12 (quarter ends: 03, 06, 09, 12)")
    if quarter > latest_quarter():
        raise ValueError(f"The dataset ends at {latest_quarter()}; {quarter} is after the last reported quarter. "
                         "That is a data limit, not a credit union that stopped reporting.")
    if quarter < FIRST_QUARTER:
        raise ValueError(f"The dataset starts at {FIRST_QUARTER}; {quarter} is before the first quarter.")
    return quarter


_SOURCES = {
    "fact_call_report_curated": "NCUA 5300 Call Report amounts as published by NCUA (quarterly). Account codes are in the description where known.",
    "metrics": "Derived by this project from NCUA 5300 Call Report fields; the formula is in the description.",
    "dim_credit_union": "NCUA credit union directory attributes, one row per credit union per quarter.",
}
NOTE = (
    "Data: NCUA 5300 call reports, federally insured credit unions, 2018-03 to 2026-06, "
    "from github.com/bnovarini/ncua-data-analysis. Dollar fields are in dollars; ratio fields "
    "are fractions unless the field description says otherwise."
)

mcp = FastMCP("ncua-data-analysis", instructions=NOTE)


SEARCH_ALIASES = {
    "net interest margin": ["nim_"], "nim": ["nim_"], "interest margin": ["nim_"],
    "capital ratio": ["net_worth_ratio"], "capital adequacy": ["net_worth_ratio"], "capital": ["net_worth_ratio"],
    "cost of funds": ["cost_of_funds"], "return on assets": ["roa_"], "roa": ["roa_"],
    "yield": ["loan_yield"], "return on equity": ["roa_", "net_worth_ratio"], "roe": ["roa_", "net_worth_ratio"],
}


def _search_terms(search: str) -> list[str]:
    low = search.lower().strip()
    key = re.sub(r"[^a-z0-9]+", " ", low).strip()
    return [low] + SEARCH_ALIASES.get(key, [])


@mcp.tool(
    description=(
        "List available fields with plain-language definitions from the data dictionary. "
        "Filter by table ('metrics' = computed ratios, 'fact_call_report_curated' = reported amounts, "
        "'dim_credit_union' = attributes), by search text, or both. "
        "Fields marked basis=year_to_date reset each January; metric_series can de-cumulate them. "
        f"Computed metrics available: {METRIC_CATALOG}"
    )
)
def list_fields(table: Optional[str] = None, search: Optional[str] = None, limit: int = 60) -> list[dict]:
    if limit < 1 or limit > MAX_ROWS:
        raise ValueError(f"limit must be between 1 and {MAX_ROWS}, got {limit}.")
    rows = []
    for t, c, d, k, cat in _DICT:
        if table and t != table:
            continue
        if search and not any(term in f"{c} {d} {cat or ''}".lower() for term in _search_terms(search)):
            continue
        row = {"table": t, "field": c, "kind": k, "category": cat, "description": d,
               "source": _SOURCES.get(t, "NCUA call report data"), "data_through": latest_quarter()}
        if k == "ytd":
            row["basis"] = "year_to_date"
        if k == "ratio":
            row["unit"] = "fraction (0.05 = 5%)"
        elif k == "pct":
            row["unit"] = "hundredths of a percent (1165 = 11.65%)"
        if c in FIELD_NOTES:
            row["note"] = FIELD_NOTES[c]
        rows.append(row)
    return rows[:limit]


_STOP = r"\b(federal|credit|union|fcu|cu|the)\b"


def _norm_sql(col: str) -> str:
    return (
        f"trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(strip_accents(lower({col})), '[.'']', '', 'g'), "
        f"'[^a-z0-9 ]', ' ', 'g'), '{_STOP}', ' ', 'g'), '\\s+', ' ', 'g'))"
    )


_NORM = _norm_sql("name")

# Brand names that differ from the legal name NCUA lists. Each alias was checked against the dataset.
ALIASES = {
    "becu": ["boeing employees"],
    "penfed": ["pentagon"],
    "secu": ["state employees'", "state employees cu of maryland"],
}


def normalize_name(text: str) -> str:
    """Lowercase, drop punctuation and the words federal/credit/union/fcu/cu/the, collapse spaces."""
    t = re.sub(r"[^a-z0-9 ]", " ", re.sub(r"[.']", "", _fold(text).lower()))
    t = re.sub(_STOP, " ", t)
    return re.sub(r"\s+", " ", t).strip()


@mcp.tool(
    description=(
        "Find credit unions by name, state, charter type (federal/state) or asset peer group, in a given quarter "
        "(default: latest). Name matching ignores case, punctuation and the words 'federal credit union' / 'FCU', "
        "so 'SRP Federal Credit Union' finds 'SRP'; partial names work. It also matches former names (a credit "
        "union that was renamed shows up under the old name, match = former_name, with its current name) and a few "
        "brand names (BECU, PenFed, SECU). Results are ranked: match = exact, starts_with, contains, former_name, "
        "then larger assets first. Each row has 'ambiguous': true when several different credit unions fit the name. "
        "In that case ask the user which one they mean (use city and state to tell them apart) rather than picking "
        "the first. If nothing matches, the result is one row with 'no_match' explaining why (for example the credit "
        "union merged or closed, with its last reported quarter). Returns cu_number, name, location, total assets, "
        "members. Use the returned cu_number in the other tools. Only federally insured credit unions."
    )
)
def find_credit_union(
    name: Optional[str] = None,
    state: Optional[str] = None,
    charter_type: Optional[str] = None,
    peer_group: Optional[int | str] = None,
    quarter: Optional[str] = None,
    limit: int = 15,
) -> list[dict]:
    qt = _quarter(quarter)
    state, peer_group = check_state(state), check_peer_group(peer_group)
    if limit < 1 or limit > 50:
        raise ValueError(f"limit must be between 1 and 50, got {limit}.")
    lim = limit
    base_where, base_p = ["is_federally_insured", "quarter = ?"], [qt]
    if state:
        base_where.append("upper(state) = ?"); base_p.append(state)
    if charter_type:
        base_where.append("lower(charter_type) = ?"); base_p.append(charter_type.lower())
    if peer_group is not None:
        base_where.append("peer_group = ?"); base_p.append(peer_group)
    cols = "cu_number, name, city, state, charter_type, peer_group_label, total_assets, members"
    has_name = bool(name and name.strip())
    if not has_name:
        rows = run(f"SELECT {cols} FROM cu WHERE {' AND '.join(base_where)} ORDER BY total_assets DESC NULLS LAST LIMIT {lim}", base_p)
        for r in rows:
            r["match"] = None
        return rows

    qn = normalize_name(name)
    # Normalized search phrases: the typed name, plus the legal names behind a known brand alias.
    phrases = [qn] if qn else []
    phrases += [normalize_name(x) for x in ALIASES.get(qn, [])]
    rows: list[dict] = []
    if not qn:  # only filler words, e.g. "federal credit union": fall back to the raw text
        rows = run(f"SELECT {cols}, 2 AS _rank FROM cu WHERE {' AND '.join(base_where)} AND upper(name) LIKE ? "
                   f"ORDER BY total_assets DESC NULLS LAST LIMIT {lim}", base_p + [f"%{name.strip().upper()}%"])
    for ph in phrases:
        toks = ph.split()
        cond = " AND ".join(f"contains({_NORM}, ?)" for _ in toks)
        rows += run(
            f"SELECT {cols}, CASE WHEN {_NORM} = ? THEN 0 WHEN starts_with({_NORM}, ?) THEN 1 ELSE 2 END AS _rank "
            f"FROM cu WHERE {' AND '.join(base_where)} AND {cond} ORDER BY _rank, total_assets DESC NULLS LAST LIMIT {lim}",
            [ph, ph] + base_p + toks,
        )
    labels = {0: "exact", 1: "starts_with", 2: "contains"}
    seen: dict[int, dict] = {}
    for r in rows:
        r["match"] = labels[r.pop("_rank")]
        if r["cu_number"] not in seen:
            seen[r["cu_number"]] = r
    # Former names: a renamed credit union is still the same cu_number.
    for ph in phrases:
        toks = ph.split()
        cond = " AND ".join(f"contains({_norm_sql('h.name')}, ?)" for _ in toks)
        former = run(
            f"SELECT DISTINCT h.cu_number, h.name AS former_name FROM dim h WHERE {cond}", toks)
        ids = [f["cu_number"] for f in former if f["cu_number"] not in seen]
        if ids:
            fm = {f["cu_number"]: f["former_name"] for f in former}
            cur = run(f"SELECT {cols} FROM cu WHERE {' AND '.join(base_where)} AND cu_number IN ({', '.join('?' for _ in ids)}) "
                      "AND name IS NOT NULL", base_p + ids)
            for r in cur:
                if r["name"] != fm[r["cu_number"]] and r["cu_number"] not in seen:
                    r["match"], r["matched_former_name"] = "former_name", fm[r["cu_number"]]
                    seen[r["cu_number"]] = r
    if not seen and qn:
        # Abbreviations and partial words: "Navy Fed" (words are prefixes of the full legal name), "NFCU" (initials).
        raw = "trim(regexp_replace(regexp_replace(strip_accents(lower(name)), '[^a-z0-9 ]', ' ', 'g'), '\\s+', ' ', 'g'))"
        words = [w for w in re.sub(r"[^a-z0-9 ]", " ", _fold(name).lower()).split() if w]
        conds, cp = [], []
        for w in words:
            conds.append(f"contains({raw}, ?)"); cp.append(w)
        extra = run(f"SELECT {cols}, 'partial_words' AS _m FROM cu WHERE {' AND '.join(base_where)} AND {' AND '.join(conds)} "
                    f"ORDER BY total_assets DESC NULLS LAST LIMIT {lim}", base_p + cp)
        if len(words) == 1 and len(words[0]) >= 3:
            ini = f"array_to_string(list_transform(list_filter(string_split({raw}, ' '), x -> x <> ''), x -> x[1]), '')"
            tk = words[0]
            extra += run(f"SELECT {cols}, 'abbreviation' AS _m FROM cu WHERE {' AND '.join(base_where)} AND "
                         f"({ini} = ? OR {ini} = ?) ORDER BY total_assets DESC NULLS LAST LIMIT {lim}", base_p + [tk, (lambda t: t if len(t) >= 2 else tk)(tk.removesuffix('fcu').removesuffix('cu'))])
        for r in extra:
            r["match"] = r.pop("_m")
            seen.setdefault(r["cu_number"], r)
    order = {"exact": 0, "starts_with": 1, "contains": 2, "former_name": 3, "partial_words": 4, "abbreviation": 5}
    out = sorted(seen.values(), key=lambda r: (order[r["match"]], -(r["total_assets"] or 0)))[:lim]
    if out:
        exact = [r for r in out if r["match"] == "exact"]
        starts = [r for r in out if r["match"] == "starts_with"]
        former = [r for r in out if r["match"] == "former_name"]
        if exact or starts:
            # one clear exact match is not made ambiguous by former-name hits; anything else that overlaps is
            amb = not (len(exact) == 1 and not starts) and len(exact) + len(starts) + len(former) > 1
        else:
            amb = len(out) > 1
        for r in out:
            if r["match"] == "exact":
                older = run(f"SELECT DISTINCT cu_number FROM dim WHERE {_NORM} = ? AND cu_number <> ? AND cu_number NOT IN "
                            "(SELECT cu_number FROM dim WHERE quarter = ?)", [qn, r["cu_number"], qt])
                if older:
                    r["older_same_name"] = [o["cu_number"] for o in older][:5]
                    r["note"] = ("Another credit union with the same name (cu_number in older_same_name) stopped reporting "
                                 "earlier. This row is the one reporting in " + qt + ".")
        corp = bool(re.search(r"\bcorporate\b", _fold(name).lower()))
        for r in out:
            if corp:
                r["note"] = CORPORATE_NOTE
            r["ambiguous"] = amb
            if r["match"] == "abbreviation":
                # initials can belong to a different credit union than the one the user means
                r["ambiguous"] = True
                r["note"] = "Matched on initials only. Confirm this is the credit union meant before using it."
        return out
    # Nothing in this quarter. Is it a credit union that stopped reporting?
    gone = []
    for ph in phrases:
        toks = ph.split()
        cond = " AND ".join(f"contains({_norm_sql('name')}, ?)" for _ in toks)
        fw, fp = [], []
        if state:
            fw.append("upper(state) = ?"); fp.append(state)
        if charter_type:
            fw.append("lower(charter_type) = ?"); fp.append(charter_type.lower())
        if peer_group is not None:
            fw.append("peer_group = ?"); fp.append(peer_group)
        cond_all = " AND ".join([cond] + fw)
        gone += run(f"SELECT cu_number, name, city, state, max(quarter) AS last_reported_quarter FROM dim "
                    f"WHERE {cond_all} GROUP BY cu_number, name, city, state HAVING max(quarter) < ? "
                    "ORDER BY max(quarter) DESC LIMIT 5", toks + fp + [qt])
    if gone:
        return [{"no_match": f"No federally insured credit union matches '{name}' (with the state, size and charter filters given) in {qt}, but these stopped reporting "
                             "earlier (merged, closed, or left federal insurance). Use their cu_number with quarter set to "
                             "the last reported quarter, and tell the user they are no longer reporting.", **g} for g in gone]
    extra_note = (" " + CORPORATE_NOTE) if re.search(r"\bcorporate\b", _fold(name).lower()) else ""
    return [{"no_match": f"No federally insured credit union matches '{name}' in {qt}. NCUA lists legal names, which can "
                         "differ from the brand name (BECU is listed as BOEING EMPLOYEES). Try a shorter or different part "
                         "of the name, or search by state. This dataset covers US federally insured credit unions only." + extra_note}]


@mcp.tool(
    description=(
        "Profile of one credit union for one quarter (default latest): attributes, size, and every computed metric "
        "with its plain-language definition available through list_fields. Returns null values where NCUA data has none."
    )
)
def credit_union_profile(cu_number: int, quarter: Optional[str] = None) -> dict:
    qt = _quarter(quarter)
    rows = run("SELECT * FROM cu WHERE cu_number = ? AND quarter = ?", [cu_number, qt])
    if not rows:
        return no_data_error(cu_number, qt)
    r = rows[0]
    hist = run("SELECT name, min(quarter) AS first_quarter, max(quarter) AS last_quarter FROM dim WHERE cu_number = ? "
               "AND name <> ? GROUP BY name ORDER BY min(quarter)", [cu_number, r["name"]])
    former = [h["name"] for h in hist if h["last_quarter"] < qt]
    later = [h["name"] for h in hist if h["first_quarter"] > qt]
    keep_fact = ["total_assets", "loans_and_leases_total", "total_shares", "members", "net_worth", "net_income_ytd"]
    return {
        "attributes": {k: r[k] for k in DIM_NAMES if k in r},
        "former_names": former,
        "later_names": later,
        "name_history": hist,
        "quarter": qt,
        "key_amounts": {k: r[k] for k in keep_fact if k in r},
        "metrics": {k: r[k] for k in METRIC_NAMES if k in r},
        "note": NOTE,
    }


SERIES_WARN_ROWS = 250
SERIES_MAX_ROWS = 1000
SUMMABLE = ("dollars", "stock", "count", "ytd")
CECL_NOTE = ("Accounting break: most credit unions adopted CECL in 2023. Allowance and provision levels step up around "
             "2023-03 because of the accounting change, not credit deterioration. Do not read the jump as a trend.")
NETWORTH_NOTE = ("From 2023 this includes the CECL transition provision. NCUA's published net worth ratio is "
                 "net_worth_ratio_ex_cecl.")
CORPORATE_NOTE = ("Corporate credit unions are not in this dataset (federally insured natural-person credit unions "
                  "only). This is a name match, not a corporate credit union.")
EXCECL_NOTE = ("Accounting break: most credit unions adopted CECL in 2023-03. The day-one adoption adjustment can lower "
               "an individual credit union's net worth ratio at the break, so a step down here can be an accounting "
               "change, not performance.")
REBASE = ("Annualized from year-to-date flow, so this ratio re-bases every January: Q4 averages the whole year, Q1 is one "
          "quarter, so a fast rate cycle shows as a step between Q4 and Q1 (NCUA's own published year-to-date ratios step "
          "the same way). ")
_ALT = {
    "roa": ("roa_ncua_ytd", "roa_quarterly"), "nim": ("nim_ncua_ytd", "nim_quarterly"),
    "loan_yield": ("loan_yield_ncua_ytd", "loan_yield_quarterly"),
    "cost_of_shares": ("cost_of_funds_ncua_ytd", "cost_of_funds_quarterly"),
    "net_chargeoff_rate": ("net_chargeoff_rate_ncua_ytd", "net_chargeoff_rate_quarterly"),
}


def _rebase_note(kind: str) -> str:
    a, b = _ALT[kind]
    return REBASE + f"For a series without the January step use {b} (single quarter); {a} matches NCUA's published basis."


_REBASE_NOTES = {
    "roa_year_end_assets": _rebase_note("roa"), "roa_avg_assets_4q": _rebase_note("roa"),
    "nim_year_end_assets": _rebase_note("nim"), "nim_avg_assets_4q": _rebase_note("nim"),
    "loan_yield": _rebase_note("loan_yield"), "cost_of_shares": _rebase_note("cost_of_shares"),
    "net_chargeoff_rate": _rebase_note("net_chargeoff_rate"), "net_chargeoff_rate_avg_loans_4q": _rebase_note("net_chargeoff_rate"),
    "roa_ncua_ytd": REBASE + "roa_quarterly is the single-quarter version.",
    "nim_ncua_ytd": REBASE + "nim_quarterly is the single-quarter version.",
    "loan_yield_ncua_ytd": REBASE + "loan_yield_quarterly is the single-quarter version.",
    "cost_of_funds_ncua_ytd": REBASE + "cost_of_funds_quarterly is the single-quarter version.",
    "net_chargeoff_rate_ncua_ytd": REBASE + "net_chargeoff_rate_quarterly is the single-quarter version.",
    "opex_to_assets": REBASE + "There is no single-quarter version; non_interest_expense_quarter has the quarter's dollars.",
    "provision_to_loans": REBASE + "provision_quarter has the quarter's dollars.",
    "compensation_per_fte": REBASE, "operating_expense_per_fte": REBASE,
}
_QUARTERLY_NOTE = ("Single-quarter flow times 4 over the average of the prior and current quarter-end balance. Derived by this "
                   "project: NCUA publishes only year-to-date figures, so this is not an NCUA published ratio. It is consistent "
                   "with NCUA's year-to-date totals (see docs/RECONCILIATION.md).")
FIELD_NOTES = {
    **_REBASE_NOTES,
    "roa_quarterly": _QUARTERLY_NOTE, "nim_quarterly": _QUARTERLY_NOTE, "loan_yield_quarterly": _QUARTERLY_NOTE,
    "cost_of_funds_quarterly": _QUARTERLY_NOTE, "net_chargeoff_rate_quarterly": _QUARTERLY_NOTE,
    "allowance_to_loans": CECL_NOTE, "allowance_for_credit_losses": CECL_NOTE, "provision_to_loans": CECL_NOTE,
    "provision_quarter": CECL_NOTE, "provision_for_loan_losses_ytd": CECL_NOTE,
    "net_worth_to_assets": NETWORTH_NOTE, "net_worth": NETWORTH_NOTE, "net_worth_ratio_ex_cecl": EXCECL_NOTE,
    "net_worth_ratio_pct_x100": "UNITS: hundredths of a percent (1165 = 11.65%). Prefer net_worth_ratio_ex_cecl, a fraction (0.1165 = 11.65%).",
}
CECL_BREAK_QUARTER = "2023-03"
YTD_WARNING = ("Year-to-date values reset every January, so charting or summing them across quarters gives a sawtooth. "
               "Leave quarterly=true (the default) for per-quarter figures.")
IDX = "(cast(left(quarter, 4) AS INTEGER) * 4 + cast(right(quarter, 2) AS INTEGER) / 3)"
# Pooled (sum-based) versions of ratio metrics. {a} is the annualisation factor for year-to-date flows.
POOLED = {
    "efficiency_ratio": "sum(non_interest_expense_ytd) / nullif(sum(interest_income_ytd) - sum(interest_expense_ytd) + sum(non_interest_income_ytd), 0)",
    "delinquency_rate": "sum(delinquent_2m_plus) / nullif(sum(loans_and_leases_total), 0)",
    "loan_to_share": "sum(loans_and_leases_total) / nullif(sum(total_shares_and_deposits), 0)",
    "loans_to_assets": "sum(loans_and_leases_total) / nullif(sum(total_assets), 0)",
    "net_worth_to_assets": "sum(net_worth) / nullif(sum(total_assets), 0)",
    "net_worth_ratio_ex_cecl": "(sum(net_worth) - sum(coalesce(cecl_transition_provision, 0))) / nullif(sum(total_assets), 0)",
    "allowance_to_loans": "sum(allowance_for_credit_losses) / nullif(sum(loans_and_leases_total), 0)",
    "mix_auto": "sum(loans_new_vehicle + loans_used_vehicle) / nullif(sum(loans_and_leases_total), 0)",
    "net_chargeoff_rate": "(sum(chargeoffs_ytd) - sum(recoveries_ytd)) * (4.0 / (cast(right(quarter, 2) AS INTEGER) / 3)) / nullif(sum(loans_and_leases_total), 0)",
    "roa_year_end_assets": "sum(net_income_ytd) * (4.0 / (cast(right(quarter, 2) AS INTEGER) / 3)) / nullif(sum(total_assets), 0)",
    "nim_year_end_assets": "(sum(interest_income_ytd) - sum(interest_expense_ytd)) * (4.0 / (cast(right(quarter, 2) AS INTEGER) / 3)) / nullif(sum(total_assets), 0)",
    "loan_yield": "sum(interest_on_loans_ytd) * (4.0 / (cast(right(quarter, 2) AS INTEGER) / 3)) / nullif(sum(loans_and_leases_total), 0)",
    "cost_of_shares": "sum(interest_expense_ytd) * (4.0 / (cast(right(quarter, 2) AS INTEGER) / 3)) / nullif(sum(total_shares_and_deposits), 0)",
    "opex_to_assets": "sum(non_interest_expense_ytd) * (4.0 / (cast(right(quarter, 2) AS INTEGER) / 3)) / nullif(sum(total_assets), 0)",
}
# Pooled versions of the NCUA-basis and single-quarter ratios. (numerator expression over cu rows, balance column).
# Year-to-date flows over the average of (prior December, now) system balances; quarterly flows over (prior quarter, now).
POOLED_NCUA = {
    "roa_ncua_ytd": ("sum(net_income_ytd)", "total_assets", "ytd"),
    "nim_ncua_ytd": ("sum(interest_income_ytd) - sum(interest_expense_ytd)", "total_assets", "ytd"),
    "loan_yield_ncua_ytd": ("sum(interest_on_loans_ytd)", "loans_and_leases_total", "ytd"),
    "cost_of_funds_ncua_ytd": ("sum(interest_expense_ytd)", "total_assets", "ytd"),
    "net_chargeoff_rate_ncua_ytd": ("sum(chargeoffs_ytd) - sum(recoveries_ytd)", "loans_and_leases_total", "ytd"),
    "roa_quarterly": ("sum(net_income_quarter)", "total_assets", "quarterly"),
    "nim_quarterly": ("sum(interest_income_quarter) - sum(interest_expense_quarter)", "total_assets", "quarterly"),
    "loan_yield_quarterly": ("sum(interest_on_loans_quarter)", "loans_and_leases_total", "quarterly"),
    "cost_of_funds_quarterly": ("sum(interest_expense_quarter)", "total_assets", "quarterly"),
    "net_chargeoff_rate_quarterly": ("sum(net_chargeoffs_quarter)", "loans_and_leases_total", "quarterly"),
}
_PRIOR_DEC = "cast(cast(left({q}, 4) AS INTEGER) - 1 AS VARCHAR) || '-12'"
_PRIOR_Q = ("CASE right({q}, 2) WHEN '03' THEN cast(cast(left({q}, 4) AS INTEGER) - 1 AS VARCHAR) || '-12' "
            "WHEN '06' THEN left({q}, 4) || '-03' WHEN '09' THEN left({q}, 4) || '-06' ELSE left({q}, 4) || '-09' END")
# Same-credit-union growth: sum(now) / sum(a year ago) - 1, over credit unions present in both quarters.
POOLED_GROWTH = {
    "asset_growth_yoy": "total_assets", "loan_growth_yoy": "loans_and_leases_total",
    "share_growth_yoy": "total_shares_and_deposits", "member_growth_yoy": "members",
}
AGGS = ("median", "mean", "sum", "count", "ratio_of_sums", "pooled")



@mcp.tool(
    description=(
        "Time series of one metric or reported field. Give cu_number for one credit union. Otherwise it aggregates "
        "all federally insured credit unions matching the optional state (2-letter code) / peer_group (1-6) / "
        "min_assets filters: aggregate='median' (default for ratios), 'mean', 'sum' (default for dollar and count "
        "fields; not allowed on ratios), 'count' (how many credit unions report the field), 'ratio_of_sums' (sum of "
        "field / sum of the denominator field, e.g. shares_certificates over total_shares_and_deposits), or 'pooled' "
        "(the system-wide version of a ratio metric: total numerators over total denominators, not a median of "
        "credit unions; available for efficiency_ratio, delinquency_rate, loan_to_share, loans_to_assets, "
        "net_worth_to_assets, net_worth_ratio_ex_cecl, allowance_to_loans, mix_auto, net_chargeoff_rate, "
        "roa_year_end_assets, nim_year_end_assets, loan_yield, cost_of_shares, opex_to_assets, the NCUA-basis ratios "
        "(roa_ncua_ytd, nim_ncua_ytd, loan_yield_ncua_ytd, cost_of_funds_ncua_ytd, net_chargeoff_rate_ncua_ytd; pooled "
        "values reproduce NCUA's published ROA, NIM and net charge-off ratio), the single-quarter ratios (roa_quarterly "
        "and the like) and the growth metrics "
        "asset/loan/share/member_growth_yoy, which compare the same credit unions a year apart). Median and mean "
        "treat every credit union equally, tiny ones included. Year-to-date fields (basis year_to_date in "
        "list_fields: income, expenses, charge-offs, loans granted) are returned per quarter by default "
        "(quarterly=true: this quarter minus the prior quarter of the same year); quarterly=false returns the raw "
        "year-to-date figure, which resets each January. Each row carries basis (and unit for percent-x100 fields); "
        "rows with no data explain why. group_by='peer_group' or 'state' returns one row per group (defaults to the "
        "latest quarter). Series that cross the 2023 CECL accounting change carry a break_note. Range in quarters "
        "like 2021-12; the data runs 2018-03 to the latest quarter. Metrics: " + METRIC_CATALOG
    )
)
def metric_series(
    field: str,
    cu_number: Optional[int] = None,
    state: Optional[str] = None,
    peer_group: Optional[int | str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    aggregate: Optional[str] = None,
    year_end_only: bool = False,
    denominator: Optional[str] = None,
    group_by: Optional[str] = None,
    quarterly: Optional[bool] = None,
    min_assets: Optional[float] = None,
) -> list[dict]:
    f = q(field)
    kind = KIND.get(field)
    if min_assets is not None and min_assets < 0:
        raise ValueError(f"min_assets must be 0 or more, got {min_assets}.")
    if start:
        _quarter(start)
    if end:
        _quarter(end)
    if group_by not in (None, "peer_group", "state"):
        raise ValueError("group_by must be 'peer_group' or 'state'")
    if group_by and cu_number is not None:
        raise ValueError("group_by compares groups of credit unions; leave cu_number out.")
    if group_by and not start and not end:
        start = end = latest_quarter()
    lo, hi = start or FIRST_QUARTER, end or latest_quarter()
    if quarterly is True and kind != "ytd":
        raise ValueError(f"quarterly applies only to year-to-date fields; '{field}' is not one.")
    is_ytd = kind == "ytd"
    use_q = is_ytd and quarterly is not False
    rng = ["quarter >= ?", "quarter <= ?"] + (["right(quarter, 2) = '12'"] if year_end_only else [])
    rng_p = [lo, hi]
    basis = ("quarterly" if use_q else "year_to_date") if is_ytd else None
    if use_q:
        vexpr = (f"CASE WHEN right(quarter, 2) = '03' THEN raw WHEN {IDX} - lag({IDX}) OVER w = 1 "
                 "THEN raw - lag(raw) OVER w END")
    else:
        vexpr = "raw"

    def finish(rows: list[dict], aggregated: bool) -> list[dict]:
        for r in rows:
            if basis:
                r["basis"] = basis
            if kind == "pct":
                r["unit"] = "hundredths of a percent (1165 = 11.65%)"
            empty = (r.get("n_credit_unions") == 0) if aggregated else (r.get("value") is None)
            if empty:
                if field.endswith("_yoy"):
                    r["note"] = "No value: year-over-year fields need the same quarter a year earlier, so they start in 2019-03."
                elif field == "cecl_transition_provision":
                    r["note"] = "No value: the CECL transition provision is reported from 2023-03."
                elif is_ytd and use_q:
                    r["note"] = "No value: a quarterly figure needs the prior quarter of the same year (or Q1)."
                else:
                    r["note"] = "No credit union reported a value for this field in this quarter."
            if field in FIELD_NOTES and FIELD_NOTES[field] is CECL_NOTE and r.get("quarter") == CECL_BREAK_QUARTER:
                r["break_note"] = CECL_NOTE
            if field == "net_worth_ratio_ex_cecl" and r.get("quarter") == CECL_BREAK_QUARTER:
                r["break_note"] = EXCECL_NOTE
            if field in ("net_worth_to_assets", "net_worth") and r.get("quarter") == CECL_BREAK_QUARTER:
                r["break_note"] = NETWORTH_NOTE
        for r in rows:
            if "peer_group" in r and r["peer_group"] in PEER_LABELS:
                r["peer_group_label"] = PEER_LABELS[r["peer_group"]]
        if rows and is_ytd and not use_q:
            rows[0]["warning"] = YTD_WARNING
        if rows and field in _REBASE_NOTES:
            rows[0]["annualization_note"] = _REBASE_NOTES[field]
        elif rows and field in FIELD_NOTES and field.endswith("_quarterly"):
            rows[0]["annualization_note"] = FIELD_NOTES[field]
        if len(rows) > SERIES_MAX_ROWS:
            raise ValueError(
                f"This would return {len(rows)} rows (about {len(rows) * 80 // 1000} KB), over the {SERIES_MAX_ROWS}-row limit. "
                "Narrow it: set start and end, use year_end_only=true, filter by state or peer_group, or drop group_by "
                "and aggregate. For a ranking, group_by with a single quarter returns one row per group.")
        if len(rows) > SERIES_WARN_ROWS:
            first = rows[0]
            first["warning"] = ((first.get("warning") + " ") if first.get("warning") else "") + (
                f"Large result: {len(rows)} rows (about {len(rows) * 80 // 1000} KB). Use year_end_only=true, a shorter "
                "start-end range, or fewer groups if you only need a trend.")
        return rows

    if cu_number is not None:
        rows = run(
            f"WITH s AS (SELECT quarter, cu_number, {f} AS raw FROM cu WHERE cu_number = ?), "
            f"v AS (SELECT quarter, {vexpr} AS value FROM s WINDOW w AS (PARTITION BY cu_number ORDER BY quarter)) "
            f"SELECT quarter, value FROM v WHERE {' AND '.join(rng)} ORDER BY quarter", [cu_number] + rng_p)
        if not rows:
            return [no_data_error(cu_number, hi)]
        return finish(rows, False)

    state, peer_group = check_state(state), check_peer_group(peer_group)
    where, p = ["is_federally_insured"], []
    if state:
        where.append("upper(state) = ?"); p.append(state)
    if peer_group is not None:
        where.append("peer_group = ?"); p.append(peer_group)
    if min_assets is not None:
        where.append("total_assets >= ?"); p.append(float(min_assets))
    agg = (aggregate or ("sum" if kind in SUMMABLE else "median")).lower()
    if agg not in AGGS:
        raise ValueError("aggregate must be one of: " + ", ".join(AGGS))
    if agg == "sum" and kind not in SUMMABLE:
        raise ValueError(f"'{field}' is a ratio or attribute, so summing it across credit unions is meaningless. Use "
                         "median or mean, pooled for the system-wide ratio, or ratio_of_sums with a denominator field.")
    gcol = f"{group_by}, " if group_by else ""
    gb = f"quarter, {group_by}" if group_by else "quarter"
    tail = f"GROUP BY {gb} ORDER BY quarter{', value DESC' if group_by else ''}"

    if agg == "pooled":
        if field in POOLED_GROWTH:
            cur = POOLED_GROWTH[field]
            ok = f"gap = 4 AND prior > 0"
            return finish(run(
                f"WITH s AS (SELECT quarter, cu_number, state, peer_group, {cur} AS cur, lag({cur}, 4) OVER w AS prior, "
                f"{IDX} - lag({IDX}, 4) OVER w AS gap FROM cu WHERE {' AND '.join(where)} "
                "WINDOW w AS (PARTITION BY cu_number ORDER BY quarter)) "
                f"SELECT quarter, {gcol}sum(CASE WHEN {ok} THEN cur END) / nullif(sum(CASE WHEN {ok} THEN prior END), 0) - 1 AS value, "
                f"count(CASE WHEN {ok} THEN 1 END) AS n_credit_unions FROM s WHERE {' AND '.join(rng)} {tail}",
                p + rng_p), True)
        if field in POOLED_NCUA:
            num, bal, mode = POOLED_NCUA[field]
            prior = (_PRIOR_DEC if mode == "ytd" else _PRIOR_Q).format(q="c.quarter")
            factor = "(4.0 / (cast(right(c.quarter, 2) AS INTEGER) / 3))" if mode == "ytd" else "4.0"
            gj = f" AND pr.{group_by} = c.{group_by}" if group_by else ""
            gsel = f"c.{group_by}, " if group_by else ""
            rows = run(
                f"WITH c AS (SELECT quarter, {gcol}{num} AS num, sum({bal}) AS bal, count(*) AS n FROM cu "
                f"WHERE {' AND '.join(where + rng)} GROUP BY {gb}), "
                f"pr AS (SELECT quarter, {gcol}sum({bal}) AS bal FROM cu WHERE {' AND '.join(where)} GROUP BY {gb}) "
                f"SELECT c.quarter, {gsel}c.num * {factor} / nullif((c.bal + pr.bal) / 2, 0) AS value, c.n AS n_credit_unions "
                f"FROM c LEFT JOIN pr ON pr.quarter = {prior}{gj} ORDER BY c.quarter{', value DESC' if group_by else ''}",
                p + rng_p + p)
            for r in rows:
                if r["value"] is None and r["n_credit_unions"]:
                    r["note"] = ("No value: this ratio needs the prior " + ("December" if mode == "ytd" else "quarter-end")
                                 + " balance, which is before the first quarter in the data (2018-03).")
            return finish(rows, True)
        if field not in POOLED:
            raise ValueError(f"No pooled version of '{field}'. Pooled is available for: "
                             + ", ".join(sorted(list(POOLED) + list(POOLED_NCUA) + list(POOLED_GROWTH))) + ".")
        return finish(run(
            f"SELECT quarter, {gcol}{POOLED[field]} AS value, count(*) AS n_credit_unions FROM cu "
            f"WHERE {' AND '.join(where + rng)} {tail}", p + rng_p), True)
    if agg == "ratio_of_sums":
        if not denominator:
            raise ValueError("ratio_of_sums needs denominator, e.g. field shares_certificates, denominator total_shares_and_deposits.")
        d = q(denominator)
        if kind not in SUMMABLE or KIND.get(denominator) not in SUMMABLE:
            raise ValueError("ratio_of_sums needs dollar or count fields for both field and denominator.")
        if is_ytd != (KIND.get(denominator) == "ytd"):
            raise ValueError("ratio_of_sums needs both fields on the same basis (both year-to-date or both balances).")
        return finish(run(
            f"SELECT quarter, {gcol}sum({f}) / nullif(sum({d}), 0) AS value, count({f}) AS n_credit_unions FROM cu "
            f"WHERE {' AND '.join(where + rng)} {tail}", p + rng_p), True)
    fn = {"median": "median(value)", "mean": "avg(value)", "sum": "sum(value)", "count": "count(value)"}[agg]
    return finish(run(
        f"WITH s AS (SELECT quarter, cu_number, state, peer_group, {f} AS raw FROM cu WHERE {' AND '.join(where)}), "
        f"v AS (SELECT quarter, cu_number, state, peer_group, {vexpr} AS value FROM s "
        "WINDOW w AS (PARTITION BY cu_number ORDER BY quarter)) "
        f"SELECT quarter, {gcol}{fn} AS value, count(value) AS n_credit_unions FROM v WHERE {' AND '.join(rng)} {tail}",
        p + rng_p), True)


PEER_BASES = {
    "peer_group": ["peer_group"],
    "state": ["state"],
    "charter_type": ["charter_type"],
    "peer_group_and_state": ["peer_group", "state"],
}


@mcp.tool(
    description=(
        "Compare one credit union with its peers on chosen metrics or fields (default: a standard set). "
        "peer_basis: 'peer_group' (same asset-size group, default), 'state', 'charter_type', or 'peer_group_and_state'. "
        "Returns the credit union's value, peer median, 25th/75th percentiles, percentile rank (0-100, higher = larger value) "
        "and peer count, for the quarter (default latest). Metrics: " + METRIC_CATALOG
    )
)
def peer_compare(
    cu_number: int,
    metrics: Optional[list[str]] = None,
    peer_basis: str = "peer_group",
    quarter: Optional[str] = None,
) -> dict:
    qt = _quarter(quarter)
    if peer_basis not in PEER_BASES:
        raise ValueError(f"peer_basis must be one of {list(PEER_BASES)}")
    metrics = metrics or [
        "roa_avg_assets_4q", "net_chargeoff_rate_avg_loans_4q", "delinquency_rate", "loan_to_share",
        "net_worth_ratio_ex_cecl", "efficiency_ratio", "members_per_fte", "operating_expense_per_fte",
    ]
    me = run("SELECT * FROM cu WHERE cu_number = ? AND quarter = ?", [cu_number, qt])
    if not me:
        return no_data_error(cu_number, qt)
    me = me[0]
    cols = PEER_BASES[peer_basis]
    where = ["quarter = ?", "is_federally_insured"] + [f"{c} = ?" for c in cols]
    p = [qt] + [me[c] for c in cols]
    out = []
    for m in metrics[:20]:
        f = q(m)
        s = run(
            f"SELECT median({f}) med, quantile_cont({f}, 0.25) p25, quantile_cont({f}, 0.75) p75, "
            f"count({f}) n, sum(CASE WHEN {f} < ? THEN 1 ELSE 0 END) below FROM cu WHERE {' AND '.join(where)}",
            [me[m]] + p,
        )[0]
        pct = None if me[m] is None or not s["n"] else round(100 * s["below"] / s["n"], 1)
        row = {"field": m, "value": me[m], "peer_median": s["med"], "peer_p25": s["p25"], "peer_p75": s["p75"],
               "percentile_rank": pct, "peers": s["n"]}
        if KIND.get(m) == "ytd":
            row["basis"] = "year_to_date"
        if KIND.get(m) == "pct":
            row["unit"] = "hundredths of a percent (1165 = 11.65%)"
        if m in FIELD_NOTES:
            row["note"] = FIELD_NOTES[m]
        out.append(row)
    return {
        "credit_union": {"cu_number": cu_number, "name": me["name"], "state": me["state"], "peer_group": me["peer_group_label"]},
        "quarter": qt,
        "peer_basis": {c: me[c] for c in cols},
        "comparisons": out,
    }


OPS = {"=": "=", "!=": "!=", ">": ">", ">=": ">=", "<": "<", "<=": "<="}
NUMERIC_ATTRS = {"cu_number", "rssd", "county_code", "peer_group", "year_opened"}
DEFAULT_RANK_FLOOR = 10_000_000.0


def _coerce(field: str, v: Any) -> Any:
    """Check a filter value fits the field, so bad input gives a clear error instead of a raw database one."""
    if v is None:
        raise ValueError(f"Filter on '{field}' needs a value.")
    kind = KIND.get(field)
    if kind == "attribute" and field not in NUMERIC_ATTRS:
        if field.startswith("is_"):
            if isinstance(v, bool):
                return v
            raise ValueError(f"'{field}' is true/false; use true or false, got {v!r}.")
        return str(v)
    if isinstance(v, bool):
        raise ValueError(f"'{field}' is numeric; got {v!r}.")
    try:
        return float(v)
    except (TypeError, ValueError):
        raise ValueError(f"'{field}' is numeric, so the filter value must be a number, got {v!r}.")


@mcp.tool(
    description=(
        "Constrained table query, one quarter at a time (default latest), federally insured credit unions only. "
        "Pick fields to return, optional filters as [{'field':..., 'op': one of = != > >= < <= in contains, 'value':...}], "
        f"an order_by field and limit (1 to {MAX_ROWS}). If more rows match than the limit, the last row is "
        "{'truncated': true, ...}. No raw SQL. When order_by is set, credit unions under $10M in assets are left out "
        "by default, because tiny credit unions produce extreme ratios (a $1M credit union can show a 34% ROA); rows "
        "carry assets_floor_applied. Set min_assets (0 to include everyone) to change it. Year-to-date fields "
        "(basis year_to_date in list_fields) are cumulative since January; use the *_quarter metrics for single "
        "quarters. Ratios are fractions (0.05 = 5%). Example: top 10 by members_per_fte in peer_group 5. Results over limit end with a truncated row giving total_matching and next_offset; pass offset to page. "
        "Field names come from list_fields. Metrics: " + METRIC_CATALOG
    )
)
def query_metrics(
    fields: list[str],
    filters: Optional[list[dict]] = None,
    order_by: Optional[str] = None,
    descending: bool = True,
    limit: int = 25,
    quarter: Optional[str] = None,
    min_assets: Optional[float] = None,
    quarterly: Optional[bool] = None,
    offset: int = 0,
) -> list[dict]:
    if limit < 1 or limit > MAX_ROWS:
        raise ValueError(f"limit must be between 1 and {MAX_ROWS}, got {limit}.")
    if offset < 0:
        raise ValueError(f"offset must be 0 or more, got {offset}.")
    if min_assets is not None and min_assets < 0:
        raise ValueError(f"min_assets must be 0 or more, got {min_assets}.")
    cols = ["cu_number", "name"] + [f for f in fields if f not in ("cu_number", "name")]
    sel = ", ".join(q(c) for c in cols[:40])
    # Year-to-date fields are returned as per-quarter figures by default (quarterly=False gives raw year-to-date).
    used = list(cols[:40]) + ([order_by] if order_by else []) + [fl.get("field") for fl in (filters or []) if isinstance(fl, dict)]
    ytd_used = sorted({c for c in used if c in ALL_FIELDS and KIND.get(c) == "ytd"})
    if quarterly is True and not ytd_used:
        raise ValueError("quarterly applies only to year-to-date fields; none of the requested fields is one.")
    use_q = bool(ytd_used) and quarterly is not False
    src = "cu"
    if use_q:
        repl = ", ".join(
            f"CASE WHEN right(quarter, 2) = '03' THEN {q(c)} WHEN {IDX} - lag({IDX}) OVER w = 1 "
            f"THEN {q(c)} - lag({q(c)}) OVER w END AS {q(c)}" for c in ytd_used)
        excl = ", ".join(q(c) for c in ytd_used)
        src = (f"(SELECT * EXCLUDE ({excl}), {repl} FROM cu "
               "WINDOW w AS (PARTITION BY cu_number ORDER BY quarter))")
    where, p = ["quarter = ?", "is_federally_insured"], [_quarter(quarter)]
    floor = min_assets if min_assets is not None else (DEFAULT_RANK_FLOOR if order_by else 0.0)
    if floor and floor > 0:
        where.append("total_assets >= ?"); p.append(float(floor))
    for flt in filters or []:
        if "field" not in flt:
            raise ValueError("Each filter needs a 'field'.")
        fname = flt["field"]
        f, op = q(fname), str(flt.get("op", "=")).lower()
        v = flt.get("value")
        if flt["field"] == "state" and op in ("=", "!=", "in"):
            v = [check_state(x) for x in v] if isinstance(v, list) else check_state(v)
        if op == "in":
            vals = [_coerce(fname, x) for x in (v if isinstance(v, list) else [v])]
            where.append(f"{f} IN ({', '.join('?' for _ in vals)})"); p.extend(vals)
        elif op == "contains":
            if v is None:
                raise ValueError(f"Filter on '{fname}' needs a value.")
            where.append(f"upper(CAST({f} AS VARCHAR)) LIKE ?"); p.append(f"%{str(v).upper()}%")
        elif op in OPS:
            where.append(f"{f} {OPS[op]} ?"); p.append(_coerce(fname, v))
        else:
            raise ValueError("op must be one of = != > >= < <= in contains")
    order = f"ORDER BY {q(order_by)} {'DESC' if descending else 'ASC'} NULLS LAST" if order_by else ""
    rows = run(f"SELECT {sel} FROM {src} t WHERE {' AND '.join(where)} {order}, cu_number LIMIT {limit + 1} OFFSET {int(offset)}"
               if order else
               f"SELECT {sel} FROM {src} t WHERE {' AND '.join(where)} ORDER BY cu_number LIMIT {limit + 1} OFFSET {int(offset)}", p)
    more = len(rows) > limit
    rows = rows[:limit]
    if order_by and floor and floor > 0:
        for r in rows:
            r["assets_floor_applied"] = floor
    elif order_by and min_assets == 0:
        for r in rows:
            r["assets_floor_applied"] = 0.0
            r["floor_note"] = "No asset floor (min_assets=0): very small credit unions can top rankings."
    if ytd_used:
        for r in rows:
            r["basis"] = {c: ("quarterly" if use_q else "year_to_date") for c in ytd_used if c in r}
        if rows and not use_q:
            rows[0]["warning"] = YTD_WARNING
        if rows and use_q:
            rows[0]["basis_note"] = ("Year-to-date fields are shown as per-quarter figures (this quarter minus the "
                                     "prior quarter of the same year). Set quarterly=false for raw year-to-date.")
    if more:
        total = run(f"SELECT count(*) AS n FROM {src} t WHERE {' AND '.join(where)}", p)[0]["n"]
        nxt = offset + limit
        rows.append({"truncated": True, "total_matching": total, "next_offset": nxt,
                     "message": f"Showing rows {offset + 1}-{offset + limit} of {total}. Call again with offset={nxt} "
                                f"for the next page (limit max {MAX_ROWS}), or narrow the filters."})
    return rows


class RateLimit:
    """Per-client sliding-window limit for the hosted endpoint. Pure ASGI, so it stays in front of the MCP app."""

    def __init__(self, app, per_minute: int):
        self.app, self.per_minute, self.hits = app, per_minute, {}

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and scope["path"] != "/healthz":
            h = dict(scope["headers"])
            ip = (h.get(b"fly-client-ip") or h.get(b"x-forwarded-for", b"").split(b",")[0] or b"?").decode().strip()
            now = time.time()
            q_ = [t for t in self.hits.get(ip, []) if now - t < 60]
            if len(q_) >= self.per_minute:
                body = b'{"error":"rate limit exceeded, try again in a minute"}'
                await send({"type": "http.response.start", "status": 429,
                            "headers": [(b"content-type", b"application/json"), (b"retry-after", b"60")]})
                await send({"type": "http.response.body", "body": body})
                return
            q_.append(now)
            self.hits[ip] = q_
            if len(self.hits) > 5000:
                self.hits = {k: v for k, v in self.hits.items() if v and now - v[-1] < 60}
        await self.app(scope, receive, send)


def _forbid_extra_arguments() -> None:
    """Reject unknown tool arguments instead of silently ignoring them."""
    for t in mcp._tool_manager.list_tools():
        model = t.fn_metadata.arg_model
        model.model_config["extra"] = "forbid"
        model.model_rebuild(force=True)


_forbid_extra_arguments()


def http_app():
    """ASGI app for the hosted, stateless streamable-HTTP endpoint at /mcp."""
    from mcp.server.transport_security import TransportSecuritySettings
    from starlette.responses import JSONResponse

    hosts = [h for h in os.environ.get("NCUA_ALLOWED_HOSTS", "").split(",") if h]
    mcp.settings.stateless_http = True
    mcp.settings.json_response = True
    mcp.settings.transport_security = TransportSecuritySettings(
        enable_dns_rebinding_protection=bool(hosts), allowed_hosts=hosts, allowed_origins=["*"] if hosts else [])

    @mcp.custom_route("/healthz", methods=["GET"])
    async def healthz(request):
        return JSONResponse({"ok": True, "latest_quarter": latest_quarter()})

    return RateLimit(mcp.streamable_http_app(), int(os.environ.get("NCUA_RATE_PER_MIN", "60")))


def main() -> None:
    argv = sys.argv[1:]
    if "--check" in argv:
        print(f"{len(METRIC_NAMES)} metrics, {len(FACT_NAMES)} fact fields, data dir {data_dir()}")
        ensure_files()
        print("latest quarter", latest_quarter())
        return
    if "--http" in argv:
        import uvicorn
        uvicorn.run(http_app(), host=os.environ.get("HOST", "0.0.0.0"), port=int(os.environ.get("PORT", "8080")),
                    log_level="warning", timeout_keep_alive=5)
        return
    mcp.run()


if __name__ == "__main__":
    main()
