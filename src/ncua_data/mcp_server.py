"""Local stdio MCP server over the published NCUA credit union dataset.

Data comes from the GitHub release parquet files, cached on first use. DuckDB does
the querying. Tool descriptions are generated from the dictionary table.
"""
from __future__ import annotations

import json
import os
import re
import sys
import threading
import time
import urllib.request
from pathlib import Path
from typing import Any, Optional

import duckdb
from mcp.server.fastmcp import FastMCP

RELEASE = "v0.1.1"
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
        c.execute(f"CREATE VIEW met AS SELECT * FROM read_parquet('{g('metrics_*.parquet')}')")
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


def latest_quarter() -> str:
    return run("SELECT max(quarter) q FROM dim")[0]["q"]


def q(name: str) -> str:
    if name not in ALL_FIELDS:
        raise ValueError(f"Unknown field '{name}'. Use list_fields to see valid names.")
    return f'"{name}"'


def _quarter(quarter: Optional[str]) -> str:
    if not quarter:
        return latest_quarter()
    if not (len(quarter) == 7 and quarter[4] == "-" and quarter[5:] in ("03", "06", "09", "12")):
        raise ValueError("quarter must look like 2025-12 (quarter ends: 03, 06, 09, 12)")
    return quarter


NOTE = (
    "Data: NCUA 5300 call reports, federally insured credit unions, 2018-03 to 2026-06, "
    "from github.com/bnovarini/ncua-data-analysis. Dollar fields are in dollars; ratio fields "
    "are fractions unless the field description says otherwise."
)

mcp = FastMCP("ncua-data-analysis", instructions=NOTE)


@mcp.tool(
    description=(
        "List available fields with plain-language definitions from the data dictionary. "
        "Filter by table ('metrics' = computed ratios, 'fact_call_report_curated' = reported amounts, "
        "'dim_credit_union' = attributes), by search text, or both. "
        f"Computed metrics available: {METRIC_CATALOG}"
    )
)
def list_fields(table: Optional[str] = None, search: Optional[str] = None, limit: int = 60) -> list[dict]:
    rows = []
    for t, c, d, k, cat in _DICT:
        if table and t != table:
            continue
        if search and search.lower() not in f"{c} {d} {cat or ''}".lower():
            continue
        rows.append({"table": t, "field": c, "kind": k, "category": cat, "description": d})
    return rows[: max(1, min(limit, MAX_ROWS))]


_STOP = r"\b(federal|credit|union|fcu|cu|the)\b"
_NORM = (
    "trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(lower(name), '[.'']', '', 'g'), '[^a-z0-9 ]', ' ', 'g'), '" + _STOP + "', ' ', 'g'), '\s+', ' ', 'g'))"
)


def normalize_name(text: str) -> str:
    """Lowercase, drop punctuation and the words federal/credit/union/fcu/cu/the, collapse spaces."""
    t = re.sub(r"[^a-z0-9 ]", " ", re.sub(r"[.']", "", text.lower()))
    t = re.sub(_STOP, " ", t)
    return re.sub(r"\s+", " ", t).strip()


@mcp.tool(
    description=(
        "Find credit unions by name, state, charter type (federal/state) or asset peer group, in a given quarter "
        "(default: latest). Name matching ignores case, punctuation and the words 'federal credit union' / 'FCU', "
        "so 'SRP Federal Credit Union' finds 'SRP'; partial names work. Results are ranked: match = exact, "
        "starts_with, then contains, then larger assets first. When several rows come back and none is an exact "
        "match, the name is ambiguous: ask the user which one rather than picking the first. Returns cu_number, "
        "name, location, total assets, members. Use the returned cu_number in the other tools. "
        "Only federally insured credit unions."
    )
)
def find_credit_union(
    name: Optional[str] = None,
    state: Optional[str] = None,
    charter_type: Optional[str] = None,
    peer_group: Optional[int] = None,
    quarter: Optional[str] = None,
    limit: int = 15,
) -> list[dict]:
    where, p = ["is_federally_insured"], []
    where.append("quarter = ?"); p.append(_quarter(quarter))
    rank_sql, rank_p = "2", []
    if name and name.strip():
        qn = normalize_name(name)
        if qn:
            for tok in qn.split():
                where.append(f"contains({_NORM}, ?)"); p.append(tok)
            rank_sql = f"CASE WHEN {_NORM} = ? THEN 0 WHEN starts_with({_NORM}, ?) THEN 1 ELSE 2 END"
            rank_p = [qn, qn]
        else:  # only filler words, e.g. "federal credit union": fall back to the raw text
            where.append("upper(name) LIKE ?"); p.append(f"%{name.strip().upper()}%")
    if state:
        where.append("upper(state) = ?"); p.append(state.upper())
    if charter_type:
        where.append("lower(charter_type) = ?"); p.append(charter_type.lower())
    if peer_group is not None:
        where.append("peer_group = ?"); p.append(peer_group)
    rows = run(
        f"SELECT cu_number, name, city, state, charter_type, peer_group_label, total_assets, members, {rank_sql} AS _rank "
        f"FROM cu WHERE {' AND '.join(where)} ORDER BY _rank, total_assets DESC NULLS LAST LIMIT {max(1, min(limit, 50))}",
        rank_p + p,
    )
    labels = {0: "exact", 1: "starts_with", 2: "contains"}
    for r in rows:
        r["match"] = labels[r.pop("_rank")] if name and name.strip() else None
    return rows


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
        return {"error": f"No data for cu_number {cu_number} in {qt}. It may have merged, closed, or not be federally insured."}
    r = rows[0]
    keep_fact = ["total_assets", "loans_and_leases_total", "total_shares", "members", "net_worth", "net_income_ytd"]
    return {
        "attributes": {k: r[k] for k in DIM_NAMES if k in r},
        "quarter": qt,
        "key_amounts": {k: r[k] for k in keep_fact if k in r},
        "metrics": {k: r[k] for k in METRIC_NAMES if k in r},
        "note": NOTE,
    }


@mcp.tool(
    description=(
        "Time series of one metric or reported field. Give cu_number for one credit union. Otherwise it aggregates "
        "all federally insured credit unions matching the optional state / peer_group filters: aggregate='median' "
        "(default for ratios), 'mean', or 'sum' (default for dollar and count fields), plus the number of credit unions. "
        "Range in quarters like 2021-12. Metrics: " + METRIC_CATALOG
    )
)
def metric_series(
    field: str,
    cu_number: Optional[int] = None,
    state: Optional[str] = None,
    peer_group: Optional[int] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    aggregate: Optional[str] = None,
    year_end_only: bool = False,
) -> list[dict]:
    f = q(field)
    where, p = ["quarter >= ?", "quarter <= ?"], [start or "2018-03", end or latest_quarter()]
    if year_end_only:
        where.append("right(quarter, 2) = '12'")
    if cu_number is not None:
        where.append("cu_number = ?"); p.append(cu_number)
        return run(f"SELECT quarter, {f} AS value FROM cu WHERE {' AND '.join(where)} ORDER BY quarter", p)
    where.append("is_federally_insured")
    if state:
        where.append("upper(state) = ?"); p.append(state.upper())
    if peer_group is not None:
        where.append("peer_group = ?"); p.append(peer_group)
    agg = (aggregate or ("sum" if KIND.get(field) in ("dollars", "stock", "count", "ytd") else "median")).lower()
    fn = {"median": f"median({f})", "mean": f"avg({f})", "sum": f"sum({f})"}.get(agg)
    if not fn:
        raise ValueError("aggregate must be median, mean or sum")
    return run(
        f"SELECT quarter, {fn} AS value, count({f}) AS n_credit_unions FROM cu WHERE {' AND '.join(where)} "
        "GROUP BY quarter ORDER BY quarter",
        p,
    )


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
        return {"error": f"No data for cu_number {cu_number} in {qt}."}
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
        out.append({"field": m, "value": me[m], "peer_median": s["med"], "peer_p25": s["p25"], "peer_p75": s["p75"],
                    "percentile_rank": pct, "peers": s["n"]})
    return {
        "credit_union": {"cu_number": cu_number, "name": me["name"], "state": me["state"], "peer_group": me["peer_group_label"]},
        "quarter": qt,
        "peer_basis": {c: me[c] for c in cols},
        "comparisons": out,
    }


OPS = {"=": "=", "!=": "!=", ">": ">", ">=": ">=", "<": "<", "<=": "<="}


@mcp.tool(
    description=(
        "Constrained table query, one quarter at a time (default latest), federally insured credit unions only. "
        "Pick fields to return, optional filters as [{'field':..., 'op': one of = != > >= < <= in contains, 'value':...}], "
        f"an order_by field and limit (max {MAX_ROWS}). No raw SQL. Example: top 10 by members_per_fte in peer_group 5. "
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
) -> list[dict]:
    cols = ["cu_number", "name"] + [f for f in fields if f not in ("cu_number", "name")]
    sel = ", ".join(q(c) for c in cols[:40])
    where, p = ["quarter = ?", "is_federally_insured"], [_quarter(quarter)]
    for flt in filters or []:
        f, op, v = q(flt["field"]), str(flt.get("op", "=")).lower(), flt.get("value")
        if op == "in":
            vals = v if isinstance(v, list) else [v]
            where.append(f"{f} IN ({', '.join('?' for _ in vals)})"); p.extend(vals)
        elif op == "contains":
            where.append(f"upper(CAST({f} AS VARCHAR)) LIKE ?"); p.append(f"%{str(v).upper()}%")
        elif op in OPS:
            where.append(f"{f} {OPS[op]} ?"); p.append(v)
        else:
            raise ValueError("op must be one of = != > >= < <= in contains")
    order = f"ORDER BY {q(order_by)} {'DESC' if descending else 'ASC'} NULLS LAST" if order_by else ""
    return run(f"SELECT {sel} FROM cu WHERE {' AND '.join(where)} {order} LIMIT {max(1, min(limit, MAX_ROWS))}", p)


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
                            "headers": [(b"content-type", b"application/json"), (b"retry-after", b"30")]})
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
