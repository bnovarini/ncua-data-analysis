"""Load raw NCUA quarterly ZIPs into long-format Parquet files.

Every account value becomes one row (quarter, cu_number, acct, value), keeping
non-zero values only. That makes the multi-year account churn easy to handle
and keeps the intermediate store small.
"""
from __future__ import annotations

import csv
import re
import zipfile
from pathlib import Path

import duckdb

ACCT_TABLE = re.compile(r"^fs220[a-z]?\.txt$", re.I)
ID_COLS = ("CU_NUMBER", "CYCLE_DATE", "JOIN_NUMBER", "UPDATE_DATE")


def extract(zip_path: Path, dest: Path) -> Path:
    out = dest / zip_path.stem.replace("call-report-data-", "")
    if not out.exists():
        out.mkdir(parents=True)
        with zipfile.ZipFile(zip_path) as z:
            z.extractall(out)
        # NCUA files are Windows-1252 (stray 0x92 apostrophes appear in free text
        # fields). Re-encode to UTF-8 so every reader agrees.
        for f in out.iterdir():
            if f.suffix.lower() == ".txt":
                f.write_bytes(f.read_bytes().decode("cp1252", errors="replace").encode("utf-8"))
    return out


def _find(folder: Path, name: str) -> Path | None:
    for p in folder.iterdir():
        if p.name.lower() == name.lower():
            return p
    return None


def _unique_header(path: Path) -> list[str]:
    """Header row with case-insensitive duplicates suffixed (some files repeat a code)."""
    with open(path, encoding="utf-8", newline="") as fh:
        header = next(csv.reader(fh))
    seen: dict[str, int] = {}
    out = []
    for h in header:
        k = h.upper()
        seen[k] = seen.get(k, 0) + 1
        out.append(h if seen[k] == 1 else f"{h}__dup{seen[k]}")
    return out


def ingest_quarter(zip_path: Path, work: Path, out_dir: Path) -> dict:
    """Write long.parquet, foicu.parquet and dict.parquet for one quarter."""
    quarter = zip_path.stem.replace("call-report-data-", "")
    folder = extract(zip_path, work)
    out_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    parts = []
    for p in sorted(folder.iterdir()):
        if ACCT_TABLE.match(p.name):
            names = _unique_header(p)
            parts.append(
                f"""SELECT CU_NUMBER::INTEGER AS cu_number, upper(acct) AS acct,
                           try_cast(val AS DOUBLE) AS value, '{p.stem.upper()}' AS source_table
                    FROM (UNPIVOT (SELECT * FROM read_csv('{p}', all_varchar=true, header=true,
                                   names={names!r}, ignore_errors=true))
                          ON COLUMNS('(?i)^acct_')
                          INTO NAME acct VALUE val)
                    WHERE try_cast(val AS DOUBLE) <> 0"""
            )
    con.execute(
        f"COPY (SELECT '{quarter}' AS quarter, * FROM ({' UNION ALL '.join(parts)})) "
        f"TO '{out_dir}/{quarter}_long.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    foicu = _find(folder, "FOICU.txt")
    con.execute(
        f"COPY (SELECT '{quarter}' AS quarter, * FROM read_csv('{foicu}', all_varchar=true, "
        f"ignore_errors=true)) TO '{out_dir}/{quarter}_foicu.parquet' (FORMAT PARQUET)"
    )
    ad = _find(folder, "AcctDesc.txt")
    con.execute(
        f"COPY (SELECT '{quarter}' AS quarter, * FROM read_csv('{ad}', all_varchar=true, "
        f"ignore_errors=true, strict_mode=false)) TO '{out_dir}/{quarter}_dict.parquet' (FORMAT PARQUET)"
    )
    n = con.execute(f"SELECT count(*), count(DISTINCT cu_number), count(DISTINCT acct) FROM '{out_dir}/{quarter}_long.parquet'").fetchone()
    return {"quarter": quarter, "rows": n[0], "cus": n[1], "accts": n[2]}
