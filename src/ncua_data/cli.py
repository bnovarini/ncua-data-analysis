"""Command line: download, build, reconcile."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from . import download as dl
from .build import build_all
from .dictionary import build_dictionary
from .ingest import ingest_quarter
from .reconcile import run as reconcile


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="ncua-data")
    ap.add_argument("--data", type=Path, default=Path("data"))
    ap.add_argument("--since", type=int, default=2018, help="first year to download")
    ap.add_argument("command", choices=["download", "ingest", "build", "reconcile", "all"])
    a = ap.parse_args(argv)
    cmds = ["download", "ingest", "build", "reconcile"] if a.command == "all" else [a.command]
    if "download" in cmds:
        dl.download(dl.list_quarters(a.since), a.data / "raw")
    if "ingest" in cmds:
        for z in sorted((a.data / "raw").glob("*.zip")):
            q = z.stem.replace("call-report-data-", "")
            if not (a.data / "long" / f"{q}_dict.parquet").exists():
                print(ingest_quarter(z, a.data / "work", a.data / "long"))
    if "build" in cmds or "reconcile" in cmds:
        con = build_all(a.data)
        if "build" in cmds:
            build_dictionary(con)
            out = a.data / "out"
            out.mkdir(exist_ok=True)
            for t in ["dim_credit_union", "fact_call_report_curated", "metrics", "dictionary"]:
                con.execute(f"COPY {t} TO '{out}/{t}.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)")
            print("wrote", sorted(p.name for p in out.glob("*.parquet")))
        if "reconcile" in cmds:
            res = reconcile(con)
            (a.data / "out").mkdir(exist_ok=True)
            (a.data / "out" / "reconciliation.json").write_text(json.dumps(res, indent=2))
            bad = [r for r in res if not r["ok"]]
            print(f"{len(res) - len(bad)}/{len(res)} checks within tolerance")
            for r in bad:
                print("MISMATCH", r["quarter"], r["check"], r["dataset"], "vs published", r["published"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
