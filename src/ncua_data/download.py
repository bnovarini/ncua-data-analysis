"""Find and download NCUA quarterly call report ZIPs.

Links are scraped from NCUA's quarterly data page rather than built from a
pattern, because the file naming has changed over the years.
"""
from __future__ import annotations

import re
import sys
import urllib.request
from pathlib import Path

BASE = "https://ncua.gov"
PAGE = BASE + "/analysis/credit-union-corporate-call-report-data/quarterly-data"
UA = {"User-Agent": "ncua-data-analysis (open source research project)"}

_LINK = re.compile(r'href="(/files/publications/analysis/call-report-data-(\d{4})-(\d{2})\.zip)"', re.I)


def _get(url: str) -> bytes:
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=120) as r:
        return r.read()


def list_quarters(min_year: int = 2018) -> dict[str, str]:
    """Return {'2026-06': url} for every quarter on the page since min_year."""
    html = _get(PAGE).decode("utf-8", "replace")
    out: dict[str, str] = {}
    for path, y, m in _LINK.findall(html):
        if int(y) >= min_year:
            out[f"{y}-{m}"] = BASE + path
    return dict(sorted(out.items()))


def download(quarters: dict[str, str], dest: Path) -> list[Path]:
    dest.mkdir(parents=True, exist_ok=True)
    paths = []
    for q, url in quarters.items():
        p = dest / f"call-report-data-{q}.zip"
        if not p.exists() or p.stat().st_size == 0:
            print(f"downloading {q}", file=sys.stderr)
            p.write_bytes(_get(url))
        paths.append(p)
    return paths
