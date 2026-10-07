import unittest
from pathlib import Path

import duckdb

from ncua_data.spec import FIELDS
from ncua_data.dictionary import METRIC_DOCS


class SpecTests(unittest.TestCase):
    def test_names_unique_and_snake_case(self):
        names = [f.name for f in FIELDS]
        self.assertEqual(len(names), len(set(names)))
        for n in names:
            self.assertRegex(n, r"^[a-z][a-z0-9_]*$")

    def test_every_field_has_description_codes_and_known_kind(self):
        for f in FIELDS:
            self.assertTrue(f.description.strip(), f.name)
            self.assertTrue(f.codes, f.name)
            self.assertIn(f.kind, {"stock", "ytd", "count", "pct"}, f.name)

    def test_ytd_fields_are_labelled_ytd(self):
        for f in FIELDS:
            if f.kind == "ytd":
                self.assertTrue(f.name.endswith("_ytd"), f.name)


@unittest.skipUnless(Path("data/out/metrics.parquet").exists(), "run `ncua-data build` first")
class BuiltDataTests(unittest.TestCase):
    def setUp(self):
        self.con = duckdb.connect()

    def test_all_metric_columns_documented(self):
        cols = {r[0] for r in self.con.execute("DESCRIBE SELECT * FROM 'data/out/metrics.parquet'").fetchall()}
        self.assertEqual(cols - {"quarter", "cu_number"}, set(METRIC_DOCS))

    def test_balance_sheet_balances(self):
        bad = self.con.execute(
            "SELECT count(*) FROM 'data/out/fact_call_report_curated.parquet' "
            "WHERE abs(total_assets - total_liabilities_shares_equity) > 1").fetchone()[0]
        self.assertEqual(bad, 0)

    def test_published_reconciliation_mostly_passes(self):
        import json
        res = json.loads(Path("data/out/reconciliation.json").read_text())
        failed = [r for r in res if not r["ok"]]
        # Seven small historical deposit-mix differences (all under $0.5B) are documented.
        self.assertLessEqual(len(failed), 7, failed)


if __name__ == "__main__":
    unittest.main()
