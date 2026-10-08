import os
import unittest
from pathlib import Path

import duckdb

os.environ.setdefault("NCUA_DATA_DIR", os.environ.get("NCUA_TEST_DATA_DIR", "/tmp/ncua-test-data"))

from ncua_data import mcp_server as srv  # noqa: E402

HAVE_DATA = all((Path(os.environ["NCUA_DATA_DIR"]) / f).exists() for f in srv.FILES)


class NameNormalizingTests(unittest.TestCase):
    def test_filler_words_and_case_and_punctuation_are_dropped(self):
        self.assertEqual(srv.normalize_name("SRP Federal Credit Union"), "srp")
        self.assertEqual(srv.normalize_name("  navy FEDERAL  c.u. "), "navy")
        self.assertEqual(srv.normalize_name("Digital Federal Credit Union (DCU)"), "digital dcu")
        self.assertEqual(srv.normalize_name("The Golden 1 Credit Union"), "golden 1")

    def test_only_filler_words_normalizes_to_empty(self):
        self.assertEqual(srv.normalize_name("Federal Credit Union"), "")


class StrictArgumentTests(unittest.TestCase):
    def test_every_tool_rejects_unknown_arguments(self):
        tools = srv.mcp._tool_manager.list_tools()
        self.assertGreaterEqual(len(tools), 6)
        for t in tools:
            with self.assertRaises(Exception, msg=t.name) as cm:
                t.fn_metadata.arg_model.model_validate({"definitely_not_an_argument": 1})
            self.assertIn("definitely_not_an_argument", str(cm.exception), t.name)

    def test_misspelled_argument_is_not_silently_ignored(self):
        t = srv.mcp._tool_manager.get_tool("find_credit_union")
        with self.assertRaises(Exception):
            t.fn_metadata.arg_model.model_validate({"query": "Navy Federal"})  # the argument is called name
        t.fn_metadata.arg_model.model_validate({"name": "Navy Federal"})  # valid ones still pass


@unittest.skipUnless(HAVE_DATA, "needs the built parquet files in NCUA_DATA_DIR")
class FindCreditUnionTests(unittest.TestCase):
    def test_suffix_and_case_tolerant(self):
        for q in ("SRP Federal Credit Union", "srp", "SRP FCU", "S.R.P."):
            rows = srv.find_credit_union(name=q)
            self.assertTrue(rows and rows[0]["cu_number"] == 24410, q)
            self.assertEqual(rows[0]["match"], "exact", q)

    def test_partial_name_returns_ranked_candidates(self):
        rows = srv.find_credit_union(name="navy")
        self.assertGreaterEqual(len(rows), 1)
        self.assertEqual(rows[0]["cu_number"], 5536)  # Navy Federal: exact normalized match first
        ranks = [{"exact": 0, "starts_with": 1, "contains": 2}[r["match"]] for r in rows]
        self.assertEqual(ranks, sorted(ranks))

    def test_ambiguous_name_lists_all_matches_not_one(self):
        rows = srv.find_credit_union(name="community")
        self.assertGreater(len(rows), 3)  # every candidate is returned, ranked, not just the first

    def test_no_match_is_empty_and_filler_only_does_not_match_everything_silently(self):
        self.assertEqual(srv.find_credit_union(name="zzzz not a credit union"), [])
        self.assertLessEqual(len(srv.find_credit_union(name="Federal Credit Union", limit=5)), 5)

    def test_filters_without_name_still_work(self):
        rows = srv.find_credit_union(state="SC", limit=3)
        self.assertTrue(all(r["state"] == "SC" and r["match"] is None for r in rows))


@unittest.skipUnless(HAVE_DATA, "needs the built parquet files in NCUA_DATA_DIR")
class VehicleCountTests(unittest.TestCase):
    def test_fields_documented(self):
        names = {r["field"] for r in srv.list_fields(search="vehicle")}
        self.assertTrue({"loans_new_vehicle_count", "loans_used_vehicle_count"} <= names)

    def test_srp_counts_match_raw_ncua_accounts(self):
        # Raw FS220 Acct_958 / Acct_968 for CU 24410 at 2025-12, read straight from the long file.
        long = Path("data/long/2025-12_long.parquet")
        if not long.exists():
            self.skipTest("raw long file not present")
        raw = dict(duckdb.connect().execute(
            f"SELECT acct, value FROM read_parquet('{long}') WHERE cu_number = 24410 AND acct IN ('ACCT_958','ACCT_968')").fetchall())
        got = srv.metric_series("loans_new_vehicle_count", cu_number=24410, start="2025-12", end="2025-12")
        got2 = srv.metric_series("loans_used_vehicle_count", cu_number=24410, start="2025-12", end="2025-12")
        self.assertEqual(got[0]["value"], raw["ACCT_958"])
        self.assertEqual(got2[0]["value"], raw["ACCT_968"])

    def test_vehicle_counts_never_exceed_total_loan_count(self):
        bad = srv.run(
            "SELECT count(*) n FROM cu WHERE quarter = '2025-12' AND is_federally_insured "
            "AND loans_new_vehicle_count + loans_used_vehicle_count > loans_and_leases_count")[0]["n"]
        self.assertEqual(bad, 0)

    def test_count_and_dollar_balance_agree_on_presence(self):
        bad = srv.run(
            "SELECT count(*) n FROM cu WHERE quarter = '2025-12' AND is_federally_insured "
            "AND ((loans_new_vehicle > 0) <> (loans_new_vehicle_count > 0))")[0]["n"]
        self.assertLessEqual(bad, 5)


if __name__ == "__main__":
    unittest.main()
