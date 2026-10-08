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
        ranks = [{"exact": 0, "starts_with": 1, "contains": 2, "former_name": 3}[r["match"]] for r in rows]
        self.assertEqual(ranks, sorted(ranks))

    def test_ambiguous_name_lists_all_matches_not_one(self):
        rows = srv.find_credit_union(name="community")
        self.assertGreater(len(rows), 3)  # every candidate is returned, ranked, not just the first

    def test_no_match_is_empty_and_filler_only_does_not_match_everything_silently(self):
        rows = srv.find_credit_union(name="zzzz not a credit union")
        self.assertEqual([list(r) for r in rows], [["no_match"]])
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


@unittest.skipUnless(HAVE_DATA, "test data not downloaded")
class BreakTestRegressions(unittest.TestCase):
    """Found by running realistic agent prompts against the hosted v0.1.1 server."""

    def test_renamed_credit_union_found_by_former_name(self):
        rows = srv.find_credit_union(name="Bethpage")
        self.assertEqual([r["cu_number"] for r in rows], [4735])
        self.assertEqual(rows[0]["match"], "former_name")
        self.assertEqual(rows[0]["matched_former_name"], "BETHPAGE")
        self.assertIn("BETHPAGE", srv.credit_union_profile(4735)["former_names"])

    def test_brand_aliases(self):
        self.assertEqual(srv.find_credit_union(name="BECU")[0]["cu_number"], 62604)
        self.assertEqual(srv.find_credit_union(name="PenFed")[0]["cu_number"], 227)

    def test_two_exact_matches_are_flagged_ambiguous(self):
        rows = srv.find_credit_union(name="State Employees")
        self.assertGreaterEqual(len([r for r in rows if r["match"] == "exact"]), 2)
        self.assertTrue(all(r["ambiguous"] for r in rows))

    def test_unambiguous_name_is_not_flagged(self):
        rows = srv.find_credit_union(name="Navy Federal")
        self.assertEqual(rows[0]["cu_number"], 5536)
        self.assertFalse(rows[0]["ambiguous"])

    def test_no_match_explains_itself(self):
        rows = srv.find_credit_union(name="Zzzzqq")
        self.assertEqual(len(rows), 1)
        self.assertIn("no_match", rows[0])

    def test_no_internal_rank_column_leaks(self):
        self.assertNotIn("_rank", srv.find_credit_union(state="NJ", limit=2)[0])

    def test_gone_credit_union_error_names_last_quarter(self):
        # 4735 reports in every quarter, so use a credit union that left
        gone = srv.run("SELECT cu_number FROM dim GROUP BY cu_number HAVING max(quarter) < '2024-12' LIMIT 1")[0]["cu_number"]
        err = srv.credit_union_profile(gone)
        self.assertIn("last reported quarter", err["error"])

    def test_unknown_field_hints(self):
        with self.assertRaises(ValueError) as cm:
            srv.q("auto_loans_originated")
        self.assertIn("not in the NCUA call report", str(cm.exception))
        with self.assertRaises(ValueError) as cm:
            srv.q("profit_margin")
        self.assertIn("roa_avg_assets_4q", str(cm.exception))

    def test_bad_filters_are_rejected_not_silently_empty(self):
        with self.assertRaises(ValueError):
            srv.metric_series("loan_to_share", state="Texas")
        with self.assertRaises(ValueError):
            srv.metric_series("loan_to_share", peer_group=9)
        with self.assertRaises(ValueError):
            srv.metric_series("deposit_mix_certificates", aggregate="sum")

    def test_group_by_peer_group_ranks_groups(self):
        rows = srv.metric_series("loan_to_share", group_by="peer_group")
        self.assertEqual(len(rows), 6)
        self.assertEqual(rows[0]["peer_group"], 6)
        self.assertEqual({r["quarter"] for r in rows}, {srv.latest_quarter()})

    def test_ratio_of_sums_is_pooled_not_median(self):
        r = srv.metric_series("shares_certificates", aggregate="ratio_of_sums", denominator="total_shares_and_deposits",
                              start="2025-12", end="2025-12")[0]
        self.assertTrue(0.2 < r["value"] < 0.4)

    def test_count_aggregate(self):
        r = srv.metric_series("total_assets", aggregate="count", start="2018-12", end="2018-12")[0]
        self.assertGreater(r["value"], 5000)
