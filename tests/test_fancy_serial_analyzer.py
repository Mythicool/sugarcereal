import unittest
from pathlib import Path
import shutil

import fancy_serial_analyzer as fsa


class FancySerialAnalyzerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data_dir = fsa.module_base_dir()
        cls.matcher = fsa.configure_dataset_date_matcher(cls.data_dir)

    def test_normalize_serial_token_ocr_repairs(self):
        parsed = fsa.normalize_serial_token("9O81-7263*")
        self.assertEqual(parsed["status"], "valid")
        self.assertEqual(parsed["digits"], "90817263")
        self.assertTrue(parsed["is_star"])
        self.assertIn("O->0", parsed["corrections"])

    def test_parse_continuous_digit_stream_into_8_digit_serials(self):
        parsed = fsa.parse_serial_input_text("1234567887654321")
        self.assertEqual(parsed["valid_serials"], ["12345678", "87654321"])
        self.assertEqual(parsed["summary"]["valid_count"], 2)
        self.assertEqual(parsed["summary"]["too_many_count"], 0)

    def test_parse_continuous_digit_stream_reports_trailing_remainder(self):
        parsed = fsa.parse_serial_input_text("1234567887654321123")
        self.assertEqual(parsed["valid_serials"], ["12345678", "87654321"])
        self.assertEqual(parsed["summary"]["insufficient_count"], 1)
        self.assertTrue(any(issue["raw"] == "123" for issue in parsed["issues"]))

    def test_us_holiday_dataset_match_present(self):
        result = fsa.analyze_serials_for_web(
            serials=["01012026"],
            include_dataset_dates=True,
            data_dir=str(self.data_dir),
        )
        pattern_names = {
            p["pattern_name"]
            for sr in result["serial_results"]
            for p in sr["patterns"]
        }
        self.assertIn("US HOLIDAY DATASET MATCH", pattern_names)

    def test_zip_code_note_has_city_state_when_reference_loaded(self):
        patterns = fsa.analyze_serial("01012026")
        zip_patterns = [p for p in patterns if p["pattern_name"] == "ZIP CODE NOTE"]
        self.assertTrue(zip_patterns, "Expected ZIP CODE NOTE to be detected")
        detail = zip_patterns[0]["pattern_detail"]
        self.assertIn("(", detail)
        self.assertIn(")", detail)

    def test_pattern_confidence_levels(self):
        self.assertEqual(fsa.pattern_confidence("INVALID (need 8 digits)"), "LOW")
        self.assertEqual(fsa.pattern_confidence("RADAR / PALINDROME"), "HIGH")
        self.assertEqual(fsa.pattern_confidence("ANGEL NUMBER"), "MEDIUM")

    def test_single_digit_bookends_not_reported(self):
        patterns = fsa.analyze_serial("12345671")
        names = {p["pattern_name"] for p in patterns}
        self.assertNotIn("BOOKENDS", names)

    def test_multiple_pairs_count_only_adjacent_left_to_right(self):
        patterns = fsa.analyze_serial("19589111")
        names = {p["pattern_name"] for p in patterns}
        self.assertNotIn("MULTIPLE PAIRS", names)

    def test_ebay_title_omits_default_2021_series(self):
        title = fsa.build_ebay_title(
            serial="01012026",
            digits="01012026",
            pattern_name="US HOLIDAY DATASET MATCH",
            pattern_detail="New Year's Day (01/01/2026)",
            denomination="$1",
            series="2021",
        )
        self.assertNotIn("2021", title)

    def test_zip_quick_reference_present_in_web_payload(self):
        result = fsa.analyze_serials_for_web(
            serials=["01012026"],
            include_dataset_dates=True,
            data_dir=str(self.data_dir),
        )
        refs = result.get("zip_quick_reference", [])
        self.assertTrue(refs)
        self.assertIn("city", refs[0])
        self.assertIn("state", refs[0])
        self.assertIn("zip", refs[0])

    def test_zip_reference_fallback_from_us_txt(self):
        root = Path("tests") / ".tmp_zip_fallback"
        if root.exists():
            shutil.rmtree(root, ignore_errors=True)
        us_dir = root / "US"
        us_dir.mkdir(parents=True, exist_ok=True)
        us_txt = us_dir / "US.txt"
        us_txt.write_text(
            "US\t01012\tChesterfield\tMassachusetts\tMA\t\t\t\t\t42.3818\t-72.8334\t4\n",
            encoding="utf-8",
        )

        matcher = fsa.DatasetDateMatcher()
        matcher.load_zip_reference(root / fsa.DEFAULT_DATASET_FILES["zip_reference"])
        self.assertIn("01012", matcher.zip_reference)
        self.assertEqual(matcher.zip_reference["01012"]["city"], "Chesterfield")

        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
