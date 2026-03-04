import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from analyzers.jtl_analyzer import compare, parse_jtl, _delta_pct, _normalize_delta_rules


def _make_rows(label: str, samples: int, elapsed: float, success: bool = True, start_ts: int = 0):
    return [
        {
            "timeStamp": start_ts + i * 1000,
            "elapsed": elapsed,
            "label": label,
            "success": success,
        }
        for i in range(samples)
    ]


class JtlAnalyzerSummaryTests(unittest.TestCase):
    def test_summary_uses_weighted_average_by_samples(self):
        # A: 100 samples, B: 1 sample. Arithmetic mean would be 550, weighted ~108.9.
        df1 = pd.DataFrame(
            _make_rows("A", 100, 100.0, True, 0) + _make_rows("B", 1, 1000.0, True, 200000)
        )
        df2 = pd.DataFrame(
            _make_rows("A", 100, 200.0, True, 0) + _make_rows("B", 1, 1000.0, True, 200000)
        )

        result = compare(df1, df2, "Run 1", "Run 2")
        summary = result["summary"]

        self.assertIsNotNone(summary)
        self.assertEqual(summary["samples_1"], 101.0)
        self.assertEqual(summary["samples_2"], 101.0)
        self.assertEqual(summary["avg_1"], 108.9)
        self.assertEqual(summary["avg_2"], 207.9)

    def test_missing_run_error_rate_is_none_and_neutral(self):
        df1 = pd.DataFrame(_make_rows("OnlyInRun1", 3, 100.0, True, 0))
        df2 = pd.DataFrame(_make_rows("OnlyInRun2", 3, 100.0, True, 0))

        result = compare(df1, df2, "Run 1", "Run 2")
        only_run1_row = next(r for r in result["rows"] if r["label"] == "OnlyInRun1")

        self.assertIsNone(only_run1_row["err_2"])
        self.assertEqual(only_run1_row["err_class"], "neutral")

    def test_custom_delta_rules_are_applied(self):
        df1 = pd.DataFrame(_make_rows("A", 10, 100.0, True, 0))
        df2 = pd.DataFrame(_make_rows("A", 10, 112.0, True, 0))

        custom_rules = {
            "time_warning_pct": 5,
            "time_critical_pct": 10,
        }
        result = compare(df1, df2, "Run 1", "Run 2", rules=custom_rules)
        row = result["rows"][0]

        self.assertEqual(row["d_avg"], 12.0)
        self.assertEqual(row["d_avg_class"], "critical")
        self.assertEqual(result["rules"]["time_warning_pct"], 5.0)
        self.assertEqual(result["rules"]["time_critical_pct"], 10.0)

    def test_delta_pct_returns_none_when_baseline_zero(self):
        self.assertIsNone(_delta_pct(0, 15))

    def test_normalize_rules_rejects_negative_value(self):
        with self.assertRaises(ValueError):
            _normalize_delta_rules({"time_warning_pct": -1})

    def test_normalize_rules_rejects_critical_less_than_warning(self):
        with self.assertRaises(ValueError):
            _normalize_delta_rules({"time_warning_pct": 15, "time_critical_pct": 10})


class ParseJtlModeTests(unittest.TestCase):
    @staticmethod
    def _write_jtl(content: str) -> str:
        td = TemporaryDirectory()
        path = Path(td.name) / "sample.jtl"
        path.write_text(content, encoding="utf-8")
        # Keep directory alive until tests end
        if not hasattr(ParseJtlModeTests, "_tmp_dirs"):
            ParseJtlModeTests._tmp_dirs = []
        ParseJtlModeTests._tmp_dirs.append(td)
        return str(path)

    @classmethod
    def tearDownClass(cls):
        for td in getattr(cls, "_tmp_dirs", []):
            td.cleanup()

    def test_parse_jtl_auto_prefers_tc_rows_when_present(self):
        csv = (
            "timeStamp,elapsed,label,success,URL\n"
            "1,100,TC,true,\n"
            "2,120,HTTP,true,https://example/a\n"
        )
        df = parse_jtl(self._write_jtl(csv), mode="auto")
        self.assertListEqual(df["label"].tolist(), ["TC"])

    def test_parse_jtl_samplers_returns_only_non_tc_rows(self):
        csv = (
            "timeStamp,elapsed,label,success,URL\n"
            "1,100,TC,true,\n"
            "2,120,HTTP,true,https://example/a\n"
        )
        df = parse_jtl(self._write_jtl(csv), mode="samplers")
        self.assertListEqual(df["label"].tolist(), ["HTTP"])

    def test_parse_jtl_tc_mode_raises_when_tc_not_found(self):
        csv = (
            "timeStamp,elapsed,label,success,URL\n"
            "1,100,HTTP,true,https://example/a\n"
        )
        with self.assertRaisesRegex(ValueError, "TC не найдены"):
            parse_jtl(self._write_jtl(csv), mode="tc")

    def test_parse_jtl_rejects_unknown_mode(self):
        csv = (
            "timeStamp,elapsed,label,success,URL\n"
            "1,100,A,true,\n"
        )
        with self.assertRaisesRegex(ValueError, "Неизвестный режим"):
            parse_jtl(self._write_jtl(csv), mode="unknown")

    def test_parse_jtl_skips_malformed_csv_lines(self):
        csv = (
            "timeStamp,elapsed,label,success,URL\n"
            "1,100,A,true,\n"
            "BROKEN,LINE,WITH,TOO,MANY,COLUMNS,1\n"
            "2,110,B,true,\n"
        )
        df = parse_jtl(self._write_jtl(csv), mode="auto")
        self.assertListEqual(df["label"].tolist(), ["A", "B"])

    def test_parse_jtl_raises_when_no_rows_after_filtering(self):
        csv = (
            "timeStamp,elapsed,label,success,URL\n"
            "1,100,TC,true,\n"
            "2,110,TC2,false,\n"
        )
        with self.assertRaisesRegex(ValueError, "не осталось строк"):
            parse_jtl(self._write_jtl(csv), mode="samplers")

    def test_parse_jtl_samplers_requires_url_column(self):
        csv = (
            "timeStamp,elapsed,label,success\n"
            "1,100,A,true\n"
            "2,110,B,true\n"
        )
        with self.assertRaisesRegex(ValueError, "требует колонку URL"):
            parse_jtl(self._write_jtl(csv), mode="samplers")


if __name__ == "__main__":
    unittest.main()
