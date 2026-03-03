import unittest

import pandas as pd

from analyzers.jtl_analyzer import compare


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


if __name__ == "__main__":
    unittest.main()
