"""
Тесты для парсеров Gatling и детектора форматов файлов.
"""

import json
import os
import tempfile
import unittest
from pathlib import Path

from analyzers.gatling_log_parser import parse_gatling_log
from analyzers.gatling_json_parser import parse_gatling_json
from analyzers.file_detector import detect_file_type
from analyzers.jtl_analyzer import compare, compare_from_agg


# ---------------------------------------------------------------------------
# Вспомогательные генераторы тестовых данных
# ---------------------------------------------------------------------------

def _write_temp(content: str, suffix: str = ".log") -> str:
    """Сохраняет строку во временный файл, возвращает путь."""
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", suffix=suffix, delete=False
    ) as f:
        f.write(content)
        return f.name


def _sim_log(lines: list[str]) -> str:
    """Формирует минимальный simulation.log из списка строк."""
    header = "RUN\tMySimulation\tsim1\t1700000000000\tTest\t3.9.5\n"
    return header + "\n".join(lines) + "\n"


def _request_line(name: str, start: int, duration: int, ok: bool) -> str:
    end = start + duration
    status = "OK" if ok else "KO"
    return f"REQUEST\t\t{name}\t{start}\t{end}\t{status}\t"


def _group_line(name: str, start: int, duration: int, ok: bool) -> str:
    end = start + duration
    status = "OK" if ok else "KO"
    cumul = duration
    return f"GROUP\t{name}\t{start}\t{end}\t{cumul}\t{status}"


def _stats_js(requests: dict[str, dict]) -> str:
    """Формирует минимальный stats.js Gatling из словаря {имя: {ok, ko, avg, p50, p95, p99, min, max, rps}}."""
    contents = {}
    for name, m in requests.items():
        total = m["ok"] + m["ko"]
        contents[name] = {
            "type": "REQUEST",
            "name": name,
            "stats": {
                "numberOfRequests": {"total": total, "ok": m["ok"], "ko": m["ko"]},
                "minResponseTime":  {"total": m["min"]},
                "maxResponseTime":  {"total": m["max"]},
                "meanResponseTime": {"total": m["avg"]},
                "percentiles1":     {"total": m["p50"]},
                "percentiles2":     {"total": m.get("p75", m["p50"])},
                "percentiles3":     {"total": m["p95"]},
                "percentiles4":     {"total": m["p99"]},
                "meanNumberOfRequestsPerSecond": {"total": m["rps"]},
            },
        }
    root = {
        "type": "GROUP",
        "name": "All Requests",
        "stats": {},
        "contents": contents,
    }
    return f"var statsResults = {json.dumps(root)};"


# ---------------------------------------------------------------------------
# Тесты: gatling_log_parser
# ---------------------------------------------------------------------------

class TestGatlingLogParser(unittest.TestCase):

    def setUp(self):
        self._files: list[str] = []

    def tearDown(self):
        for f in self._files:
            if os.path.exists(f):
                os.remove(f)

    def _tmp(self, content: str) -> str:
        path = _write_temp(content)
        self._files.append(path)
        return path

    def test_uses_group_records_when_present(self):
        """Если есть GROUP-записи, должны использоваться они, а не REQUEST."""
        content = _sim_log([
            _request_line("GetProduct", 1700000001000, 200, True),
            _request_line("GetProduct", 1700000002000, 180, True),
            _group_line("Checkout", 1700000001000, 500, True),
            _group_line("Checkout", 1700000002000, 450, True),
        ])
        df = parse_gatling_log(self._tmp(content))
        labels = set(df["label"].unique())
        self.assertIn("Checkout", labels)
        self.assertNotIn("GetProduct", labels)

    def test_uses_request_records_when_no_group(self):
        """Если GROUP нет, берём REQUEST-записи."""
        content = _sim_log([
            _request_line("Login", 1700000001000, 300, True),
            _request_line("Login", 1700000002000, 320, False),
        ])
        df = parse_gatling_log(self._tmp(content))
        self.assertEqual(list(df["label"].unique()), ["Login"])
        self.assertEqual(len(df), 2)

    def test_ok_ko_status_mapping(self):
        """OK → success=True, KO → success=False."""
        content = _sim_log([
            _request_line("Req", 1700000001000, 100, True),
            _request_line("Req", 1700000002000, 100, False),
        ])
        df = parse_gatling_log(self._tmp(content))
        self.assertTrue(df[df["timeStamp"] == 1700000001000]["success"].iloc[0])
        self.assertFalse(df[df["timeStamp"] == 1700000002000]["success"].iloc[0])

    def test_elapsed_is_end_minus_start(self):
        """elapsed должен быть end - start."""
        content = _sim_log([
            _request_line("Req", 1700000000000, 250, True),
        ])
        df = parse_gatling_log(self._tmp(content))
        self.assertEqual(df["elapsed"].iloc[0], 250.0)

    def test_group_elapsed_is_wall_clock(self):
        """Для GROUP elapsed = endTs - startTs (не cumulatedRT)."""
        start = 1700000000000
        duration = 600
        content = _sim_log([
            _group_line("TxGroup", start, duration, True),
        ])
        df = parse_gatling_log(self._tmp(content))
        self.assertEqual(df["elapsed"].iloc[0], float(duration))

    def test_raises_on_empty_file(self):
        """Пустой файл → ValueError."""
        with self.assertRaises(ValueError):
            parse_gatling_log(self._tmp(""))

    def test_raises_on_no_valid_records(self):
        """Файл только с RUN-строкой → ValueError."""
        content = "RUN\tSim\tsim1\t1700000000000\tDesc\t3.9.5\n"
        with self.assertRaises(ValueError):
            parse_gatling_log(self._tmp(content))

    def test_end_to_end_compare(self):
        """Полный пайплайн: parse_gatling_log → compare → rows."""
        t = 1700000000000
        content1 = _sim_log([
            _request_line("Search", t, 200, True),
            _request_line("Search", t + 1000, 210, True),
        ])
        content2 = _sim_log([
            _request_line("Search", t, 300, True),   # деградация
            _request_line("Search", t + 1000, 310, True),
        ])
        from analyzers.gatling_log_parser import parse_gatling_log as pgl

        df1 = pgl(self._tmp(content1))
        df2 = pgl(self._tmp(content2))
        result = compare(df1, df2, "Run1", "Run2")
        self.assertEqual(len(result["rows"]), 1)
        row = result["rows"][0]
        self.assertEqual(row["label"], "Search")
        self.assertIsNotNone(row["d_avg"])
        self.assertGreater(row["d_avg"], 0)  # время выросло


# ---------------------------------------------------------------------------
# Тесты: gatling_json_parser
# ---------------------------------------------------------------------------

class TestGatlingJsonParser(unittest.TestCase):

    def setUp(self):
        self._files: list[str] = []

    def tearDown(self):
        for f in self._files:
            if os.path.exists(f):
                os.remove(f)

    def _tmp(self, content: str) -> str:
        path = _write_temp(content, suffix=".js")
        self._files.append(path)
        return path

    def test_extracts_all_request_nodes(self):
        """Все REQUEST-узлы собираются из contents."""
        content = _stats_js({
            "Login":   {"ok": 100, "ko": 0, "avg": 150, "p50": 120, "p95": 300, "p99": 450, "min": 50, "max": 600, "rps": 5.0},
            "Checkout":{"ok": 80,  "ko": 2, "avg": 200, "p50": 180, "p95": 400, "p99": 600, "min": 80, "max": 800, "rps": 4.0},
        })
        df = parse_gatling_json(self._tmp(content))
        labels = set(df["label"].unique())
        self.assertEqual(labels, {"Login", "Checkout"})

    def test_error_rate_calculation(self):
        """error_rate = ko / total * 100."""
        content = _stats_js({
            "Req": {"ok": 90, "ko": 10, "avg": 100, "p50": 90, "p95": 200, "p99": 300, "min": 20, "max": 500, "rps": 10.0},
        })
        df = parse_gatling_json(self._tmp(content))
        self.assertAlmostEqual(df[df["label"] == "Req"]["error_rate"].iloc[0], 10.0)

    def test_p90_is_none(self):
        """p90 всегда None — не доступен в stats.js."""
        content = _stats_js({
            "Req": {"ok": 100, "ko": 0, "avg": 100, "p50": 90, "p95": 200, "p99": 300, "min": 20, "max": 500, "rps": 5.0},
        })
        df = parse_gatling_json(self._tmp(content))
        self.assertIsNone(df["p90"].iloc[0])

    def test_metrics_mapped_correctly(self):
        """percentiles3 → p95, percentiles4 → p99."""
        content = _stats_js({
            "Req": {"ok": 50, "ko": 0, "avg": 200, "p50": 180, "p95": 400, "p99": 600, "min": 50, "max": 700, "rps": 2.5},
        })
        df = parse_gatling_json(self._tmp(content))
        row = df.iloc[0]
        self.assertEqual(row["p95"], 400.0)
        self.assertEqual(row["p99"], 600.0)
        self.assertEqual(row["avg"], 200.0)

    def test_raises_on_invalid_json(self):
        """Некорректный JSON → ValueError."""
        with self.assertRaises(ValueError):
            parse_gatling_json(self._tmp("var statsResults = {not valid json};"))

    def test_raises_on_no_requests(self):
        """stats.js без REQUEST-узлов → ValueError."""
        content = "var statsResults = " + json.dumps({"type": "GROUP", "name": "All", "stats": {}, "contents": {}}) + ";"
        with self.assertRaises(ValueError):
            parse_gatling_json(self._tmp(content))

    def test_compare_from_agg_end_to_end(self):
        """parse_gatling_json → compare_from_agg → rows."""
        content1 = _stats_js({
            "Search": {"ok": 100, "ko": 0, "avg": 150, "p50": 130, "p95": 300, "p99": 450, "min": 50, "max": 600, "rps": 10.0},
        })
        content2 = _stats_js({
            "Search": {"ok": 100, "ko": 0, "avg": 200, "p50": 170, "p95": 400, "p99": 550, "min": 60, "max": 700, "rps": 8.0},
        })
        agg1 = parse_gatling_json(self._tmp(content1))
        agg2 = parse_gatling_json(self._tmp(content2))
        result = compare_from_agg(agg1, agg2, "Run1", "Run2")
        self.assertEqual(len(result["rows"]), 1)
        row = result["rows"][0]
        self.assertGreater(row["d_avg"], 0)  # время выросло


# ---------------------------------------------------------------------------
# Тесты: file_detector
# ---------------------------------------------------------------------------

class TestFileDetector(unittest.TestCase):

    def setUp(self):
        self._files: list[str] = []

    def tearDown(self):
        for f in self._files:
            if os.path.exists(f):
                os.remove(f)

    def _tmp(self, content: str, suffix: str = ".log") -> str:
        path = _write_temp(content, suffix=suffix)
        self._files.append(path)
        return path

    def test_detects_gatling_log_by_run_header(self):
        content = "RUN\tMySimulation\tsim1\t1700000000000\tTest\t3.9.5\n"
        self.assertEqual(detect_file_type(self._tmp(content)), "gatling_log")

    def test_detects_gatling_log_by_request_line(self):
        content = "REQUEST\t\tLogin\t1700000000000\t1700000000200\tOK\t\n"
        self.assertEqual(detect_file_type(self._tmp(content)), "gatling_log")

    def test_detects_gatling_json(self):
        content = _stats_js({
            "Req": {"ok": 100, "ko": 0, "avg": 100, "p50": 90, "p95": 200, "p99": 300, "min": 20, "max": 500, "rps": 5.0},
        })
        self.assertEqual(detect_file_type(self._tmp(content, suffix=".js")), "gatling_json")

    def test_detects_jtl(self):
        content = "timeStamp,elapsed,label,responseCode,responseMessage,threadName,dataType,success\n"
        content += "1700000000000,200,Login,200,OK,Thread-1,text,true\n"
        self.assertEqual(detect_file_type(self._tmp(content, suffix=".jtl")), "jtl")

    def test_raises_on_unknown_format(self):
        content = "this is some random content that doesn't match any format\n"
        with self.assertRaises(ValueError):
            detect_file_type(self._tmp(content))

    def test_raises_on_empty_file(self):
        with self.assertRaises(ValueError):
            detect_file_type(self._tmp(""))


if __name__ == "__main__":
    unittest.main()
