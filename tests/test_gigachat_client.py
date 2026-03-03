"""
Тесты для analyzers/gigachat_client.py.

Покрывают:
- build_prompt: чистая функция, не требует моков.
- _load_credentials: логика чтения из файла и env.
- generate_summary: ошибка при отсутствии credentials.
"""

import os
import unittest
from unittest.mock import patch

from analyzers.gigachat_client import build_prompt, _load_credentials, generate_summary


def _make_data(
    name1: str = "Run1",
    name2: str = "Run2",
    rows: list | None = None,
    summary: dict | None = None,
) -> dict:
    """Вспомогательная фабрика тестовых данных."""
    if rows is None:
        rows = []
    return {"name1": name1, "name2": name2, "rows": rows, "summary": summary}


def _make_row(
    label: str,
    avg_1: float = 100.0,
    avg_2: float = 120.0,
    d_avg: float = 20.0,
    d_avg_class: str = "warning",
    d_p99: float | None = None,
    err_2: float = 0.0,
) -> dict:
    return {
        "label": label,
        "avg_1": avg_1, "avg_2": avg_2,
        "d_avg": d_avg, "d_avg_class": d_avg_class,
        "d_p99": d_p99,
        "err_2": err_2,
        # Поля для корректного попадания в both_rows
        "p95_1": None, "p95_2": None, "p99_1": None, "p99_2": None,
        "rps_1": None, "rps_2": None, "err_1": None,
        "d_p95": None, "d_p95_class": "neutral",
        "d_p99_class": "neutral", "d_rps": None, "d_rps_class": "neutral",
        "err_class": "neutral",
    }


class BuildPromptTests(unittest.TestCase):

    def test_includes_run_names(self):
        data = _make_data(name1="Baseline", name2="Release")
        prompt = build_prompt(data)
        self.assertIn("Baseline", prompt)
        self.assertIn("Release", prompt)

    def test_includes_summary_metrics(self):
        summary = {
            "avg_1": 100.0, "avg_2": 115.0, "d_avg": 15.0,
            "p95_1": 200.0, "p95_2": 210.0, "d_p95": 5.0,
            "p99_1": 300.0, "p99_2": 350.0, "d_p99": 16.7,
            "rps_1": 50.0, "rps_2": 48.0, "d_rps": -4.0,
            "err_1": 0.5, "err_2": 0.6,
        }
        data = _make_data(summary=summary)
        prompt = build_prompt(data)
        self.assertIn("100.0", prompt)
        self.assertIn("115.0", prompt)

    def test_section_sizes_limits_warning_rows(self):
        # 10 warning-строк, лимит warning=5 → не более 5 попадает в промпт
        rows = [_make_row(f"TX{i}", d_avg_class="warning") for i in range(10)]
        data = _make_data(rows=rows)
        prompt = build_prompt(data, section_sizes={"critical": 0, "warning": 5, "improved": 0})
        count = sum(1 for r in rows if r["label"] in prompt)
        self.assertLessEqual(count, 5)

    def test_critical_rows_appear_before_warning(self):
        rows = [
            _make_row("Warning-TX", d_avg=15.0, d_avg_class="warning"),
            _make_row("Critical-TX", d_avg=35.0, d_avg_class="critical"),
        ]
        data = _make_data(rows=rows)
        prompt = build_prompt(data)
        idx_critical = prompt.index("Critical-TX")
        idx_warning = prompt.index("Warning-TX")
        self.assertLess(idx_critical, idx_warning)

    def test_no_rows_shows_no_data_message(self):
        data = _make_data(rows=[])
        prompt = build_prompt(data)
        self.assertIn("Нет транзакций", prompt)

    def test_no_summary_shows_unavailable_message(self):
        data = _make_data(summary=None)
        prompt = build_prompt(data)
        self.assertIn("недоступны", prompt)

    def test_distribution_line_shows_counts(self):
        rows = [
            _make_row("C1", d_avg_class="critical"),
            _make_row("C2", d_avg_class="critical"),
            _make_row("W1", d_avg_class="warning"),
            _make_row("I1", d_avg_class="improved"),
        ]
        data = _make_data(rows=rows)
        prompt = build_prompt(data)
        self.assertIn("2 critical", prompt)
        self.assertIn("1 warning", prompt)
        self.assertIn("1 improved", prompt)

    def test_improved_section_present(self):
        rows = [
            _make_row("Fast-TX", d_avg=-15.0, d_avg_class="improved"),
        ]
        data = _make_data(rows=rows)
        prompt = build_prompt(data)
        self.assertIn("Fast-TX", prompt)
        self.assertIn("Улучшения", prompt)

    def test_drift_section_shows_only_r2_transactions(self):
        only_r2_row = {
            "label": "New-Endpoint",
            "avg_1": None, "avg_2": 200.0, "d_avg": None, "d_avg_class": "neutral",
            "d_p99": None, "err_2": 0.0, "err_1": None,
            "p95_1": None, "p95_2": None, "p99_1": None, "p99_2": None,
            "rps_1": None, "rps_2": None,
            "d_p95": None, "d_p95_class": "neutral",
            "d_p99_class": "neutral", "d_rps": None, "d_rps_class": "neutral",
            "err_class": "neutral",
        }
        data = _make_data(rows=[_make_row("Existing-TX"), only_r2_row])
        prompt = build_prompt(data)
        self.assertIn("New-Endpoint", prompt)
        self.assertIn("Новые", prompt)


class LoadCredentialsTests(unittest.TestCase):

    def test_returns_none_when_no_file_and_no_env(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("GIGACHAT_CREDENTIALS", None)
            # Временно подменяем путь к файлу на несуществующий
            import analyzers.gigachat_client as gc
            original = gc._KEY_FILE
            gc._KEY_FILE = gc._KEY_FILE.parent / "__nonexistent_key_file__.key"
            try:
                result = _load_credentials()
            finally:
                gc._KEY_FILE = original
            self.assertIsNone(result)

    def test_returns_env_var_as_fallback(self):
        import analyzers.gigachat_client as gc
        original = gc._KEY_FILE
        gc._KEY_FILE = gc._KEY_FILE.parent / "__nonexistent_key_file__.key"
        try:
            with patch.dict(os.environ, {"GIGACHAT_CREDENTIALS": "test_creds_123"}):
                result = _load_credentials()
        finally:
            gc._KEY_FILE = original
        self.assertEqual(result, "test_creds_123")


class GenerateSummaryTests(unittest.TestCase):

    def test_raises_runtime_error_when_no_credentials(self):
        import analyzers.gigachat_client as gc
        original = gc._KEY_FILE
        gc._KEY_FILE = gc._KEY_FILE.parent / "__nonexistent_key_file__.key"
        try:
            with patch.dict(os.environ, {}, clear=False):
                os.environ.pop("GIGACHAT_CREDENTIALS", None)
                with self.assertRaises(RuntimeError) as ctx:
                    generate_summary(_make_data())
        finally:
            gc._KEY_FILE = original
        self.assertIn("gigachat.key", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
