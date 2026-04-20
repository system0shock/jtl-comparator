import csv
import tempfile
import threading
import unittest
from pathlib import Path

from werkzeug.serving import make_server

from app import app

try:
    from playwright.sync_api import sync_playwright  # type: ignore
except Exception:  # pragma: no cover
    sync_playwright = None


class _ServerThread(threading.Thread):
    def __init__(self, flask_app, host: str, port: int):
        super().__init__(daemon=True)
        self._server = make_server(host, port, flask_app)
        self._ctx = flask_app.app_context()
        self._ctx.push()

    def run(self):
        self._server.serve_forever()

    def shutdown(self):
        self._server.shutdown()
        self._ctx.pop()


def _write_jtl(path: Path, elapsed_by_label: dict[str, int], rows_per_label: int = 10):
    header = ["timeStamp", "elapsed", "label", "success", "URL"]
    ts = 1772560000000
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        labels = list(elapsed_by_label.keys())
        for i in range(rows_per_label):
            for label in labels:
                writer.writerow([ts, elapsed_by_label[label], label, "true", ""])
                ts += 100


class UiSortingE2ETest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if sync_playwright is None:
            raise unittest.SkipTest("Playwright не установлен")

        cls._server = _ServerThread(app, "127.0.0.1", 5055)
        cls._server.start()
        cls.base_url = "http://127.0.0.1:5055"

        cls._tmp = tempfile.TemporaryDirectory()
        tmpdir = Path(cls._tmp.name)
        cls.run1 = tmpdir / "run1.jtl"
        cls.run2 = tmpdir / "run2.jtl"

        # Ожидаемые дельты:
        # A: -10%, B: +20%, C: +5%
        _write_jtl(cls.run1, {"A-trx": 100, "B-trx": 100, "C-trx": 100})
        _write_jtl(cls.run2, {"A-trx": 90, "B-trx": 120, "C-trx": 105})

        try:
            cls._pw = sync_playwright().start()
            cls._browser = cls._pw.chromium.launch(headless=True)
        except Exception as exc:  # pragma: no cover
            cls._server.shutdown()
            cls._tmp.cleanup()
            raise unittest.SkipTest(f"Chromium для Playwright недоступен: {exc}")

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "_browser"):
            cls._browser.close()
        if hasattr(cls, "_pw"):
            cls._pw.stop()
        if hasattr(cls, "_server"):
            cls._server.shutdown()
        if hasattr(cls, "_tmp"):
            cls._tmp.cleanup()

    def test_sort_by_delta_avg_asc_and_desc(self):
        page = self._browser.new_page()
        try:
            page.goto(self.base_url, wait_until="networkidle")
            page.set_input_files("#file1", str(self.run1))
            page.set_input_files("#file2", str(self.run2))
            page.click("#btnCompare")
            page.wait_for_selector("#mainTable tbody tr", timeout=60_000)

            # Первый клик: asc -> минимальная дельта (A-trx, -10%)
            page.click('th[data-sort-key="d_avg"]')
            page.wait_for_timeout(120)
            first_label = page.locator("#mainTable tbody tr:not(.summary-row) td.label-cell").first.inner_text()
            first_idx = page.locator("#mainTable tbody tr:not(.summary-row) td.idx").first.inner_text()
            self.assertEqual(first_label, "A-trx")
            self.assertEqual(first_idx, "1")

            # Второй клик: desc -> максимальная дельта (B-trx, +20%)
            page.click('th[data-sort-key="d_avg"]')
            page.wait_for_timeout(120)
            first_label_desc = page.locator("#mainTable tbody tr:not(.summary-row) td.label-cell").first.inner_text()
            first_idx_desc = page.locator("#mainTable tbody tr:not(.summary-row) td.idx").first.inner_text()
            self.assertEqual(first_label_desc, "B-trx")
            self.assertEqual(first_idx_desc, "1")
        finally:
            page.close()


if __name__ == "__main__":
    unittest.main()
