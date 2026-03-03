import io
import unittest

from app import app


def _jtl_csv(rows: list[dict]) -> str:
    header = "timeStamp,elapsed,label,success,URL\n"
    body = "\n".join(
        f"{r['timeStamp']},{r['elapsed']},{r['label']},{str(r['success']).lower()},{r.get('URL', '')}"
        for r in rows
    )
    return header + body + "\n"


class CompareApiTests(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()

    def _post_compare(self, csv1: str, csv2: str, extra_form: dict | None = None):
        form = {
            "name1": "R1",
            "name2": "R2",
            "file1": (io.BytesIO(csv1.encode("utf-8")), "run1.jtl"),
            "file2": (io.BytesIO(csv2.encode("utf-8")), "run2.jtl"),
        }
        if extra_form:
            form.update(extra_form)
        return self.client.post("/compare", data=form, content_type="multipart/form-data")

    def test_compare_happy_path_returns_rows_and_summary(self):
        csv1 = _jtl_csv(
            [
                {"timeStamp": 1, "elapsed": 100, "label": "A", "success": True, "URL": ""},
                {"timeStamp": 2, "elapsed": 120, "label": "A", "success": True, "URL": ""},
            ]
        )
        csv2 = _jtl_csv(
            [
                {"timeStamp": 1, "elapsed": 110, "label": "A", "success": True, "URL": ""},
                {"timeStamp": 2, "elapsed": 130, "label": "A", "success": False, "URL": ""},
            ]
        )

        resp = self._post_compare(csv1, csv2)
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertIn("rows", data)
        self.assertIn("summary", data)
        self.assertEqual(data["name1"], "R1")
        self.assertEqual(data["name2"], "R2")
        self.assertTrue(len(data["rows"]) >= 1)

    def test_compare_returns_400_when_files_missing(self):
        resp = self.client.post("/compare", data={"name1": "R1", "name2": "R2"})
        self.assertEqual(resp.status_code, 400)
        self.assertIn("error", resp.get_json())

    def test_compare_returns_422_for_invalid_rules(self):
        csv = _jtl_csv(
            [
                {"timeStamp": 1, "elapsed": 100, "label": "A", "success": True, "URL": ""},
                {"timeStamp": 2, "elapsed": 100, "label": "A", "success": True, "URL": ""},
            ]
        )
        resp = self._post_compare(
            csv,
            csv,
            extra_form={
                "time_warning_pct": "25",
                "time_critical_pct": "15",
            },
        )
        self.assertEqual(resp.status_code, 422)
        payload = resp.get_json()
        self.assertIn("error", payload)
        self.assertIn("critical", payload["error"])

    def test_compare_applies_custom_rules_from_form(self):
        csv1 = _jtl_csv(
            [
                {"timeStamp": 1, "elapsed": 100, "label": "A", "success": True, "URL": ""},
                {"timeStamp": 2, "elapsed": 100, "label": "A", "success": True, "URL": ""},
            ]
        )
        csv2 = _jtl_csv(
            [
                {"timeStamp": 1, "elapsed": 112, "label": "A", "success": True, "URL": ""},
                {"timeStamp": 2, "elapsed": 112, "label": "A", "success": True, "URL": ""},
            ]
        )

        resp = self._post_compare(
            csv1,
            csv2,
            extra_form={
                "time_warning_pct": "5",
                "time_critical_pct": "10",
            },
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data["rules"]["time_warning_pct"], 5.0)
        self.assertEqual(data["rules"]["time_critical_pct"], 10.0)
        self.assertEqual(data["rows"][0]["d_avg"], 12.0)
        self.assertEqual(data["rows"][0]["d_avg_class"], "critical")


class SummarizeApiTests(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()

    def test_summarize_returns_503_when_no_credentials(self):
        """Без gigachat.key и без env-переменной должен вернуться 503."""
        import analyzers.gigachat_client as gc
        import os
        original_key_file = gc._KEY_FILE
        gc._KEY_FILE = gc._KEY_FILE.parent / "__nonexistent__.key"
        try:
            from unittest.mock import patch
            with patch.dict(os.environ, {}, clear=False):
                os.environ.pop("GIGACHAT_CREDENTIALS", None)
                payload = {"name1": "R1", "name2": "R2", "rows": [], "summary": None}
                resp = self.client.post(
                    "/summarize",
                    json=payload,
                    content_type="application/json",
                )
        finally:
            gc._KEY_FILE = original_key_file
        self.assertEqual(resp.status_code, 503)
        self.assertIn("error", resp.get_json())

    def test_summarize_returns_400_on_empty_body(self):
        """Пустое тело запроса должно вернуть 400."""
        import analyzers.gigachat_client as gc
        import os
        # Имитируем наличие credentials, чтобы пройти первую проверку
        original_key_file = gc._KEY_FILE
        gc._KEY_FILE = gc._KEY_FILE.parent / "__nonexistent__.key"
        try:
            from unittest.mock import patch
            with patch.dict(os.environ, {"GIGACHAT_CREDENTIALS": "fake_b64_creds"}):
                resp = self.client.post(
                    "/summarize",
                    data="",
                    content_type="application/json",
                )
        finally:
            gc._KEY_FILE = original_key_file
        self.assertEqual(resp.status_code, 400)
        self.assertIn("error", resp.get_json())


if __name__ == "__main__":
    unittest.main()
