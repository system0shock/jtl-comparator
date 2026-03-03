"""
Парсер файлов stats.js из HTML-отчётов Gatling 3.8.x — 3.9.x.

Структура файла:
  var statsResults = {
    "type": "GROUP",
    "name": "All Requests",
    "stats": { ... },
    "contents": {
      "reqName": {
        "type": "REQUEST",
        "name": "reqName",
        "stats": {
          "numberOfRequests": {"total": N, "ok": N, "ko": N},
          "minResponseTime":  {"total": N},
          "maxResponseTime":  {"total": N},
          "meanResponseTime": {"total": N},
          "percentiles1":     {"total": N},  // p50
          "percentiles2":     {"total": N},  // p75
          "percentiles3":     {"total": N},  // p95
          "percentiles4":     {"total": N},  // p99
          "meanNumberOfRequestsPerSecond": {"total": N}
        }
      }
    }
  }

Примечание: p90 в stats.js недоступен, соответствующая колонка будет None.
"""

import json
import re
from pathlib import Path

import pandas as pd


# Шаблон для удаления JS-обёртки вида «var statsResults = {...};»
_JS_WRAPPER_RE = re.compile(r"^\s*var\s+\w+\s*=\s*", re.MULTILINE)


def parse_gatling_json(filepath: str | Path) -> pd.DataFrame:
    """
    Читает stats.js из Gatling HTML-отчёта.

    Возвращает уже агрегированный DataFrame с колонками:
      label, samples, avg, p50, p90 (None), p95, p99, min, max, throughput, error_rate

    :param filepath: путь к stats.js
    :return: агрегированный DataFrame
    :raises ValueError: если файл не является корректным stats.js
    """
    path = Path(filepath)

    try:
        raw = path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        raise ValueError(f"Не удалось прочитать файл «{path.name}»: {exc}") from exc

    # Снимаем JS-обёртку, оставляем только JSON
    json_text = _JS_WRAPPER_RE.sub("", raw).rstrip().rstrip(";")

    try:
        data = json.loads(json_text)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Файл «{path.name}» не является корректным Gatling stats.js: {exc}"
        ) from exc

    # Рекурсивно собираем все REQUEST-узлы
    request_nodes: list[dict] = []
    _collect_requests(data, request_nodes)

    if not request_nodes:
        raise ValueError(
            f"Файл «{path.name}» не содержит REQUEST-записей. "
            "Убедитесь, что это stats.js из Gatling HTML-отчёта."
        )

    rows = []
    for node in request_nodes:
        row = _extract_stats_row(node)
        if row is not None:
            rows.append(row)

    if not rows:
        raise ValueError(f"Файл «{path.name}»: не удалось извлечь метрики ни из одного запроса.")

    return pd.DataFrame(rows)


def _collect_requests(node: dict, result: list[dict]) -> None:
    """Рекурсивно обходит дерево stats.js и собирает узлы с type=REQUEST."""
    if not isinstance(node, dict):
        return
    if node.get("type") == "REQUEST":
        result.append(node)
    for child in node.get("contents", {}).values():
        _collect_requests(child, result)


def _extract_stats_row(node: dict) -> dict | None:
    """Извлекает метрики из одного REQUEST-узла в плоский словарь."""
    stats = node.get("stats", {})
    name = node.get("name", "unknown")

    def total(key: str) -> float | None:
        """Возвращает поле ['total'] из вложенного объекта, или None."""
        value = stats.get(key, {}).get("total")
        if value is None or value == "-":
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    samples_total = total("numberOfRequests")
    if not samples_total:
        return None

    samples_ok = total("numberOfRequests") or 0  # для error_rate
    samples_ko_raw = stats.get("numberOfRequests", {}).get("ko")
    try:
        samples_ko = float(samples_ko_raw) if samples_ko_raw not in (None, "-") else 0.0
    except (TypeError, ValueError):
        samples_ko = 0.0

    error_rate = round(samples_ko / samples_total * 100, 2) if samples_total > 0 else 0.0

    throughput_raw = total("meanNumberOfRequestsPerSecond")
    throughput = round(throughput_raw, 3) if throughput_raw is not None else None

    def ms(key: str) -> float | None:
        v = total(key)
        return round(v, 1) if v is not None else None

    return {
        "label":       name,
        "samples":     int(samples_total),
        "avg":         ms("meanResponseTime"),
        "p50":         ms("percentiles1"),
        "p90":         None,               # p90 недоступен в stats.js
        "p95":         ms("percentiles3"),
        "p99":         ms("percentiles4"),
        "min":         ms("minResponseTime"),
        "max":         ms("maxResponseTime"),
        "throughput":  throughput,
        "error_rate":  error_rate,
    }
