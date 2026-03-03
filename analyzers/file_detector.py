"""
Автоматическое определение типа файла нагрузочного тестирования.

Поддерживаемые форматы:
  - 'jtl'          — JMeter JTL (CSV с заголовком timeStamp,elapsed,label,...)
  - 'gatling_log'  — Gatling simulation.log (tab-separated, строки RUN/REQUEST/GROUP)
  - 'gatling_json' — Gatling stats.js из HTML-отчёта (JS с var statsResults = {...})
"""

from pathlib import Path

# Количество байт, читаемых для определения типа
_PROBE_BYTES = 8192

# Маркеры Gatling simulation.log — запись начинается с одного из этих типов
_GATLING_LOG_MARKERS = ("RUN\t", "REQUEST\t", "GROUP\t", "USER\t")


def detect_file_type(filepath: str | Path) -> str:
    """
    Определяет тип файла нагрузочного тестирования по содержимому.

    :param filepath: путь к файлу
    :return: 'jtl' | 'gatling_log' | 'gatling_json'
    :raises ValueError: если формат не распознан
    """
    path = Path(filepath)

    try:
        with path.open(encoding="utf-8", errors="replace") as fh:
            probe = fh.read(_PROBE_BYTES)
    except OSError as exc:
        raise ValueError(f"Не удалось прочитать файл «{path.name}»: {exc}") from exc

    if not probe.strip():
        raise ValueError(f"Файл «{path.name}» пустой.")

    # Ищем первую непустую строку
    first_line = ""
    for line in probe.splitlines():
        stripped = line.strip()
        if stripped:
            first_line = stripped
            break

    # --- Проверка: Gatling simulation.log ---
    if any(first_line.startswith(marker) for marker in _GATLING_LOG_MARKERS):
        return "gatling_log"

    # --- Проверка: Gatling stats.js (JS-обёртка или чистый JSON) ---
    if first_line.startswith("var ") or first_line.startswith("{"):
        # Пробуем распознать как Gatling stats.js
        if _looks_like_gatling_stats(probe):
            return "gatling_json"

    # --- Проверка: JTL (CSV с нужными заголовками) ---
    if _looks_like_jtl(first_line):
        return "jtl"

    raise ValueError(
        f"Формат файла «{path.name}» не распознан. "
        "Поддерживаются: JMeter JTL (.jtl, .csv), "
        "Gatling simulation.log (.log) и Gatling stats.js (.js)."
    )


def _looks_like_jtl(first_line: str) -> bool:
    """Проверяет, является ли первая строка заголовком JTL CSV."""
    cols = {c.strip() for c in first_line.split(",")}
    return {"timeStamp", "elapsed", "label"}.issubset(cols)


def _looks_like_gatling_stats(probe: str) -> bool:
    """
    Проверяет, является ли фрагмент файла Gatling stats.js.
    Ищет характерные поля JSON-структуры отчёта.
    """
    markers = ('"statsResults"', '"numberOfRequests"', '"percentiles1"', '"meanResponseTime"')
    return any(m in probe for m in markers)
