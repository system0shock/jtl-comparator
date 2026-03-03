"""
Модуль анализа JTL-файлов JMeter.
Парсинг, агрегация метрик и сравнение двух прогонов нагрузочного тестирования.
"""

import pandas as pd
import numpy as np
from pathlib import Path


# Ожидаемые колонки стандартного JTL-формата JMeter
REQUIRED_COLUMNS = {"timeStamp", "elapsed", "label", "success"}


def parse_jtl(filepath: str | Path) -> pd.DataFrame:
    """
    Читает JTL-файл и возвращает DataFrame с родительскими транзакциями.

    Логика фильтрации:
    - Если в файле есть строки с пустым URL (Transaction Controller) — берём только их.
    - Если все строки имеют URL (только HTTP-сэмплеры) — берём все строки.

    :param filepath: путь к .jtl файлу
    :return: очищенный DataFrame
    :raises ValueError: при проблемах с форматом файла
    """
    path = Path(filepath)

    try:
        df = pd.read_csv(path, low_memory=False)
    except Exception as exc:
        raise ValueError(f"Не удалось прочитать файл «{path.name}»: {exc}") from exc

    # Проверяем наличие обязательных колонок
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(
            f"Файл «{path.name}» не является корректным JTL: "
            f"отсутствуют колонки {missing}"
        )

    if df.empty:
        raise ValueError(f"Файл «{path.name}» пустой.")

    # Определяем тип строк: Transaction Controller — URL пустой/null/None
    if "URL" in df.columns:
        is_parent = df["URL"].isna() | (df["URL"].astype(str).str.strip().isin(["", "null", "None"]))
        has_parents = is_parent.any()
    else:
        has_parents = False

    if has_parents:
        df = df[is_parent].copy()
    # Иначе — берём все строки как есть

    # Приводим типы
    df["elapsed"] = pd.to_numeric(df["elapsed"], errors="coerce")
    df["timeStamp"] = pd.to_numeric(df["timeStamp"], errors="coerce")

    # Поле success: JMeter пишет строки "true"/"false"
    df["success"] = df["success"].astype(str).str.lower().str.strip() == "true"

    # Убираем строки с невалидными значениями времени
    df = df.dropna(subset=["elapsed", "timeStamp"])

    return df


def aggregate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Агрегирует метрики по полю label (имя транзакции).

    Возвращает DataFrame с колонками:
    label, samples, avg, p50, p90, p95, p99, min, max, throughput, error_rate
    """
    if df.empty:
        return pd.DataFrame()

    # Длительность прогона в секундах для расчёта throughput
    total_duration_sec = (df["timeStamp"].max() - df["timeStamp"].min()) / 1000.0
    if total_duration_sec <= 0:
        total_duration_sec = 1.0  # защита от деления на ноль

    def agg_group(g: pd.DataFrame) -> pd.Series:
        samples = len(g)
        elapsed = g["elapsed"]
        # Throughput считается относительно всего прогона, не только группы
        group_duration = (g["timeStamp"].max() - g["timeStamp"].min()) / 1000.0
        throughput_duration = group_duration if group_duration > 0 else total_duration_sec
        return pd.Series({
            "samples":     samples,
            "avg":         round(elapsed.mean(), 1),
            "p50":         round(elapsed.quantile(0.50), 1),
            "p90":         round(elapsed.quantile(0.90), 1),
            "p95":         round(elapsed.quantile(0.95), 1),
            "p99":         round(elapsed.quantile(0.99), 1),
            "min":         round(elapsed.min(), 1),
            "max":         round(elapsed.max(), 1),
            "throughput":  round(samples / throughput_duration, 3),
            "error_rate":  round((~g["success"]).sum() / samples * 100, 2),
        })

    result = df.groupby("label", sort=False).apply(agg_group).reset_index()
    return result


def _delta_pct(v1: float, v2: float) -> float | None:
    """Вычисляет процентное изменение от v1 к v2. Возвращает None если v1 == 0."""
    if v1 == 0:
        return None
    return round((v2 - v1) / v1 * 100, 1)


def _time_css_class(delta: float | None) -> str:
    """CSS-класс для метрик времени (деградация = рост)."""
    if delta is None:
        return "neutral"
    if delta > 20:
        return "critical"
    if delta > 10:
        return "warning"
    if delta < -10:
        return "improved"
    return "neutral"


def _rps_css_class(delta: float | None) -> str:
    """CSS-класс для RPS (деградация = снижение, инвертированная логика)."""
    if delta is None:
        return "neutral"
    if delta < -20:
        return "critical"
    if delta < -10:
        return "warning"
    if delta > 10:
        return "improved"
    return "neutral"


def _err_css_class(err1: float, err2: float) -> str:
    """CSS-класс для Error Rate."""
    diff = err2 - err1
    if diff > 3:
        return "critical"
    if diff > 1:
        return "warning"
    if diff < 0:
        return "improved"
    return "neutral"


def compare(df1: pd.DataFrame, df2: pd.DataFrame, name1: str, name2: str) -> dict:
    """
    Сравнивает агрегированные метрики двух прогонов.

    :param df1: агрегированный DataFrame прогона 1
    :param df2: агрегированный DataFrame прогона 2
    :param name1: название прогона 1
    :param name2: название прогона 2
    :return: словарь с результатами для отдачи в JSON
    """
    agg1 = aggregate(df1)
    agg2 = aggregate(df2)

    # Объединяем по label — outer join, чтобы видеть транзакции из обоих прогонов
    merged = pd.merge(agg1, agg2, on="label", how="outer", suffixes=("_1", "_2"))

    rows = []
    for _, row in merged.iterrows():
        label = row["label"]

        # Вспомогательная функция: безопасно читает значение, возвращает None если NaN
        def val(col: str) -> float | None:
            v = row.get(col)
            return None if (v is None or (isinstance(v, float) and np.isnan(v))) else round(float(v), 1)

        avg1, avg2 = val("avg_1"), val("avg_2")
        p95_1, p95_2 = val("p95_1"), val("p95_2")
        p99_1, p99_2 = val("p99_1"), val("p99_2")
        rps1, rps2 = val("throughput_1"), val("throughput_2")
        err1 = val("error_rate_1") or 0.0
        err2 = val("error_rate_2") or 0.0

        d_avg = _delta_pct(avg1 or 0, avg2 or 0) if (avg1 is not None and avg2 is not None) else None
        d_p95 = _delta_pct(p95_1 or 0, p95_2 or 0) if (p95_1 is not None and p95_2 is not None) else None
        d_p99 = _delta_pct(p99_1 or 0, p99_2 or 0) if (p99_1 is not None and p99_2 is not None) else None
        d_rps = _delta_pct(rps1 or 0, rps2 or 0) if (rps1 is not None and rps2 is not None) else None

        rows.append({
            "label":       label,
            # Прогон 1
            "samples_1":   val("samples_1"),
            "avg_1":       avg1,
            "p95_1":       p95_1,
            "p99_1":       p99_1,
            "rps_1":       rps1,
            "err_1":       round(err1, 2),
            # Прогон 2
            "samples_2":   val("samples_2"),
            "avg_2":       avg2,
            "p95_2":       p95_2,
            "p99_2":       p99_2,
            "rps_2":       rps2,
            "err_2":       round(err2, 2),
            # Дельты и классы
            "d_avg":       d_avg,
            "d_avg_class": _time_css_class(d_avg),
            "d_p95":       d_p95,
            "d_p95_class": _time_css_class(d_p95),
            "d_p99":       d_p99,
            "d_p99_class": _time_css_class(d_p99),
            "d_rps":       d_rps,
            "d_rps_class": _rps_css_class(d_rps),
            "err_class":   _err_css_class(err1, err2),
        })

    # Summary: средние значения по всем транзакциям (только где есть оба прогона)
    both = [r for r in rows if r["avg_1"] is not None and r["avg_2"] is not None]
    summary = _build_summary(both, name1, name2) if both else None

    return {
        "name1":   name1,
        "name2":   name2,
        "rows":    rows,
        "summary": summary,
    }


def _build_summary(rows: list[dict], name1: str, name2: str) -> dict:
    """Строит строку Summary — среднее по всем транзакциям."""
    def avg_metric(key: str) -> float | None:
        vals = [r[key] for r in rows if r.get(key) is not None]
        return round(sum(vals) / len(vals), 1) if vals else None

    avg1 = avg_metric("avg_1")
    avg2 = avg_metric("avg_2")
    p95_1 = avg_metric("p95_1")
    p95_2 = avg_metric("p95_2")
    p99_1 = avg_metric("p99_1")
    p99_2 = avg_metric("p99_2")
    rps1 = avg_metric("rps_1")
    rps2 = avg_metric("rps_2")
    err1 = avg_metric("err_1") or 0.0
    err2 = avg_metric("err_2") or 0.0

    d_avg = _delta_pct(avg1 or 0, avg2 or 0)
    d_p95 = _delta_pct(p95_1 or 0, p95_2 or 0)
    d_p99 = _delta_pct(p99_1 or 0, p99_2 or 0)
    d_rps = _delta_pct(rps1 or 0, rps2 or 0)

    return {
        "label":       "SUMMARY (avg)",
        "samples_1":   sum(r["samples_1"] for r in rows if r.get("samples_1")),
        "avg_1":       avg1,
        "p95_1":       p95_1,
        "p99_1":       p99_1,
        "rps_1":       rps1,
        "err_1":       round(err1, 2),
        "samples_2":   sum(r["samples_2"] for r in rows if r.get("samples_2")),
        "avg_2":       avg2,
        "p95_2":       p95_2,
        "p99_2":       p99_2,
        "rps_2":       rps2,
        "err_2":       round(err2, 2),
        "d_avg":       d_avg,
        "d_avg_class": _time_css_class(d_avg),
        "d_p95":       d_p95,
        "d_p95_class": _time_css_class(d_p95),
        "d_p99":       d_p99,
        "d_p99_class": _time_css_class(d_p99),
        "d_rps":       d_rps,
        "d_rps_class": _rps_css_class(d_rps),
        "err_class":   _err_css_class(err1, err2),
    }
