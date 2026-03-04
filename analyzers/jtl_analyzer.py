"""
Модуль анализа JTL-файлов JMeter.
Парсинг, агрегация метрик и сравнение двух прогонов нагрузочного тестирования.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Any


# Ожидаемые колонки стандартного JTL-формата JMeter
REQUIRED_COLUMNS = {"timeStamp", "elapsed", "label", "success"}


DEFAULT_DELTA_RULES = {
    "time_warning_pct": 10.0,
    "time_critical_pct": 20.0,
    "time_improved_pct": 10.0,
    "rps_warning_drop_pct": 10.0,
    "rps_critical_drop_pct": 20.0,
    "rps_improved_gain_pct": 10.0,
    "err_warning_increase_pct": 1.0,
    "err_critical_increase_pct": 3.0,
    "err_improved_decrease_pct": 0.0,
}


def _to_non_negative_float(value: Any, name: str) -> float:
    """Конвертирует значение в float >= 0."""
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Некорректное значение правила '{name}': {value}") from exc
    if result < 0:
        raise ValueError(f"Значение правила '{name}' не может быть отрицательным: {value}")
    return result


def _normalize_delta_rules(rules: dict | None) -> dict[str, float]:
    """
    Возвращает валидированные правила подсветки дельт.
    Если rules не переданы, используются значения по умолчанию.
    """
    normalized = DEFAULT_DELTA_RULES.copy()
    if not rules:
        return normalized

    for key in normalized:
        raw_value = rules.get(key)
        if raw_value is None or str(raw_value).strip() == "":
            continue
        normalized[key] = _to_non_negative_float(raw_value, key)

    if normalized["time_critical_pct"] < normalized["time_warning_pct"]:
        raise ValueError("Для времени critical порог должен быть >= warning порога.")
    if normalized["rps_critical_drop_pct"] < normalized["rps_warning_drop_pct"]:
        raise ValueError("Для RPS critical порог должен быть >= warning порога.")
    if normalized["err_critical_increase_pct"] < normalized["err_warning_increase_pct"]:
        raise ValueError("Для Error Rate critical порог должен быть >= warning порога.")

    return normalized


def parse_jtl(filepath: str | Path, mode: str = "auto") -> pd.DataFrame:
    """
    Читает JTL-файл и возвращает DataFrame для анализа.

    Режимы фильтрации (mode):
    - 'auto'     : TC-строки если есть (пустой URL), иначе все HTTP-сэмплеры.
    - 'tc'       : только Transaction Controller-строки; ошибка если их нет.
    - 'samplers' : только HTTP-сэмплеры (строки с непустым URL).

    Битые строки CSV (несовпадение числа колонок) пропускаются автоматически.

    :param filepath: путь к .jtl файлу
    :param mode: режим фильтрации — 'auto' | 'tc' | 'samplers'
    :return: очищенный DataFrame
    :raises ValueError: при проблемах с форматом файла
    """
    if mode not in ("auto", "tc", "samplers"):
        raise ValueError(f"Неизвестный режим parse_jtl: '{mode}'. Допустимые: auto, tc, samplers.")

    path = Path(filepath)

    try:
        # on_bad_lines='skip' — пропускаем строки с неверным числом полей
        df = pd.read_csv(path, low_memory=False, on_bad_lines="skip")
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

    # Определяем строки Transaction Controller (URL пустой/null/None)
    if "URL" in df.columns:
        is_tc = df["URL"].isna() | (df["URL"].astype(str).str.strip().isin(["", "null", "None"]))
        has_tc = is_tc.any()
    else:
        is_tc = pd.Series(False, index=df.index)
        has_tc = False

    if mode == "tc":
        if not has_tc:
            raise ValueError(
                f"Файл «{path.name}»: выбран режим «только Transaction Controllers», "
                f"но строки TC не найдены (нет строк с пустым URL). "
                f"Попробуйте режим «Авто» или «HTTP-сэмплеры»."
            )
        df = df[is_tc].copy()
    elif mode == "samplers":
        # Берём только строки с реальным URL (HTTP-сэмплеры)
        df = df[~is_tc].copy() if has_tc else df.copy()
    else:  # auto
        if has_tc:
            df = df[is_tc].copy()
        # Иначе — берём все строки как есть

    # Приводим типы
    df["elapsed"] = pd.to_numeric(df["elapsed"], errors="coerce")
    df["timeStamp"] = pd.to_numeric(df["timeStamp"], errors="coerce")

    # Поле success: JMeter пишет строки "true"/"false"
    df["success"] = df["success"].astype(str).str.lower().str.strip() == "true"

    # Убираем строки с невалидными значениями времени
    df = df.dropna(subset=["elapsed", "timeStamp"])

    if df.empty:
        raise ValueError(
            f"Файл «{path.name}»: после фильтрации (режим «{mode}») не осталось строк. "
            f"Проверьте формат файла или смените режим анализа."
        )

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


def _time_css_class(delta: float | None, rules: dict[str, float]) -> str:
    """CSS-класс для метрик времени (деградация = рост)."""
    if delta is None:
        return "neutral"
    if delta > rules["time_critical_pct"]:
        return "critical"
    if delta > rules["time_warning_pct"]:
        return "warning"
    if delta < -rules["time_improved_pct"]:
        return "improved"
    return "neutral"


def _rps_css_class(delta: float | None, rules: dict[str, float]) -> str:
    """CSS-класс для RPS (деградация = снижение, инвертированная логика)."""
    if delta is None:
        return "neutral"
    if delta < -rules["rps_critical_drop_pct"]:
        return "critical"
    if delta < -rules["rps_warning_drop_pct"]:
        return "warning"
    if delta > rules["rps_improved_gain_pct"]:
        return "improved"
    return "neutral"


def _err_css_class(err1: float, err2: float, rules: dict[str, float]) -> str:
    """CSS-класс для Error Rate."""
    diff = err2 - err1
    if diff > rules["err_critical_increase_pct"]:
        return "critical"
    if diff > rules["err_warning_increase_pct"]:
        return "warning"
    if diff < -rules["err_improved_decrease_pct"]:
        return "improved"
    return "neutral"


def compare(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    name1: str,
    name2: str,
    rules: dict | None = None,
) -> dict:
    """
    Сравнивает агрегированные метрики двух прогонов.

    :param df1: агрегированный DataFrame прогона 1
    :param df2: агрегированный DataFrame прогона 2
    :param name1: название прогона 1
    :param name2: название прогона 2
    :return: словарь с результатами для отдачи в JSON
    """
    active_rules = _normalize_delta_rules(rules)

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
        err1 = val("error_rate_1")
        err2 = val("error_rate_2")

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
            "err_1":       round(err1, 2) if err1 is not None else None,
            # Прогон 2
            "samples_2":   val("samples_2"),
            "avg_2":       avg2,
            "p95_2":       p95_2,
            "p99_2":       p99_2,
            "rps_2":       rps2,
            "err_2":       round(err2, 2) if err2 is not None else None,
            # Дельты и классы
            "d_avg":       d_avg,
            "d_avg_class": _time_css_class(d_avg, active_rules),
            "d_p95":       d_p95,
            "d_p95_class": _time_css_class(d_p95, active_rules),
            "d_p99":       d_p99,
            "d_p99_class": _time_css_class(d_p99, active_rules),
            "d_rps":       d_rps,
            "d_rps_class": _rps_css_class(d_rps, active_rules),
            "err_class":   _err_css_class(err1, err2, active_rules) if (err1 is not None and err2 is not None) else "neutral",
        })

    # Summary: средние значения по всем транзакциям (только где есть оба прогона)
    both = [r for r in rows if r["avg_1"] is not None and r["avg_2"] is not None]
    summary = _build_summary(both, active_rules) if both else None

    return {
        "name1":   name1,
        "name2":   name2,
        "rules":   active_rules,
        "rows":    rows,
        "summary": summary,
    }


def _build_summary(rows: list[dict], rules: dict[str, float]) -> dict:
    """Строит строку Summary — взвешенные средние по всем транзакциям (вес = samples)."""
    def weighted_avg_metric(value_key: str, weight_key: str) -> float | None:
        total_weight = 0.0
        weighted_sum = 0.0
        for r in rows:
            value = r.get(value_key)
            weight = r.get(weight_key)
            if value is None or weight is None or weight <= 0:
                continue
            total_weight += float(weight)
            weighted_sum += float(value) * float(weight)
        if total_weight == 0:
            return None
        return round(weighted_sum / total_weight, 1)

    avg1 = weighted_avg_metric("avg_1", "samples_1")
    avg2 = weighted_avg_metric("avg_2", "samples_2")
    p95_1 = weighted_avg_metric("p95_1", "samples_1")
    p95_2 = weighted_avg_metric("p95_2", "samples_2")
    p99_1 = weighted_avg_metric("p99_1", "samples_1")
    p99_2 = weighted_avg_metric("p99_2", "samples_2")
    rps1 = weighted_avg_metric("rps_1", "samples_1")
    rps2 = weighted_avg_metric("rps_2", "samples_2")
    err1 = weighted_avg_metric("err_1", "samples_1")
    err2 = weighted_avg_metric("err_2", "samples_2")

    d_avg = _delta_pct(avg1, avg2) if (avg1 is not None and avg2 is not None) else None
    d_p95 = _delta_pct(p95_1, p95_2) if (p95_1 is not None and p95_2 is not None) else None
    d_p99 = _delta_pct(p99_1, p99_2) if (p99_1 is not None and p99_2 is not None) else None
    d_rps = _delta_pct(rps1, rps2) if (rps1 is not None and rps2 is not None) else None

    return {
        "label":       "SUMMARY (avg)",
        "samples_1":   sum(r["samples_1"] for r in rows if r.get("samples_1")),
        "avg_1":       avg1,
        "p95_1":       p95_1,
        "p99_1":       p99_1,
        "rps_1":       rps1,
        "err_1":       round(err1, 2) if err1 is not None else None,
        "samples_2":   sum(r["samples_2"] for r in rows if r.get("samples_2")),
        "avg_2":       avg2,
        "p95_2":       p95_2,
        "p99_2":       p99_2,
        "rps_2":       rps2,
        "err_2":       round(err2, 2) if err2 is not None else None,
        "d_avg":       d_avg,
        "d_avg_class": _time_css_class(d_avg, rules),
        "d_p95":       d_p95,
        "d_p95_class": _time_css_class(d_p95, rules),
        "d_p99":       d_p99,
        "d_p99_class": _time_css_class(d_p99, rules),
        "d_rps":       d_rps,
        "d_rps_class": _rps_css_class(d_rps, rules),
        "err_class":   _err_css_class(err1, err2, rules) if (err1 is not None and err2 is not None) else "neutral",
    }
