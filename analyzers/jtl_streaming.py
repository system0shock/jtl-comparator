"""
Streaming helpers for JTL parsing and exact aggregation.
"""

from __future__ import annotations

import csv
import os
import tempfile
from pathlib import Path
from itertools import zip_longest
from typing import Iterator

import numpy as np
import pandas as pd

from analyzers.jtl_analyzer import REQUIRED_COLUMNS


def _is_tc_url(raw_value: str | None) -> bool:
    if raw_value is None:
        return True
    stripped = raw_value.strip()
    return stripped in ("", "null", "None")


def _coerce_numeric(value: str, field_name: str, path: Path) -> int | float:
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        raise ValueError(f"File '{path.name}': invalid numeric value in '{field_name}': {value!r}")
    if isinstance(numeric, np.generic):
        numeric = numeric.item()
    return numeric


def _normalize_row(row: dict[str, str], path: Path) -> dict[str, object] | None:
    elapsed_raw = row.get("elapsed")
    timestamp_raw = row.get("timeStamp")
    label = row.get("label")
    success_raw = row.get("success")

    if elapsed_raw is None or timestamp_raw is None or label is None or success_raw is None:
        return None

    try:
        elapsed = _coerce_numeric(elapsed_raw, "elapsed", path)
        timestamp = _coerce_numeric(timestamp_raw, "timeStamp", path)
    except ValueError:
        return None

    normalized = {
        "timeStamp": timestamp,
        "elapsed": elapsed,
        "label": label,
        "success": str(success_raw).lower().strip() == "true",
    }

    if "URL" in row:
        normalized["URL"] = np.nan if _is_tc_url(row.get("URL")) else row.get("URL")

    return normalized


def _stream_all_rows(path: Path) -> Iterator[dict[str, object]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        try:
            header = next(reader)
        except StopIteration:
            return

        if not header:
            return

        header = [column.strip() for column in header]
        if not REQUIRED_COLUMNS.issubset(header):
            missing = REQUIRED_COLUMNS - set(header)
            raise ValueError(
                f"Файл «{path.name}» не является корректным JTL: отсутствуют колонки {missing}"
            )

        for raw_row in reader:
            padded_row = list(zip_longest(header, raw_row, fillvalue=""))
            row = {key: value for key, value in padded_row}
            normalized = _normalize_row(row, path)
            if normalized is not None:
                yield normalized


def _scan_jtl_metadata(path: Path) -> tuple[bool, bool, bool, bool]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        try:
            header = next(reader)
        except StopIteration:
            return False, False, False, False

        if not header:
            return False, False, False, True

        header = [column.strip() for column in header]
        if not REQUIRED_COLUMNS.issubset(header):
            missing = REQUIRED_COLUMNS - set(header)
            raise ValueError(
                f"Файл «{path.name}» не является корректным JTL: отсутствуют колонки {missing}"
            )

        has_url_column = "URL" in header
        saw_any_row = False
        if not has_url_column:
            for raw_row in reader:
                saw_any_row = True
                if len(raw_row) != len(header):
                    continue
            return False, False, saw_any_row, True

        for raw_row in reader:
            saw_any_row = True
            padded_row = list(zip_longest(header, raw_row, fillvalue=""))
            row = {key: value for key, value in padded_row}
            if _is_tc_url(row.get("URL")):
                return True, True, True, True

        return True, False, saw_any_row, True


def stream_jtl_rows(filepath: str | Path, mode: str = "auto") -> Iterator[dict[str, object]]:
    path = Path(filepath)
    if mode not in ("auto", "tc", "samplers"):
        raise ValueError(f"Неизвестный режим parse_jtl: '{mode}'. Допустимые: auto, tc, samplers.")

    has_url_column, has_tc_rows, has_rows, has_header = _scan_jtl_metadata(path)
    if not has_header:
        raise ValueError(f"Не удалось прочитать файл «{path.name}»: No columns to parse from file")
    if not has_rows:
        raise ValueError(f"Файл «{path.name}» пустой.")
    if mode == "samplers" and not has_url_column:
        raise ValueError(
            f"Файл «{path.name}»: режим «HTTP-сэмплеры» требует колонку URL, но она отсутствует."
        )
    if mode == "tc" and not has_tc_rows:
        raise ValueError(
            f"Файл «{path.name}»: выбран режим «только Transaction Controllers», "
            f"но строки TC не найдены (нет строк с пустым URL). "
            f"Попробуйте режим «Авто» или «HTTP-сэмплеры»."
        )

    def _iterator() -> Iterator[dict[str, object]]:
        yielded = 0
        if mode == "tc":
            for row in _stream_all_rows(path):
                if _is_tc_url(row.get("URL") if isinstance(row.get("URL"), str) else None):
                    yielded += 1
                    yield row
        elif mode == "samplers":
            for row in _stream_all_rows(path):
                if not _is_tc_url(row.get("URL") if isinstance(row.get("URL"), str) else None):
                    yielded += 1
                    yield row
        elif has_url_column and has_tc_rows:
            for row in _stream_all_rows(path):
                if _is_tc_url(row.get("URL") if isinstance(row.get("URL"), str) else None):
                    yielded += 1
                    yield row
        else:
            for row in _stream_all_rows(path):
                yielded += 1
                yield row

        if yielded == 0:
            if mode == "tc":
                raise ValueError(
                    f"Файл «{path.name}»: после фильтрации (режим «tc») не осталось строк. "
                    f"Проверьте формат файла или смените режим анализа."
                )
            if mode == "samplers":
                raise ValueError(
                    f"Файл «{path.name}»: после фильтрации (режим «samplers») не осталось строк. "
                    f"Проверьте формат файла или смените режим анализа."
                )
            raise ValueError(
                f"Файл «{path.name}»: после фильтрации (режим «auto») не осталось строк. "
                f"Проверьте формат файла или смените режим анализа."
            )

    return _iterator()


def _group_stats(df: pd.DataFrame, total_duration_sec: float) -> dict[str, object]:
    samples = float(len(df))
    elapsed = df["elapsed"]
    group_duration = (df["timeStamp"].max() - df["timeStamp"].min()) / 1000.0
    throughput_duration = group_duration if group_duration > 0 else total_duration_sec
    if throughput_duration <= 0:
        throughput_duration = 1.0

    return {
        "samples": samples,
        "avg": round(float(elapsed.mean()), 1),
        "p50": round(float(elapsed.quantile(0.50)), 1),
        "p90": round(float(elapsed.quantile(0.90)), 1),
        "p95": round(float(elapsed.quantile(0.95)), 1),
        "p99": round(float(elapsed.quantile(0.99)), 1),
        "min": round(float(elapsed.min()), 1),
        "max": round(float(elapsed.max()), 1),
        "throughput": round(float(samples / throughput_duration), 3),
        "error_rate": round(float((~df["success"]).sum() / samples * 100), 2),
    }


def _read_label_frame(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["elapsed"] = pd.to_numeric(df["elapsed"], errors="coerce")
    df["timeStamp"] = pd.to_numeric(df["timeStamp"], errors="coerce")
    df["success"] = df["success"].astype(str).str.lower().str.strip() == "true"
    return df.dropna(subset=["elapsed", "timeStamp"])


def aggregate_streaming_jtl(filepath: str | Path, mode: str = "auto") -> pd.DataFrame:
    path = Path(filepath)
    tmp_root = Path.cwd() / ".tmp-test" / "jtl-streaming"
    tmp_root.mkdir(parents=True, exist_ok=True)

    label_files: dict[str, Path] = {}
    label_order: list[str] = []
    min_ts: float | None = None
    max_ts: float | None = None

    def _label_path(label: str) -> Path:
        existing = label_files.get(label)
        if existing is not None:
            return existing

        fd, raw_path = tempfile.mkstemp(dir=tmp_root, suffix=".csv", prefix="label-")
        os.close(fd)
        file_path = Path(raw_path)
        with file_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["timeStamp", "elapsed", "success"])
        label_files[label] = file_path
        label_order.append(label)
        return file_path

    try:
        for row in stream_jtl_rows(path, mode=mode):
            label = str(row["label"])
            file_path = _label_path(label)
            with file_path.open("a", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow([row["timeStamp"], row["elapsed"], row["success"]])

            ts = float(row["timeStamp"])
            min_ts = ts if min_ts is None else min(min_ts, ts)
            max_ts = ts if max_ts is None else max(max_ts, ts)

        total_duration_sec = 1.0
        if min_ts is not None and max_ts is not None:
            total_duration_sec = (max_ts - min_ts) / 1000.0
            if total_duration_sec <= 0:
                total_duration_sec = 1.0

        rows: list[dict[str, object]] = []
        for label in label_order:
            df = _read_label_frame(label_files[label])
            if df.empty:
                continue
            stats = _group_stats(df, total_duration_sec)
            stats["label"] = label
            rows.append(stats)

        return pd.DataFrame(rows, columns=[
            "label",
            "samples",
            "avg",
            "p50",
            "p90",
            "p95",
            "p99",
            "min",
            "max",
            "throughput",
            "error_rate",
        ])
    finally:
        for file_path in label_files.values():
            try:
                file_path.unlink()
            except FileNotFoundError:
                pass
