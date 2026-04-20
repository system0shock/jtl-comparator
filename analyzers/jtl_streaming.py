"""
Streaming helpers for JTL parsing and exact aggregation.
"""

from __future__ import annotations

import csv
import os
import shutil
import tempfile
import uuid
from pathlib import Path
from itertools import zip_longest
import threading
from typing import Callable, Iterator

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


_PROGRESS_ROW_INTERVAL = 5_000


class CancelledError(Exception):
    """Raised when a streaming job is cancelled via a threading.Event."""


def _stream_all_rows(
    path: Path,
    _pos_reporter: "Callable[[int], None] | None" = None,
    _cancel_event: "threading.Event | None" = None,
) -> Iterator[dict[str, object]]:
    row_count = 0
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
            if len(raw_row) > len(header):
                continue
            padded_row = list(zip_longest(header, raw_row, fillvalue=""))
            row = {key: value for key, value in padded_row}
            normalized = _normalize_row(row, path)
            if normalized is not None:
                yield normalized
                row_count += 1
                if row_count % _PROGRESS_ROW_INTERVAL == 0:
                    if _cancel_event and _cancel_event.is_set():
                        raise CancelledError("Job cancelled")
                    if _pos_reporter:
                        _pos_reporter(handle.tell())


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
            if len(raw_row) > len(header):
                continue
            padded_row = list(zip_longest(header, raw_row, fillvalue=""))
            row = {key: value for key, value in padded_row}
            if _is_tc_url(row.get("URL")):
                return True, True, True, True

        return True, False, saw_any_row, True


def stream_jtl_rows(
    filepath: str | Path,
    mode: str = "auto",
    _pos_reporter: "Callable[[int], None] | None" = None,
    _cancel_event: "threading.Event | None" = None,
) -> Iterator[dict[str, object]]:
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
            for row in _stream_all_rows(path, _pos_reporter, _cancel_event):
                if _is_tc_url(row.get("URL") if isinstance(row.get("URL"), str) else None):
                    yielded += 1
                    yield row
        elif mode == "samplers":
            for row in _stream_all_rows(path, _pos_reporter, _cancel_event):
                if not _is_tc_url(row.get("URL") if isinstance(row.get("URL"), str) else None):
                    yielded += 1
                    yield row
        elif has_url_column and has_tc_rows:
            for row in _stream_all_rows(path, _pos_reporter, _cancel_event):
                if _is_tc_url(row.get("URL") if isinstance(row.get("URL"), str) else None):
                    yielded += 1
                    yield row
        else:
            for row in _stream_all_rows(path, _pos_reporter, _cancel_event):
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


def _create_spill_dir(base_dir: Path) -> Path:
    candidate_roots = [Path(tempfile.gettempdir()), base_dir / ".tmp-runtime"]

    for root in candidate_roots:
        root.mkdir(parents=True, exist_ok=True)
        candidate = root / f"jtl-streaming-{uuid.uuid4().hex}"
        try:
            candidate.mkdir()
            probe = candidate / "probe.txt"
            probe.write_text("ok", encoding="utf-8")
            probe.unlink()
            return candidate
        except PermissionError:
            shutil.rmtree(candidate, ignore_errors=True)

    raise PermissionError("Unable to create a writable spill directory for JTL streaming.")


def aggregate_streaming_jtl(
    filepath: str | Path,
    mode: str = "auto",
    on_progress: "Callable[[float], None] | None" = None,
    cancel_event: "threading.Event | None" = None,
) -> pd.DataFrame:
    """Parse and aggregate a JTL file using streaming + disk spill.

    on_progress(frac) is called periodically with a float in [0, 1].
    cancel_event, when set, causes CancelledError to be raised mid-stream.
    """
    path = Path(filepath)
    total_bytes = path.stat().st_size if on_progress else 1
    tmp_root = _create_spill_dir(path.parent)

    label_files: dict[str, Path] = {}
    label_order: list[str] = []
    min_ts: float | None = None
    max_ts: float | None = None

    def _label_path(label: str) -> Path:
        existing = label_files.get(label)
        if existing is not None:
            return existing

        file_path = tmp_root / f"label-{len(label_files):06d}.csv"
        with file_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["timeStamp", "elapsed", "success"])
        label_files[label] = file_path
        label_order.append(label)
        return file_path

    def _pos_reporter(pos: int) -> None:
        if on_progress and total_bytes > 0:
            on_progress(min(pos / total_bytes, 1.0))

    try:
        for row in stream_jtl_rows(path, mode=mode,
                                    _pos_reporter=_pos_reporter if on_progress else None,
                                    _cancel_event=cancel_event):
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
        shutil.rmtree(tmp_root, ignore_errors=True)
