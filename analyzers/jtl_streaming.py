"""
Streaming helpers for JTL parsing and exact aggregation.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterator

import numpy as np
import pandas as pd

from analyzers.jtl_analyzer import REQUIRED_COLUMNS, aggregate


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
        url_raw = row.get("URL")
        normalized["URL"] = np.nan if _is_tc_url(url_raw) else url_raw

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
                f"File '{path.name}' is not a valid JTL: missing columns {missing}"
            )

        for raw_row in reader:
            if len(raw_row) != len(header):
                continue
            row = dict(zip(header, raw_row))
            normalized = _normalize_row(row, path)
            if normalized is not None:
                yield normalized


def _scan_jtl_metadata(path: Path) -> tuple[bool, bool, bool]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        try:
            header = next(reader)
        except StopIteration:
            return False, False, False

        if not header:
            return False, False, False

        header = [column.strip() for column in header]
        if not REQUIRED_COLUMNS.issubset(header):
            missing = REQUIRED_COLUMNS - set(header)
            raise ValueError(
                f"File '{path.name}' is not a valid JTL: missing columns {missing}"
            )

        has_url_column = "URL" in header
        if not has_url_column:
            return False, False, True

        url_index = header.index("URL")
        for raw_row in reader:
            if len(raw_row) != len(header):
                continue
            if _is_tc_url(raw_row[url_index]):
                return True, True, True

        return True, False, True


def stream_jtl_rows(filepath: str | Path, mode: str = "auto") -> Iterator[dict[str, object]]:
    path = Path(filepath)
    if mode not in ("auto", "tc", "samplers"):
        raise ValueError(f"Unknown parse_jtl mode: '{mode}'. Allowed: auto, tc, samplers.")

    has_url_column, has_tc_rows, has_rows = _scan_jtl_metadata(path)
    if not has_rows:
        raise ValueError(f"File '{path.name}' is empty.")
    if not has_url_column and mode == "samplers":
        raise ValueError(
            f"File '{path.name}': samplers mode requires URL column, but it is missing."
        )
    if not has_url_column and mode == "tc":
        raise ValueError(f"File '{path.name}': selected mode 'tc' but no TC rows were found.")
    if not has_url_column and mode == "auto":
        pass

    if mode == "tc":
        if not has_tc_rows:
            raise ValueError(
                f"File '{path.name}': selected mode 'tc' but no TC rows were found."
            )
        for row in _stream_all_rows(path):
            if _is_tc_url(row.get("URL") if isinstance(row.get("URL"), str) else None):
                yield row
        return

    if mode == "samplers":
        any_row = False
        for row in _stream_all_rows(path):
            if not _is_tc_url(row.get("URL") if isinstance(row.get("URL"), str) else None):
                any_row = True
                yield row
        if not any_row:
            raise ValueError(
                f"File '{path.name}': after filtering (mode '{mode}') no rows remain."
            )
        return

    if has_url_column and has_tc_rows:
        for row in _stream_all_rows(path):
            if _is_tc_url(row.get("URL") if isinstance(row.get("URL"), str) else None):
                yield row
        return

    for row in _stream_all_rows(path):
        yield row


def aggregate_streaming_jtl(filepath: str | Path, mode: str = "auto") -> pd.DataFrame:
    rows = list(stream_jtl_rows(filepath, mode=mode))
    if not rows:
        return pd.DataFrame()
    return aggregate(pd.DataFrame(rows))
