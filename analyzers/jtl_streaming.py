"""
Async-friendly JTL aggregation wrapper.

Uses the same pd.read_csv-based parse_jtl/aggregate pipeline as the sync path,
but exposes on_progress callbacks and a cancel_event so the background worker
can report stage progress and respond to cancellation requests.
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Callable

import pandas as pd

from analyzers.jtl_analyzer import parse_jtl, aggregate


class CancelledError(Exception):
    """Raised when a job is cancelled via a threading.Event."""


def aggregate_streaming_jtl(
    filepath: str | Path,
    mode: str = "auto",
    on_progress: "Callable[[float], None] | None" = None,
    cancel_event: "threading.Event | None" = None,
) -> pd.DataFrame:
    """Parse and aggregate a JTL file, reporting coarse stage progress.

    on_progress(frac) is called with 0.5 after parsing and 1.0 after aggregation.
    cancel_event, when set before aggregation starts, causes CancelledError.
    """
    path = Path(filepath)

    if on_progress:
        on_progress(0.0)

    df = parse_jtl(path, mode=mode)

    if cancel_event and cancel_event.is_set():
        raise CancelledError("Job cancelled")

    if on_progress:
        on_progress(0.5)

    result = aggregate(df)

    if on_progress:
        on_progress(1.0)

    return result
