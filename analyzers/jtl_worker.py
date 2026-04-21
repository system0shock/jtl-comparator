"""Thread-based worker for async large-JTL comparison jobs.

Runs aggregate_streaming_jtl in a background thread and writes progress.json
and result.json (or error.json) to work_dir so the parent can poll status.
Cancellation is cooperative: set the cancel_event and the streaming loop exits
within _PROGRESS_ROW_INTERVAL rows (defined in jtl_streaming).
"""

from __future__ import annotations

import json
import threading
import time
import traceback
from pathlib import Path

from analyzers.jtl_analyzer import compare_preaggregated
from analyzers.jtl_streaming import aggregate_streaming_jtl, CancelledError


_PROGRESS_FILE = "progress.json"
_RESULT_FILE = "result.json"
_ERROR_FILE = "error.json"
_DONE_FILE = "done.json"

# Weight bands
_PARSE1_START = 10.0
_PARSE1_END   = 52.0
_PARSE2_START = 52.0
_PARSE2_END   = 90.0
_BUILD_START  = 90.0


def _write(path: Path, data: dict) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    # On Windows, os.replace() raises PermissionError if the target is briefly
    # locked by a concurrent reader (e.g. the polling endpoint). Retry a few times.
    for attempt in range(10):
        try:
            tmp.replace(path)
            return
        except PermissionError:
            if attempt == 9:
                raise
            time.sleep(0.01)


def _progress(work_dir: Path, pct: float, stage: str, message: str) -> None:
    _write(work_dir / _PROGRESS_FILE, {
        "progress_pct": round(pct, 1),
        "stage": stage,
        "message": message,
    })


def run_comparison_job(
    work_dir: Path,
    path1: str,
    path2: str,
    name1: str,
    name2: str,
    jtl_mode: str,
    delta_rules: dict,
    cancel_event: threading.Event,
) -> None:
    """Called inside a background thread. Writes results to work_dir."""
    try:
        _progress(work_dir, _PARSE1_START, "parsing_run1", f"Анализ «{name1}»…")

        def prog1(frac: float) -> None:
            pct = _PARSE1_START + frac * (_PARSE1_END - _PARSE1_START)
            _progress(work_dir, pct, "parsing_run1", f"Анализ «{name1}»: {round(frac * 100)}%")

        agg1 = aggregate_streaming_jtl(path1, mode=jtl_mode, on_progress=prog1,
                                       cancel_event=cancel_event)

        if cancel_event.is_set():
            return

        _progress(work_dir, _PARSE2_START, "parsing_run2", f"Анализ «{name2}»…")

        def prog2(frac: float) -> None:
            pct = _PARSE2_START + frac * (_PARSE2_END - _PARSE2_START)
            _progress(work_dir, pct, "parsing_run2", f"Анализ «{name2}»: {round(frac * 100)}%")

        agg2 = aggregate_streaming_jtl(path2, mode=jtl_mode, on_progress=prog2,
                                       cancel_event=cancel_event)

        if cancel_event.is_set():
            return

        _progress(work_dir, _BUILD_START, "building_result", "Формирование результата…")
        result = compare_preaggregated(agg1, agg2, name1, name2, rules=delta_rules)

        result_path = work_dir / _RESULT_FILE
        _write(result_path, result)
        _write(work_dir / _DONE_FILE, {"status": "completed"})
        _progress(work_dir, 100.0, "completed", "Готово")

    except CancelledError:
        pass
    except Exception:
        _write(work_dir / _ERROR_FILE, {"error": traceback.format_exc()})
        _progress(work_dir, 0.0, "failed", "Ошибка при обработке")
