from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4


TERMINAL_STATUSES = {"completed", "failed", "cancelled"}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _as_aware_utc(value: datetime | None) -> datetime:
    if value is None:
        return _utcnow()
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


@dataclass
class JobRecord:
    job_id: str
    work_dir: Path
    status: str = "queued"
    stage: str = "queued"
    message: str = "Queued"
    progress_pct: float = 0
    created_at: datetime = field(default_factory=_utcnow)
    updated_at: datetime = field(default_factory=_utcnow)
    result_path: Path | None = None
    error: str | None = None
    process_pid: int | None = None
    terminal_at: datetime | None = None
    expires_at: datetime | None = None

    def snapshot(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "status": self.status,
            "stage": self.stage,
            "message": self.message,
            "progress_pct": self.progress_pct,
            "created_at": _iso(self.created_at),
            "updated_at": _iso(self.updated_at),
            "work_dir": str(self.work_dir),
            "result_path": str(self.result_path) if self.result_path is not None else None,
            "error": self.error,
            "process_pid": self.process_pid,
        }


class JobRegistry:
    def __init__(
        self,
        root_dir: str | Path,
        *,
        completed_ttl_seconds: int = 15 * 60,
        failed_ttl_seconds: int = 0,
        cancelled_ttl_seconds: int = 0,
    ) -> None:
        self.root_dir = Path(root_dir)
        self.root_dir.mkdir(parents=True, exist_ok=True)
        self.completed_ttl_seconds = completed_ttl_seconds
        self.failed_ttl_seconds = failed_ttl_seconds
        self.cancelled_ttl_seconds = cancelled_ttl_seconds
        self._jobs: dict[str, JobRecord] = {}

    def create_job(self, *, job_id: str | None = None, now: datetime | None = None) -> dict[str, Any]:
        created_at = _as_aware_utc(now)
        job_id = job_id or uuid4().hex
        work_dir = self.root_dir / job_id
        work_dir.mkdir(parents=True, exist_ok=False)

        record = JobRecord(
            job_id=job_id,
            work_dir=work_dir,
            created_at=created_at,
            updated_at=created_at,
        )
        self._jobs[job_id] = record
        return record.snapshot()

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        record = self._jobs.get(job_id)
        if record is None:
            return None
        return record.snapshot()

    def start_worker(
        self,
        job_id: str,
        *,
        process_pid: int | None = None,
        stage: str = "running",
        message: str = "Worker started",
        progress_pct: float = 0,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        return self.update_job(
            job_id,
            status="running",
            stage=stage,
            message=message,
            progress_pct=progress_pct,
            process_pid=process_pid,
            now=now,
        )

    def update_job(
        self,
        job_id: str,
        *,
        status: str | None = None,
        stage: str | None = None,
        message: str | None = None,
        progress_pct: float | int | None = None,
        result_path: str | Path | None = None,
        error: str | None = None,
        process_pid: int | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        record = self._require_job(job_id)
        updated_at = _as_aware_utc(now)

        if status is not None:
            record.status = status
            if status in TERMINAL_STATUSES:
                record.terminal_at = updated_at
                record.expires_at = updated_at + timedelta(
                    seconds=self._retention_seconds(status)
                )
            else:
                record.terminal_at = None
                record.expires_at = None

        if stage is not None:
            record.stage = stage
        if message is not None:
            record.message = message
        if progress_pct is not None:
            record.progress_pct = progress_pct
        if result_path is not None:
            record.result_path = Path(result_path)
        if error is not None:
            record.error = error
        if process_pid is not None:
            record.process_pid = process_pid

        record.updated_at = updated_at
        return record.snapshot()

    def cancel_job(self, job_id: str, *, now: datetime | None = None) -> dict[str, Any]:
        record = self._require_job(job_id)
        updated_at = _as_aware_utc(now)
        record.status = "cancelled"
        record.stage = "cancelled"
        record.message = "Cancelled"
        record.progress_pct = 100
        record.terminal_at = updated_at
        record.expires_at = updated_at + timedelta(seconds=self.cancelled_ttl_seconds)
        record.updated_at = updated_at
        self._remove_work_dir(record.work_dir)
        return record.snapshot()

    def reap_expired_jobs(self, *, now: datetime | None = None) -> list[str]:
        current = _as_aware_utc(now)
        removed: list[str] = []
        for job_id, record in list(self._jobs.items()):
            if record.expires_at is None or record.expires_at > current:
                continue
            self._remove_work_dir(record.work_dir)
            removed.append(job_id)
            del self._jobs[job_id]
        return removed

    def reap_orphan_work_dirs(self) -> list[str]:
        removed: list[str] = []
        for child in self.root_dir.iterdir():
            if not child.is_dir():
                continue
            if child.name in self._jobs:
                continue
            self._remove_work_dir(child)
            removed.append(child.name)
        return removed

    def _require_job(self, job_id: str) -> JobRecord:
        record = self._jobs.get(job_id)
        if record is None:
            raise KeyError(f"Unknown job_id: {job_id}")
        return record

    def _retention_seconds(self, status: str) -> int:
        if status == "completed":
            return self.completed_ttl_seconds
        if status == "failed":
            return self.failed_ttl_seconds
        if status == "cancelled":
            return self.cancelled_ttl_seconds
        return 0

    @staticmethod
    def _remove_work_dir(path: Path) -> None:
        shutil.rmtree(path, ignore_errors=True)
