import shutil
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from analyzers.jtl_jobs import JobRegistry


class JobRegistryTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = Path.cwd() / ".tmp-test" / "jtl-jobs" / next(tempfile._get_candidate_names())
        self.tmpdir.mkdir(parents=True, exist_ok=False)
        self.addCleanup(lambda: shutil.rmtree(self.tmpdir, ignore_errors=True))
        self.registry = JobRegistry(self.tmpdir)

    def test_create_job_returns_queued_snapshot_with_job_id(self):
        snapshot = self.registry.create_job()

        self.assertIsInstance(snapshot["job_id"], str)
        self.assertEqual(snapshot["status"], "queued")
        self.assertEqual(snapshot["stage"], "queued")
        self.assertEqual(snapshot["progress_pct"], 0)
        self.assertTrue(Path(snapshot["work_dir"]).is_dir())
        self.assertIsNone(snapshot["result_path"])
        self.assertIsNone(snapshot["error"])
        self.assertIsNone(snapshot["process_pid"])

    def test_start_worker_and_update_job_change_status_fields(self):
        created = datetime(2026, 4, 20, 10, 0, tzinfo=timezone.utc)
        snapshot = self.registry.create_job(now=created)

        running = self.registry.start_worker(
            snapshot["job_id"],
            process_pid=4321,
            stage="parsing_run1",
            message="Parsing run 1",
            progress_pct=17,
            now=created + timedelta(seconds=2),
        )
        updated = self.registry.update_job(
            snapshot["job_id"],
            stage="aggregating_run1",
            message="Aggregating run 1",
            progress_pct=81,
            now=created + timedelta(seconds=5),
        )

        self.assertEqual(running["status"], "running")
        self.assertEqual(running["process_pid"], 4321)
        self.assertEqual(running["stage"], "parsing_run1")
        self.assertEqual(running["progress_pct"], 17)

        self.assertEqual(updated["status"], "running")
        self.assertEqual(updated["stage"], "aggregating_run1")
        self.assertEqual(updated["message"], "Aggregating run 1")
        self.assertEqual(updated["progress_pct"], 81)
        self.assertEqual(updated["process_pid"], 4321)

    def test_cancel_job_marks_cancelled_and_removes_work_dir(self):
        snapshot = self.registry.create_job()
        work_dir = Path(snapshot["work_dir"])
        (work_dir / "result.json").write_text("{}", encoding="utf-8")

        cancelled = self.registry.cancel_job(snapshot["job_id"])

        self.assertEqual(cancelled["status"], "cancelled")
        self.assertEqual(cancelled["stage"], "cancelled")
        self.assertEqual(cancelled["progress_pct"], 100)
        self.assertFalse(work_dir.exists())

    def test_completed_job_is_reaped_after_ttl(self):
        created = datetime(2026, 4, 20, 10, 0, tzinfo=timezone.utc)
        registry = JobRegistry(self.tmpdir, completed_ttl_seconds=30)
        snapshot = registry.create_job(now=created)
        registry.start_worker(snapshot["job_id"], now=created + timedelta(seconds=1))
        registry.update_job(
            snapshot["job_id"],
            status="completed",
            result_path=Path(snapshot["work_dir"]) / "result.json",
            progress_pct=100,
            stage="completed",
            message="Done",
            now=created + timedelta(seconds=2),
        )

        self.assertEqual(registry.reap_expired_jobs(now=created + timedelta(seconds=20)), [])
        removed = registry.reap_expired_jobs(now=created + timedelta(seconds=35))

        self.assertEqual(removed, [snapshot["job_id"]])
        self.assertFalse(Path(snapshot["work_dir"]).exists())
        self.assertIsNone(registry.get_job(snapshot["job_id"]))


if __name__ == "__main__":
    unittest.main()
