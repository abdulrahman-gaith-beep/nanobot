"""Cron service for scheduling agent tasks."""

import asyncio
import json
import sqlite3
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Coroutine

from loguru import logger

from nanobot.cron.types import CronJob, CronJobState, CronPayload, CronSchedule, CronStore


def _now_ms() -> int:
    return int(time.time() * 1000)


def _compute_next_run(schedule: CronSchedule, now_ms: int) -> int | None:
    """Compute next run time in ms."""
    if schedule.kind == "at":
        return schedule.at_ms if schedule.at_ms and schedule.at_ms > now_ms else None

    if schedule.kind == "every":
        if not schedule.every_ms or schedule.every_ms <= 0:
            return None
        # Next interval from now
        return now_ms + schedule.every_ms

    if schedule.kind == "cron" and schedule.expr:
        try:
            from zoneinfo import ZoneInfo

            from croniter import croniter
            # Use caller-provided reference time for deterministic scheduling
            base_time = now_ms / 1000
            tz = ZoneInfo(schedule.tz) if schedule.tz else datetime.now().astimezone().tzinfo
            base_dt = datetime.fromtimestamp(base_time, tz=tz)
            cron = croniter(schedule.expr, base_dt)
            next_dt = cron.get_next(datetime)
            return int(next_dt.timestamp() * 1000)
        except Exception:
            return None

    return None


class CronService:
    """Service for managing and executing scheduled jobs."""

    def __init__(
        self,
        store_path: Path,
        on_job: Callable[[CronJob], Coroutine[Any, Any, str | None]] | None = None,
        legacy_store_path: Path | None = None,
    ):
        self.store_path = store_path if store_path.suffix == ".db" else store_path.with_suffix(".db")
        self.legacy_store_path = legacy_store_path or (
            store_path if store_path.suffix == ".json" else None
        )
        self.on_job = on_job  # Callback to execute job, returns response text
        self._store: CronStore | None = None
        self._timer_task: asyncio.Task | None = None
        self._running = False
        self._db_initialized = False

    def _connect(self) -> sqlite3.Connection:
        """Open the cron SQLite database."""
        self.store_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.store_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_db(self, conn: sqlite3.Connection) -> None:
        """Create required tables if they do not exist."""
        if self._db_initialized:
            return
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cron_jobs (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                enabled INTEGER NOT NULL,
                schedule_kind TEXT NOT NULL,
                schedule_at_ms INTEGER,
                schedule_every_ms INTEGER,
                schedule_expr TEXT,
                schedule_tz TEXT,
                payload_kind TEXT NOT NULL,
                payload_message TEXT NOT NULL,
                payload_deliver INTEGER NOT NULL,
                payload_channel TEXT,
                payload_to TEXT,
                state_next_run_at_ms INTEGER,
                state_last_run_at_ms INTEGER,
                state_last_status TEXT,
                state_last_error TEXT,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                delete_after_run INTEGER NOT NULL
            )
            """
        )
        self._db_initialized = True

    @staticmethod
    def _row_to_job(row: sqlite3.Row) -> CronJob:
        """Convert a database row into a CronJob."""
        return CronJob(
            id=row["id"],
            name=row["name"],
            enabled=bool(row["enabled"]),
            schedule=CronSchedule(
                kind=row["schedule_kind"],
                at_ms=row["schedule_at_ms"],
                every_ms=row["schedule_every_ms"],
                expr=row["schedule_expr"],
                tz=row["schedule_tz"],
            ),
            payload=CronPayload(
                kind=row["payload_kind"],
                message=row["payload_message"],
                deliver=bool(row["payload_deliver"]),
                channel=row["payload_channel"],
                to=row["payload_to"],
            ),
            state=CronJobState(
                next_run_at_ms=row["state_next_run_at_ms"],
                last_run_at_ms=row["state_last_run_at_ms"],
                last_status=row["state_last_status"],
                last_error=row["state_last_error"],
            ),
            created_at_ms=row["created_at_ms"],
            updated_at_ms=row["updated_at_ms"],
            delete_after_run=bool(row["delete_after_run"]),
        )

    def _upsert_job(self, conn: sqlite3.Connection, job: CronJob) -> None:
        """Insert or update a single job row."""
        conn.execute(
            """
            INSERT INTO cron_jobs (
                id,
                name,
                enabled,
                schedule_kind,
                schedule_at_ms,
                schedule_every_ms,
                schedule_expr,
                schedule_tz,
                payload_kind,
                payload_message,
                payload_deliver,
                payload_channel,
                payload_to,
                state_next_run_at_ms,
                state_last_run_at_ms,
                state_last_status,
                state_last_error,
                created_at_ms,
                updated_at_ms,
                delete_after_run
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                enabled = excluded.enabled,
                schedule_kind = excluded.schedule_kind,
                schedule_at_ms = excluded.schedule_at_ms,
                schedule_every_ms = excluded.schedule_every_ms,
                schedule_expr = excluded.schedule_expr,
                schedule_tz = excluded.schedule_tz,
                payload_kind = excluded.payload_kind,
                payload_message = excluded.payload_message,
                payload_deliver = excluded.payload_deliver,
                payload_channel = excluded.payload_channel,
                payload_to = excluded.payload_to,
                state_next_run_at_ms = excluded.state_next_run_at_ms,
                state_last_run_at_ms = excluded.state_last_run_at_ms,
                state_last_status = excluded.state_last_status,
                state_last_error = excluded.state_last_error,
                created_at_ms = excluded.created_at_ms,
                updated_at_ms = excluded.updated_at_ms,
                delete_after_run = excluded.delete_after_run
            """,
            (
                job.id,
                job.name,
                int(job.enabled),
                job.schedule.kind,
                job.schedule.at_ms,
                job.schedule.every_ms,
                job.schedule.expr,
                job.schedule.tz,
                job.payload.kind,
                job.payload.message,
                int(job.payload.deliver),
                job.payload.channel,
                job.payload.to,
                job.state.next_run_at_ms,
                job.state.last_run_at_ms,
                job.state.last_status,
                job.state.last_error,
                job.created_at_ms,
                job.updated_at_ms,
                int(job.delete_after_run),
            ),
        )

    def _write_store(self, conn: sqlite3.Connection, store: CronStore) -> None:
        """Persist the in-memory store to SQLite."""
        conn.execute("DELETE FROM cron_jobs")
        for job in store.jobs:
            self._upsert_job(conn, job)

    def _import_legacy_store(self, conn: sqlite3.Connection) -> None:
        """Import legacy jobs.json records that are not yet present in SQLite."""
        if self.legacy_store_path is None or not self.legacy_store_path.exists():
            return

        try:
            data = json.loads(self.legacy_store_path.read_text())
        except Exception as e:
            logger.warning(f"Failed to read legacy cron store: {e}")
            return

        existing_ids = {
            row["id"] for row in conn.execute("SELECT id FROM cron_jobs").fetchall()
        }
        imported = 0
        for raw_job in data.get("jobs", []):
            job_id = raw_job["id"]
            if job_id in existing_ids:
                continue
            self._upsert_job(
                conn,
                CronJob(
                    id=job_id,
                    name=raw_job["name"],
                    enabled=raw_job.get("enabled", True),
                    schedule=CronSchedule(
                        kind=raw_job["schedule"]["kind"],
                        at_ms=raw_job["schedule"].get("atMs"),
                        every_ms=raw_job["schedule"].get("everyMs"),
                        expr=raw_job["schedule"].get("expr"),
                        tz=raw_job["schedule"].get("tz"),
                    ),
                    payload=CronPayload(
                        kind=raw_job["payload"].get("kind", "agent_turn"),
                        message=raw_job["payload"].get("message", ""),
                        deliver=raw_job["payload"].get("deliver", False),
                        channel=raw_job["payload"].get("channel"),
                        to=raw_job["payload"].get("to"),
                    ),
                    state=CronJobState(
                        next_run_at_ms=raw_job.get("state", {}).get("nextRunAtMs"),
                        last_run_at_ms=raw_job.get("state", {}).get("lastRunAtMs"),
                        last_status=raw_job.get("state", {}).get("lastStatus"),
                        last_error=raw_job.get("state", {}).get("lastError"),
                    ),
                    created_at_ms=raw_job.get("createdAtMs", 0),
                    updated_at_ms=raw_job.get("updatedAtMs", 0),
                    delete_after_run=raw_job.get("deleteAfterRun", False),
                ),
            )
            imported += 1

        if imported:
            logger.info(f"Imported {imported} legacy cron jobs into SQLite store")

    def _load_store(self) -> CronStore:
        """Load jobs from disk."""
        if self._store:
            return self._store

        try:
            with self._connect() as conn:
                self._ensure_db(conn)
                self._import_legacy_store(conn)
                rows = conn.execute(
                    "SELECT * FROM cron_jobs ORDER BY created_at_ms ASC, id ASC"
                ).fetchall()
                self._store = CronStore(jobs=[self._row_to_job(row) for row in rows])
                conn.commit()
        except Exception as e:
            logger.warning(f"Failed to load cron store: {e}")
            self._store = CronStore()

        return self._store

    def _save_store(self) -> None:
        """Save jobs to disk."""
        if not self._store:
            return

        with self._connect() as conn:
            self._ensure_db(conn)
            self._write_store(conn, self._store)
            conn.commit()

    async def start(self) -> None:
        """Start the cron service."""
        self._running = True
        self._load_store()
        self._recompute_next_runs()
        self._save_store()
        self._arm_timer()
        logger.info(f"Cron service started with {len(self._store.jobs if self._store else [])} jobs")

    def stop(self) -> None:
        """Stop the cron service."""
        self._running = False
        if self._timer_task:
            self._timer_task.cancel()
            self._timer_task = None

    def _recompute_next_runs(self) -> None:
        """Recompute next run times for all enabled jobs."""
        if not self._store:
            return
        now = _now_ms()
        for job in self._store.jobs:
            if job.enabled:
                job.state.next_run_at_ms = _compute_next_run(job.schedule, now)

    def _get_next_wake_ms(self) -> int | None:
        """Get the earliest next run time across all jobs."""
        if not self._store:
            return None
        times = [j.state.next_run_at_ms for j in self._store.jobs
                 if j.enabled and j.state.next_run_at_ms]
        return min(times) if times else None

    def _arm_timer(self) -> None:
        """Schedule the next timer tick."""
        if self._timer_task:
            self._timer_task.cancel()

        next_wake = self._get_next_wake_ms()
        if not next_wake or not self._running:
            return

        delay_ms = max(0, next_wake - _now_ms())
        delay_s = delay_ms / 1000

        async def tick():
            await asyncio.sleep(delay_s)
            if self._running:
                await self._on_timer()

        self._timer_task = asyncio.create_task(tick())

    async def _on_timer(self) -> None:
        """Handle timer tick - run due jobs."""
        if not self._store:
            return

        now = _now_ms()
        due_jobs = [
            j for j in self._store.jobs
            if j.enabled and j.state.next_run_at_ms and now >= j.state.next_run_at_ms
        ]

        for job in due_jobs:
            await self._execute_job(job)

        self._save_store()
        self._arm_timer()

    async def _execute_job(self, job: CronJob) -> None:
        """Execute a single job."""
        start_ms = _now_ms()
        logger.info(f"Cron: executing job '{job.name}' ({job.id})")

        try:
            if self.on_job:
                await self.on_job(job)

            job.state.last_status = "ok"
            job.state.last_error = None
            logger.info(f"Cron: job '{job.name}' completed")

        except Exception as e:
            job.state.last_status = "error"
            job.state.last_error = str(e)
            logger.error(f"Cron: job '{job.name}' failed: {e}")

        job.state.last_run_at_ms = start_ms
        job.updated_at_ms = _now_ms()

        # Handle one-shot jobs
        if job.schedule.kind == "at":
            if job.delete_after_run:
                self._store.jobs = [j for j in self._store.jobs if j.id != job.id]
            else:
                job.enabled = False
                job.state.next_run_at_ms = None
        else:
            # Compute next run
            job.state.next_run_at_ms = _compute_next_run(job.schedule, _now_ms())

    # ========== Public API ==========

    def list_jobs(self, include_disabled: bool = False) -> list[CronJob]:
        """List all jobs."""
        store = self._load_store()
        jobs = store.jobs if include_disabled else [j for j in store.jobs if j.enabled]
        return sorted(jobs, key=lambda j: j.state.next_run_at_ms or float('inf'))

    def add_job(
        self,
        name: str,
        schedule: CronSchedule,
        message: str,
        deliver: bool = False,
        channel: str | None = None,
        to: str | None = None,
        delete_after_run: bool = False,
    ) -> CronJob:
        """Add a new job."""
        store = self._load_store()
        now = _now_ms()

        job = CronJob(
            id=str(uuid.uuid4())[:8],
            name=name,
            enabled=True,
            schedule=schedule,
            payload=CronPayload(
                kind="agent_turn",
                message=message,
                deliver=deliver,
                channel=channel,
                to=to,
            ),
            state=CronJobState(next_run_at_ms=_compute_next_run(schedule, now)),
            created_at_ms=now,
            updated_at_ms=now,
            delete_after_run=delete_after_run,
        )

        store.jobs.append(job)
        self._save_store()
        self._arm_timer()

        logger.info(f"Cron: added job '{name}' ({job.id})")
        return job

    def remove_job(self, job_id: str) -> bool:
        """Remove a job by ID."""
        store = self._load_store()
        before = len(store.jobs)
        store.jobs = [j for j in store.jobs if j.id != job_id]
        removed = len(store.jobs) < before

        if removed:
            self._save_store()
            self._arm_timer()
            logger.info(f"Cron: removed job {job_id}")

        return removed

    def enable_job(self, job_id: str, enabled: bool = True) -> CronJob | None:
        """Enable or disable a job."""
        store = self._load_store()
        for job in store.jobs:
            if job.id == job_id:
                job.enabled = enabled
                job.updated_at_ms = _now_ms()
                if enabled:
                    job.state.next_run_at_ms = _compute_next_run(job.schedule, _now_ms())
                else:
                    job.state.next_run_at_ms = None
                self._save_store()
                self._arm_timer()
                return job
        return None

    async def run_job(self, job_id: str, force: bool = False) -> bool:
        """Manually run a job."""
        store = self._load_store()
        for job in store.jobs:
            if job.id == job_id:
                if not force and not job.enabled:
                    return False
                await self._execute_job(job)
                self._save_store()
                self._arm_timer()
                return True
        return False

    def status(self) -> dict:
        """Get service status."""
        store = self._load_store()
        return {
            "enabled": self._running,
            "jobs": len(store.jobs),
            "next_wake_at_ms": self._get_next_wake_ms(),
        }
