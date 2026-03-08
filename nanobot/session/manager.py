"""Session management for conversation history."""

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from loguru import logger

from nanobot.storage.paths import get_workspace_state_db_path
from nanobot.utils.helpers import ensure_dir, safe_filename


@dataclass
class Session:
    """
    A conversation session.

    Important: Messages are append-only for LLM cache efficiency.
    The consolidation process writes summaries to MEMORY.md/HISTORY.md
    but does NOT modify the messages list or get_history() output.
    """

    key: str  # channel:chat_id
    messages: list[dict[str, Any]] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    metadata: dict[str, Any] = field(default_factory=dict)
    last_consolidated: int = 0  # Number of messages already consolidated to files

    def add_message(self, role: str, content: str, **kwargs: Any) -> None:
        """Add a message to the session."""
        msg = {
            "role": role,
            "content": content,
            "timestamp": datetime.now().isoformat(),
            **kwargs
        }
        self.messages.append(msg)
        self.updated_at = datetime.now()

    def get_history(self, max_messages: int = 500) -> list[dict[str, Any]]:
        """Get recent messages in LLM format, preserving tool metadata."""
        out: list[dict[str, Any]] = []
        for m in self.messages[-max_messages:]:
            entry: dict[str, Any] = {"role": m["role"], "content": m.get("content", "")}
            for k in ("tool_calls", "tool_call_id", "name"):
                if k in m:
                    entry[k] = m[k]
            out.append(entry)
        return out

    def clear(self) -> None:
        """Clear all messages and reset session to initial state."""
        self.messages = []
        self.last_consolidated = 0
        self.updated_at = datetime.now()


class SessionManager:
    """
    Manages conversation sessions.

    Sessions are stored in a workspace-local SQLite database.
    Legacy JSONL sessions are imported on first use.
    """

    def __init__(self, workspace: Path, db_path: Path | None = None):
        self.workspace = workspace
        self.sessions_dir = ensure_dir(self.workspace / "sessions")
        self.legacy_sessions_dir = Path.home() / ".nanobot" / "sessions"
        self.db_path = db_path or get_workspace_state_db_path(self.workspace)
        self._cache: dict[str, Session] = {}
        self._db_initialized = False
        self._legacy_sessions_imported = False

    def _get_session_path(self, key: str) -> Path:
        """Get the file path for a session."""
        safe_key = safe_filename(key.replace(":", "_"))
        return self.sessions_dir / f"{safe_key}.jsonl"

    def _get_legacy_session_path(self, key: str) -> Path:
        """Legacy global session path (~/.nanobot/sessions/)."""
        safe_key = safe_filename(key.replace(":", "_"))
        return self.legacy_sessions_dir / f"{safe_key}.jsonl"

    def _connect(self) -> sqlite3.Connection:
        """Open a SQLite connection for session state."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _ensure_db(self, conn: sqlite3.Connection) -> None:
        """Create required tables if they do not exist."""
        if self._db_initialized:
            return
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                session_key TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                last_consolidated INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS session_messages (
                session_key TEXT NOT NULL,
                message_idx INTEGER NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (session_key, message_idx),
                FOREIGN KEY (session_key) REFERENCES sessions(session_key) ON DELETE CASCADE
            );
            """
        )
        self._db_initialized = True

    @staticmethod
    def _parse_legacy_session(key: str, path: Path) -> Session | None:
        """Parse a legacy JSONL session file."""
        try:
            messages = []
            metadata: dict[str, Any] = {}
            created_at = None
            updated_at = None
            last_consolidated = 0

            with open(path) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    data = json.loads(line)
                    if data.get("_type") == "metadata":
                        metadata = data.get("metadata", {})
                        created_at = (
                            datetime.fromisoformat(data["created_at"])
                            if data.get("created_at")
                            else None
                        )
                        updated_at = (
                            datetime.fromisoformat(data["updated_at"])
                            if data.get("updated_at")
                            else None
                        )
                        last_consolidated = data.get("last_consolidated", 0)
                    else:
                        messages.append(data)

            return Session(
                key=key,
                messages=messages,
                created_at=created_at or datetime.now(),
                updated_at=updated_at or created_at or datetime.now(),
                metadata=metadata,
                last_consolidated=last_consolidated,
            )
        except Exception as e:
            logger.warning(f"Failed to parse legacy session {key}: {e}")
            return None

    def _save_session(self, conn: sqlite3.Connection, session: Session) -> None:
        """Persist a session using the provided connection."""
        conn.execute(
            """
            INSERT INTO sessions (
                session_key, created_at, updated_at, metadata_json, last_consolidated
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(session_key) DO UPDATE SET
                created_at = excluded.created_at,
                updated_at = excluded.updated_at,
                metadata_json = excluded.metadata_json,
                last_consolidated = excluded.last_consolidated
            """,
            (
                session.key,
                session.created_at.isoformat(),
                session.updated_at.isoformat(),
                json.dumps(session.metadata),
                session.last_consolidated,
            ),
        )
        conn.execute(
            "DELETE FROM session_messages WHERE session_key = ?",
            (session.key,),
        )
        conn.executemany(
            """
            INSERT INTO session_messages (session_key, message_idx, payload_json)
            VALUES (?, ?, ?)
            """,
            [
                (session.key, idx, json.dumps(message))
                for idx, message in enumerate(session.messages)
            ],
        )

    def _session_exists(self, conn: sqlite3.Connection, key: str) -> bool:
        """Return whether a session already exists in SQLite."""
        return (
            conn.execute(
                "SELECT 1 FROM sessions WHERE session_key = ? LIMIT 1",
                (key,),
            ).fetchone()
            is not None
        )

    def _load_session(self, conn: sqlite3.Connection, key: str) -> Session | None:
        """Load a session from SQLite."""
        row = conn.execute(
            """
            SELECT session_key, created_at, updated_at, metadata_json, last_consolidated
            FROM sessions
            WHERE session_key = ?
            """,
            (key,),
        ).fetchone()
        if row is None:
            return None

        messages = [
            json.loads(message_row["payload_json"])
            for message_row in conn.execute(
                """
                SELECT payload_json
                FROM session_messages
                WHERE session_key = ?
                ORDER BY message_idx
                """,
                (key,),
            ).fetchall()
        ]
        return Session(
            key=key,
            messages=messages,
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
            metadata=json.loads(row["metadata_json"]),
            last_consolidated=row["last_consolidated"],
        )

    def _import_legacy_session_if_needed(
        self,
        conn: sqlite3.Connection,
        key: str,
    ) -> Session | None:
        """Import a legacy session file into SQLite if it exists."""
        for path in (self._get_session_path(key), self._get_legacy_session_path(key)):
            if not path.exists():
                continue
            session = self._parse_legacy_session(key, path)
            if session is None:
                return None
            self._save_session(conn, session)
            logger.info(f"Imported legacy session {key} into SQLite store")
            return session
        return None

    def _maybe_import_legacy_sessions(self) -> None:
        """Import all legacy session files once for session listing and migration."""
        if self._legacy_sessions_imported:
            return

        with self._connect() as conn:
            self._ensure_db(conn)
            for directory in (self.sessions_dir, self.legacy_sessions_dir):
                if not directory.exists():
                    continue
                for path in sorted(directory.glob("*.jsonl")):
                    key = path.stem.replace("_", ":")
                    if self._session_exists(conn, key):
                        continue
                    session = self._parse_legacy_session(key, path)
                    if session is None:
                        continue
                    self._save_session(conn, session)
            conn.commit()

        self._legacy_sessions_imported = True

    def get_or_create(self, key: str) -> Session:
        """
        Get an existing session or create a new one.

        Args:
            key: Session key (usually channel:chat_id).

        Returns:
            The session.
        """
        if key in self._cache:
            return self._cache[key]

        session = self._load(key)
        if session is None:
            session = Session(key=key)

        self._cache[key] = session
        return session

    def _load(self, key: str) -> Session | None:
        """Load a session from durable storage."""
        try:
            with self._connect() as conn:
                self._ensure_db(conn)
                session = self._load_session(conn, key)
                if session is not None:
                    return session

                imported = self._import_legacy_session_if_needed(conn, key)
                if imported is not None:
                    conn.commit()
                    return imported
        except Exception as e:
            logger.warning(f"Failed to load session {key}: {e}")
        return None

    def save(self, session: Session) -> None:
        """Save a session to durable storage."""
        with self._connect() as conn:
            self._ensure_db(conn)
            self._save_session(conn, session)
            conn.commit()
        self._cache[session.key] = session

    def invalidate(self, key: str) -> None:
        """Remove a session from the in-memory cache."""
        self._cache.pop(key, None)

    def list_sessions(self) -> list[dict[str, Any]]:
        """
        List all sessions.

        Returns:
            List of session info dicts.
        """
        self._maybe_import_legacy_sessions()
        with self._connect() as conn:
            self._ensure_db(conn)
            rows = conn.execute(
                """
                SELECT session_key, created_at, updated_at
                FROM sessions
                ORDER BY updated_at DESC
                """
            ).fetchall()
        return [
            {
                "key": row["session_key"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "path": str(self.db_path),
            }
            for row in rows
        ]
