"""Shared path helpers for durable runtime state."""

from pathlib import Path

from nanobot.utils.helpers import ensure_dir, get_data_path


def get_workspace_state_dir(workspace: Path) -> Path:
    """Return the workspace-local state directory."""
    return ensure_dir(workspace / ".nanobot")


def get_workspace_state_db_path(workspace: Path) -> Path:
    """Return the workspace-local SQLite database path."""
    return get_workspace_state_dir(workspace) / "state.db"


def get_legacy_cron_store_path() -> Path:
    """Return the legacy global cron store path."""
    return ensure_dir(get_data_path() / "cron") / "jobs.json"
