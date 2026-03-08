"""Typed runtime models for normalized turn processing."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal
from uuid import uuid4


def _new_turn_id() -> str:
    return uuid4().hex[:12]


@dataclass(frozen=True)
class ReplyTarget:
    """How a turn should route its response back to the user."""

    channel: str
    chat_id: str
    reply_to: str | None = None
    thread_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TurnRequest:
    """Normalized inbound turn consumed by the orchestration layer."""

    content: str
    reply_target: ReplyTarget
    sender_id: str = "user"
    session_key: str | None = None
    kind: Literal["user", "system", "cron", "heartbeat", "direct"] = "user"
    media: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)
    turn_id: str = field(default_factory=_new_turn_id)
    created_at: datetime = field(default_factory=datetime.now)

    @property
    def resolved_session_key(self) -> str:
        return self.session_key or f"{self.reply_target.channel}:{self.reply_target.chat_id}"


@dataclass(frozen=True)
class TurnEvent:
    """Intermediate event emitted while a turn is being processed."""

    kind: Literal["progress", "info", "warning", "error"] = "progress"
    content: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ToolCallEvent(TurnEvent):
    """Structured event for a tool call."""

    tool_name: str = ""
    arguments: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TurnResult:
    """Normalized turn output returned by the orchestration layer."""

    content: str
    reply_target: ReplyTarget
    turn_id: str
    tools_used: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)
    events: tuple[TurnEvent, ...] = ()
