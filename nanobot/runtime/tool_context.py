"""Request-scoped tool execution context."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass

from nanobot.runtime.turns import ReplyTarget


@dataclass(frozen=True)
class ToolExecutionContext:
    """Immutable per-turn context visible to tool implementations."""

    turn_id: str
    session_key: str
    kind: str
    reply_target: ReplyTarget


_CURRENT_TOOL_CONTEXT: ContextVar[ToolExecutionContext | None] = ContextVar(
    "nanobot_tool_context", default=None
)


def get_tool_context() -> ToolExecutionContext | None:
    """Return the current request-scoped tool context, if any."""

    return _CURRENT_TOOL_CONTEXT.get()


@contextmanager
def bind_tool_context(ctx: ToolExecutionContext):
    """Bind tool context for the current task."""

    token: Token[ToolExecutionContext | None] = _CURRENT_TOOL_CONTEXT.set(ctx)
    try:
        yield ctx
    finally:
        _CURRENT_TOOL_CONTEXT.reset(token)
