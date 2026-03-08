"""Agent core module."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = ["AgentLoop", "ContextBuilder", "MemoryStore", "SkillsLoader"]


def __getattr__(name: str) -> Any:
    if name == "AgentLoop":
        return import_module("nanobot.agent.loop").AgentLoop
    if name == "ContextBuilder":
        return import_module("nanobot.agent.context").ContextBuilder
    if name == "MemoryStore":
        return import_module("nanobot.agent.memory").MemoryStore
    if name == "SkillsLoader":
        return import_module("nanobot.agent.skills").SkillsLoader
    raise AttributeError(name)
