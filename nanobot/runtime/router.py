"""Compatibility router between bus messages and normalized turns."""

from __future__ import annotations

from uuid import uuid4

from nanobot.bus.events import InboundMessage, OutboundMessage
from nanobot.runtime.turns import ReplyTarget, TurnRequest, TurnResult


class ChannelRouter:
    """Translate legacy bus envelopes to normalized runtime turns."""

    def inbound_to_turn(
        self,
        msg: InboundMessage,
        session_key: str | None = None,
        kind: str = "user",
    ) -> TurnRequest:
        reply_target = ReplyTarget(
            channel=msg.channel,
            chat_id=msg.chat_id,
            reply_to=msg.reply_to,
            thread_id=msg.thread_id,
            metadata=dict(msg.metadata or {}),
        )
        return TurnRequest(
            content=msg.content,
            reply_target=reply_target,
            sender_id=msg.sender_id,
            session_key=session_key or msg.session_key,
            kind=kind,  # type: ignore[arg-type]
            media=tuple(msg.media or []),
            metadata=dict(msg.metadata or {}),
            turn_id=msg.turn_id or uuid4().hex[:12],
        )

    def result_to_outbound(self, result: TurnResult) -> OutboundMessage:
        metadata = dict(result.reply_target.metadata)
        metadata.update(result.metadata)
        return OutboundMessage(
            channel=result.reply_target.channel,
            chat_id=result.reply_target.chat_id,
            content=result.content,
            reply_to=result.reply_target.reply_to,
            thread_id=result.reply_target.thread_id,
            metadata=metadata,
            turn_id=result.turn_id,
        )

    def progress_to_outbound(self, content: str, request: TurnRequest) -> OutboundMessage:
        return OutboundMessage(
            channel=request.reply_target.channel,
            chat_id=request.reply_target.chat_id,
            content=content,
            reply_to=request.reply_target.reply_to,
            thread_id=request.reply_target.thread_id,
            metadata=dict(request.reply_target.metadata),
            turn_id=request.turn_id,
        )
