import logging
from collections.abc import Callable
from dataclasses import dataclass

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class MessageFailed:
    bot_id: int
    chat_id: int
    stream_id: int
    index: int
    reason: str
    # The exception itself, where there was one. Result carries it for exactly
    # this reason: a string cannot be written into a trace with a stack trace
    # behind it, and the sink is the only place that could write one.
    error: BaseException | None = None


@dataclass(frozen=True, slots=True)
class StreamFailed:
    bot_id: int
    chat_id: int
    stream_id: int
    reason: str
    error: BaseException | None = None


@dataclass(frozen=True, slots=True)
class ChatHold:
    bot_id: int
    chat_id: int
    until: float


Event = MessageFailed | StreamFailed | ChatHold
EventSink = Callable[[Event], None]


def emit(sink: EventSink | None, event: Event) -> None:
    if sink is None:
        return
    try:
        sink(event)
    except Exception:
        log.exception("event sink failed: %r", event)
