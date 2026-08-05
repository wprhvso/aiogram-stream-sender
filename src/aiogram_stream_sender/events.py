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


@dataclass(frozen=True, slots=True)
class StreamFailed:
    bot_id: int
    chat_id: int
    stream_id: int
    reason: str


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
