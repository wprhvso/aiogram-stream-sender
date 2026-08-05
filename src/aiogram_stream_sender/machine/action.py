from dataclasses import dataclass

from aiogram_stream_sender.errors import Failure
from aiogram_stream_sender.message.intent import ActionKind, Intent


@dataclass(frozen=True, slots=True)
class ScopedAction:
    stream_id: int
    index: int
    thread_id: int | None
    intent: Intent
    kind: ActionKind


@dataclass(frozen=True, slots=True)
class Result:
    ok: bool = True
    message_id: int | None = None
    failure: Failure | None = None
    reason: str = ""
    retry_after: float | None = None
