from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Literal

from aiogram_stream_sender.chunk import Chunk
from aiogram_stream_sender.errors import StreamFinalizedError
from aiogram_stream_sender.message.intent import DeleteIntent, Intent
from aiogram_stream_sender.message.message import SenderMessage

StreamState = Literal["active", "finished", "dead"]


@dataclass(slots=True)
class SenderStream:
    stream_id: int
    thread_id: int | None = None
    messages: list[SenderMessage] = field(default_factory=list)
    is_final: bool = False
    state: StreamState = "active"
    reason: str | None = None

    def update(self, chunks: Sequence[Chunk]) -> None:
        if self.is_final:
            raise StreamFinalizedError(str(self.stream_id))
        if self.state != "active":
            return
        for index, chunk in enumerate(chunks):
            if index < len(self.messages):
                self.messages[index].set_desired(chunk)
            else:
                self.messages.append(SenderMessage(desired=chunk))
        tail = self.messages[len(chunks) :]
        for message in tail:
            message.mark_for_deletion()
        self.messages = self.messages[: len(chunks)] + [
            message
            for message in tail
            if message.message_id is not None and message.state != "dead"
        ]

    def finalize(self) -> None:
        self.is_final = True
        self._refresh()

    def pending(self) -> tuple[int, Intent] | None:
        for index, message in enumerate(self.messages):
            intent = message.intent()
            if intent is None or isinstance(intent, DeleteIntent):
                continue
            return index, intent
        for index in reversed(range(len(self.messages))):
            intent = self.messages[index].intent()
            if isinstance(intent, DeleteIntent):
                return index, intent
        return None

    def attempts_at(self, index: int) -> int:
        if 0 <= index < len(self.messages):
            return self.messages[index].attempts
        return 0

    def apply_success(self, index: int, intent: Intent, message_id: int | None) -> None:
        if not 0 <= index < len(self.messages):
            return
        self.messages[index].on_success(intent, message_id)
        if isinstance(intent, DeleteIntent) and index == len(self.messages) - 1:
            self.messages.pop()
        self._refresh()

    def apply_failure(self, index: int, *, terminal: bool) -> None:
        if not 0 <= index < len(self.messages):
            return
        self.messages[index].on_failure(terminal=terminal)
        self._refresh()

    def kill(self, reason: str) -> None:
        for message in self.messages:
            if message.state != "dead":
                message.state = "dead"
        self.state = "dead"
        self.reason = reason

    def _refresh(self) -> None:
        if self.state != "active":
            return
        if self.is_final and self.pending() is None:
            self.state = "finished"

    @property
    def message_ids(self) -> list[int]:
        return [
            message.message_id
            for message in self.messages
            if message.message_id is not None and message.state != "dead"
        ]

    @property
    def has_dead_messages(self) -> bool:
        return any(message.state == "dead" for message in self.messages)

    @property
    def is_done(self) -> bool:
        return self.state in ("finished", "dead")
