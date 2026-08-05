from dataclasses import dataclass
from typing import Literal

from aiogram_stream_sender.chunk import Chunk
from aiogram_stream_sender.message.intent import (
    DeleteIntent,
    EditIntent,
    Intent,
    SendIntent,
)

MessageState = Literal["pending", "live", "dead"]


@dataclass(slots=True)
class SenderMessage:
    desired: Chunk | None = None
    delivered_hash: str | None = None
    message_id: int | None = None
    attempts: int = 0
    state: MessageState = "pending"

    def set_desired(self, chunk: Chunk) -> None:
        if self.state == "dead":
            return
        self.desired = chunk

    def mark_for_deletion(self) -> None:
        if self.state == "dead":
            return
        self.desired = None

    def intent(self) -> Intent | None:
        if self.state == "dead":
            return None
        if self.desired is None:
            if self.message_id is None:
                return None
            return DeleteIntent(message_id=self.message_id)
        if self.message_id is None:
            return SendIntent(chunk=self.desired)
        if self.desired.content_hash != self.delivered_hash:
            return EditIntent(message_id=self.message_id, chunk=self.desired)
        return None

    def on_success(self, intent: Intent, message_id: int | None) -> None:
        self.attempts = 0
        if isinstance(intent, SendIntent):
            if message_id is None:
                return
            self.message_id = message_id
            self.delivered_hash = intent.chunk.content_hash
            self.state = "live"
        elif isinstance(intent, EditIntent):
            self.delivered_hash = intent.chunk.content_hash
        elif isinstance(intent, DeleteIntent):
            self.state = "dead"

    def on_failure(self, *, terminal: bool) -> None:
        if terminal:
            self.state = "dead"
            return
        self.attempts += 1

    @property
    def is_settled(self) -> bool:
        return self.intent() is None
