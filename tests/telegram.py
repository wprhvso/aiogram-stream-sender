import asyncio
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from aiogram import Bot
from aiogram.client.session.base import BaseSession
from aiogram.exceptions import TelegramBadRequest
from aiogram.methods import (
    DeleteMessage,
    EditMessageText,
    SendChatAction,
    SendMessage,
    TelegramMethod,
)
from aiogram.types import Chat, Message

TOKEN = "42:TESTTESTTESTTESTTESTTESTTESTTESTTEST"


@dataclass(slots=True)
class Delivered:
    message_id: int
    text: str
    thread_id: int | None
    entities: tuple[dict[str, Any], ...] = ()
    edits: int = 0
    deleted: bool = False


@dataclass(slots=True)
class FakeTelegram(BaseSession):
    chat: dict[int, Delivered] = field(default_factory=dict)
    calls: list[TelegramMethod[Any]] = field(default_factory=list)
    actions: list[str] = field(default_factory=list)
    bot_ids: list[int] = field(default_factory=list)
    faults: dict[type[TelegramMethod[Any]], list[Exception]] = field(
        default_factory=dict
    )
    next_message_id: int = 1000

    def __post_init__(self) -> None:
        BaseSession.__init__(self)

    def fail(
        self, method: type[TelegramMethod[Any]], error: Exception, times: int = 1
    ) -> None:
        self.faults.setdefault(method, []).extend([error] * times)

    @property
    def live(self) -> list[Delivered]:
        return [item for item in self.chat.values() if not item.deleted]

    @property
    def texts(self) -> list[str]:
        return [item.text for item in self.live]

    def counted(self, method: type[TelegramMethod[Any]]) -> int:
        return sum(1 for call in self.calls if isinstance(call, method))

    async def close(self) -> None:
        return None

    async def stream_content(
        self,
        url: str,
        headers: dict[str, Any] | None = None,
        timeout: int = 30,
        chunk_size: int = 65536,
        raise_for_status: bool = True,
    ) -> AsyncGenerator[bytes]:
        _ = (url, headers, timeout, chunk_size, raise_for_status)
        yield b""

    async def make_request(
        self, bot: Bot, method: TelegramMethod[Any], timeout: int | None = None
    ) -> Any:
        _ = timeout
        self.bot_ids.append(bot.id)
        self.calls.append(method)
        await asyncio.sleep(0)

        pending = self.faults.get(type(method))
        if pending:
            raise pending.pop(0)

        if isinstance(method, SendMessage):
            return self._send(method)
        if isinstance(method, EditMessageText):
            return self._edit(method)
        if isinstance(method, DeleteMessage):
            return self._delete(method)
        if isinstance(method, SendChatAction):
            self.actions.append(method.action)
            return True
        raise AssertionError(type(method).__name__)

    def _send(self, method: SendMessage) -> Message:
        self.next_message_id += 1
        entities = tuple(
            entity.model_dump(exclude_none=True) for entity in method.entities or ()
        )
        self.chat[self.next_message_id] = Delivered(
            message_id=self.next_message_id,
            text=method.text,
            thread_id=method.message_thread_id,
            entities=entities,
        )
        return Message(
            message_id=self.next_message_id,
            date=datetime.now(UTC),
            chat=Chat(id=int(str(method.chat_id)), type="private"),
            text=method.text,
        )

    def _edit(self, method: EditMessageText) -> Message:
        target = self._require(method.message_id, method)
        if target.text == method.text:
            raise TelegramBadRequest(
                method=method,
                message="Bad Request: message is not modified",
            )
        target.text = method.text or ""
        target.edits += 1
        return Message(
            message_id=target.message_id,
            date=datetime.now(UTC),
            chat=Chat(id=int(str(method.chat_id)), type="private"),
            text=target.text,
        )

    def _delete(self, method: DeleteMessage) -> bool:
        target = self._require(method.message_id, method)
        target.deleted = True
        return True

    def _require(
        self, message_id: int | None, method: TelegramMethod[Any]
    ) -> Delivered:
        target = self.chat.get(message_id or 0)
        if target is None or target.deleted:
            raise TelegramBadRequest(
                method=method,
                message="Bad Request: message to edit not found",
            )
        return target


def make_bot(session: FakeTelegram) -> Bot:
    return Bot(token=TOKEN, session=session)


def message_update(update_id: int, chat_id: int, text: str) -> dict[str, Any]:
    return {
        "update_id": update_id,
        "message": {
            "message_id": update_id,
            "date": int(datetime.now(UTC).timestamp()),
            "chat": {"id": chat_id, "type": "private"},
            "from": {"id": 7, "is_bot": False, "first_name": "Tester"},
            "text": text,
        },
    }
