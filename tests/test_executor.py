from types import SimpleNamespace
from typing import Any

from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramRetryAfter,
)
from aiogram.methods import SendMessage

from aiogram_stream_sender.chunk import Chunk
from aiogram_stream_sender.errors import Failure
from aiogram_stream_sender.machine.action import ScopedAction
from aiogram_stream_sender.message.intent import (
    ActionIntent,
    ActionKind,
    DeleteIntent,
    EditIntent,
    Intent,
    SendIntent,
)
from aiogram_stream_sender.transport.executor import TelegramExecutor


class RecordingBot:
    def __init__(self, error: Exception | None = None) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.error = error

    def _record(self, name: str, kwargs: dict[str, Any]) -> None:
        self.calls.append((name, kwargs))
        if self.error is not None:
            raise self.error

    async def send_message(self, **kwargs: Any) -> Any:
        self._record("send_message", kwargs)
        return SimpleNamespace(message_id=77)

    async def edit_message_text(self, **kwargs: Any) -> Any:
        self._record("edit_message_text", kwargs)
        return SimpleNamespace(message_id=kwargs["message_id"])

    async def delete_message(self, **kwargs: Any) -> bool:
        self._record("delete_message", kwargs)
        return True

    async def send_chat_action(self, **kwargs: Any) -> bool:
        self._record("send_chat_action", kwargs)
        return True


def _action(intent: Intent, kind: ActionKind) -> ScopedAction:
    return ScopedAction(stream_id=1, index=0, thread_id=5, intent=intent, kind=kind)


def _bad(message: str) -> TelegramBadRequest:
    return TelegramBadRequest(method=SendMessage(chat_id=1, text="x"), message=message)


async def test_send_passes_thread_and_entities() -> None:
    bot = RecordingBot()
    executor = TelegramExecutor(bot, 10)
    chunk = Chunk(text="hi", entities=({"type": "bold", "offset": 0, "length": 2},))

    result = await executor.execute(_action(SendIntent(chunk=chunk), ActionKind.SEND))

    name, kwargs = bot.calls[0]
    assert name == "send_message"
    assert kwargs["chat_id"] == 10
    assert kwargs["message_thread_id"] == 5
    assert kwargs["entities"][0].type == "bold"
    assert result.ok
    assert result.message_id == 77


async def test_edit_returns_same_message_id() -> None:
    bot = RecordingBot()
    executor = TelegramExecutor(bot, 10)
    intent = EditIntent(message_id=42, chunk=Chunk(text="hi"))

    result = await executor.execute(_action(intent, ActionKind.EDIT))

    assert bot.calls[0][0] == "edit_message_text"
    assert result.message_id == 42


async def test_delete_calls_api() -> None:
    bot = RecordingBot()
    executor = TelegramExecutor(bot, 10)

    result = await executor.execute(
        _action(DeleteIntent(message_id=42), ActionKind.DELETE)
    )

    assert bot.calls[0][0] == "delete_message"
    assert result.message_id == 42


async def test_action_sends_typing() -> None:
    bot = RecordingBot()
    executor = TelegramExecutor(bot, 10)

    result = await executor.execute(_action(ActionIntent(), ActionKind.ACTION))

    name, kwargs = bot.calls[0]
    assert name == "send_chat_action"
    assert kwargs["message_thread_id"] == 5
    assert result.ok
    assert result.message_id is None


async def test_not_modified_is_success() -> None:
    bot = RecordingBot(_bad("Bad Request: message is not modified"))
    executor = TelegramExecutor(bot, 10)
    intent = EditIntent(message_id=42, chunk=Chunk(text="hi"))

    result = await executor.execute(_action(intent, ActionKind.EDIT))

    assert result.ok
    assert result.failure is None


async def test_retry_after_is_reported() -> None:
    error = TelegramRetryAfter(
        method=SendMessage(chat_id=1, text="x"), message="flood", retry_after=9
    )
    bot = RecordingBot(error)
    executor = TelegramExecutor(bot, 10)

    result = await executor.execute(
        _action(SendIntent(chunk=Chunk(text="a")), ActionKind.SEND)
    )

    assert not result.ok
    assert result.retry_after == 9.0


async def test_forbidden_kills_stream() -> None:
    error = TelegramForbiddenError(
        method=SendMessage(chat_id=1, text="x"), message="bot was blocked"
    )
    bot = RecordingBot(error)
    executor = TelegramExecutor(bot, 10)

    result = await executor.execute(
        _action(SendIntent(chunk=Chunk(text="a")), ActionKind.SEND)
    )

    assert result.failure is Failure.STREAM_DEAD
