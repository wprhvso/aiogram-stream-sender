from datetime import UTC, datetime
from typing import Any

from aiogram.types import CallbackQuery, Chat, Message, User
from tests.conftest import FakeBot

from aiogram_stream_sender.middleware import SenderMiddleware, _target
from aiogram_stream_sender.options import Options
from aiogram_stream_sender.runtime.runtime import SenderRuntime
from aiogram_stream_sender.runtime.scoped import ScopedSender


def _message() -> Message:
    return Message(
        message_id=1,
        date=datetime.now(UTC),
        chat=Chat(id=10, type="private"),
    )


def _user() -> User:
    return User(id=5, is_bot=False, first_name="a")


def test_target_of_message() -> None:
    message = _message()
    assert _target(message) is message


def test_target_of_callback_query() -> None:
    message = _message()
    query = CallbackQuery(id="1", from_user=_user(), chat_instance="x", message=message)
    assert _target(query) is message


def test_target_of_callback_query_without_message() -> None:
    query = CallbackQuery(id="1", from_user=_user(), chat_instance="x")
    assert _target(query) is None


async def test_missing_bot_skips_injection() -> None:
    runtime = SenderRuntime(Options())
    middleware = SenderMiddleware(runtime)

    async def handler(event: Any, data: dict[str, Any]) -> dict[str, Any]:
        return data

    data = await middleware(handler, _message(), {})
    assert "sender" not in data
    await runtime.aclose()


async def test_custom_key_is_used() -> None:
    runtime = SenderRuntime(Options())
    middleware = SenderMiddleware(runtime, key="stream")

    async def handler(event: Any, data: dict[str, Any]) -> dict[str, Any]:
        return data

    data = await middleware(handler, _message(), {"bot": FakeBot()})
    assert isinstance(data["stream"], ScopedSender)
    await runtime.aclose()
