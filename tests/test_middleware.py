from typing import Any

from tests.conftest import FakeBot

from aiogram_stream_sender.middleware import SenderMiddleware
from aiogram_stream_sender.options import Options
from aiogram_stream_sender.runtime.runtime import SenderRuntime
from aiogram_stream_sender.runtime.scoped import ScopedSender


class FakeChat:
    def __init__(self, chat_id: int) -> None:
        self.id = chat_id


class FakeMessage:
    def __init__(self, chat_id: int, thread_id: int | None) -> None:
        self.chat = FakeChat(chat_id)
        self.message_thread_id = thread_id


async def test_middleware_injects_scoped_sender(monkeypatch: Any) -> None:
    runtime = SenderRuntime(Options())
    middleware = SenderMiddleware(runtime)
    monkeypatch.setattr("aiogram_stream_sender.middleware._target", lambda event: event)
    captured: dict[str, Any] = {}

    async def handler(event: Any, data: dict[str, Any]) -> str:
        captured.update(data)
        return "ok"

    event = FakeMessage(10, 3)
    result = await middleware(handler, event, {"bot": FakeBot()})

    assert result == "ok"
    assert isinstance(captured["sender"], ScopedSender)
    await runtime.aclose()


async def test_middleware_skips_events_without_chat() -> None:
    runtime = SenderRuntime(Options())
    middleware = SenderMiddleware(runtime)

    async def handler(event: Any, data: dict[str, Any]) -> dict[str, Any]:
        return data

    data = await middleware(handler, object(), {"bot": FakeBot()})
    assert "sender" not in data
    await runtime.aclose()
