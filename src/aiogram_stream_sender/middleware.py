from collections.abc import Awaitable, Callable
from typing import Any, Final

from aiogram import BaseMiddleware
from aiogram.types import CallbackQuery, Message, TelegramObject

from aiogram_stream_sender.runtime.runtime import SenderRuntime


def _target(event: TelegramObject) -> Message | None:
    if isinstance(event, Message):
        return event
    if isinstance(event, CallbackQuery) and isinstance(event.message, Message):
        return event.message
    return None


class SenderMiddleware(BaseMiddleware):
    def __init__(self, runtime: SenderRuntime, key: str = "sender") -> None:
        self._runtime: Final = runtime
        self._key: Final = key

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        message = _target(event)
        bot = data.get("bot")
        if message is not None and bot is not None:
            data[self._key] = self._runtime.scoped(
                bot, message.chat.id, message.message_thread_id
            )
        return await handler(event, data)
