import logging
from typing import Final, Protocol

from aiogram import Bot
from aiogram.enums import ChatAction
from aiogram.types import MessageEntity

from aiogram_stream_sender.chunk import Chunk
from aiogram_stream_sender.machine.action import Result, ScopedAction
from aiogram_stream_sender.message.intent import (
    DeleteIntent,
    EditIntent,
    SendIntent,
)
from aiogram_stream_sender.transport.classify import classify, is_not_modified

log = logging.getLogger(__name__)


class Executor(Protocol):
    async def execute(self, action: ScopedAction) -> Result: ...


def _entities(chunk: Chunk) -> list[MessageEntity]:
    return [MessageEntity.model_validate(entity) for entity in chunk.entities]


class TelegramExecutor:
    def __init__(self, bot: Bot, chat_id: int) -> None:
        self._bot: Final = bot
        self._chat_id: Final = chat_id

    async def execute(self, action: ScopedAction) -> Result:
        try:
            return await self._dispatch(action)
        except Exception as error:
            if is_not_modified(error):
                return Result(ok=True)
            failure, retry_after, reason = classify(error)
            if retry_after is None:
                log.warning("action failed: %r (%s)", action, reason)
            return Result(
                ok=False, failure=failure, reason=reason, retry_after=retry_after
            )

    async def _dispatch(self, action: ScopedAction) -> Result:
        intent = action.intent

        if isinstance(intent, SendIntent):
            message = await self._bot.send_message(
                chat_id=self._chat_id,
                message_thread_id=action.thread_id,
                text=intent.chunk.text,
                entities=_entities(intent.chunk),
            )
            return Result(ok=True, message_id=message.message_id)

        if isinstance(intent, EditIntent):
            await self._bot.edit_message_text(
                chat_id=self._chat_id,
                message_id=intent.message_id,
                text=intent.chunk.text,
                entities=_entities(intent.chunk),
            )
            return Result(ok=True, message_id=intent.message_id)

        if isinstance(intent, DeleteIntent):
            await self._bot.delete_message(
                chat_id=self._chat_id, message_id=intent.message_id
            )
            return Result(ok=True, message_id=intent.message_id)

        await self._bot.send_chat_action(
            chat_id=self._chat_id,
            message_thread_id=action.thread_id,
            action=ChatAction.TYPING,
        )
        return Result(ok=True)
