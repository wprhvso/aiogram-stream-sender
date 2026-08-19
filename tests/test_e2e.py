import asyncio
from typing import Any

import pytest
from aiogram import Bot, Dispatcher
from aiogram.client.default import Default
from aiogram.exceptions import TelegramForbiddenError, TelegramRetryAfter
from aiogram.methods import (
    DeleteMessage,
    EditMessageText,
    SendChatAction,
    SendMessage,
)
from aiogram.types import Message, Update
from tests.telegram import FakeTelegram, make_bot, message_update

from aiogram_stream_sender.errors import StreamFailedError
from aiogram_stream_sender.events import ChatHold, Event
from aiogram_stream_sender.middleware import SenderMiddleware
from aiogram_stream_sender.options import Options
from aiogram_stream_sender.runtime.runtime import SenderRuntime
from aiogram_stream_sender.runtime.scoped import ScopedSender

CHAT_ID = 555
TICK = 0.05


def _options(**overrides: Any) -> Options:
    settings: dict[str, Any] = {
        "send_interval": 0.001,
        "edit_interval": 0.001,
        "delete_interval": 0.001,
        "action_interval": 0.001,
        "typing_enabled": False,
        "backoff_base": 0.001,
        "backoff_jitter": 0.0,
        "stream_ttl": 0.05,
        "machine_ttl": 0.1,
        "shutdown_timeout": 5.0,
    }
    settings.update(overrides)
    return Options(**settings)


@pytest.fixture
def session() -> FakeTelegram:
    return FakeTelegram()


@pytest.fixture
def bot(session: FakeTelegram) -> Bot:
    return make_bot(session)


async def _shutdown(runtime: SenderRuntime, bot: Bot) -> None:
    await runtime.aclose()
    await bot.session.close()


async def test_streamed_text_lands_in_one_edited_message(
    session: FakeTelegram, bot: Bot
) -> None:
    runtime = SenderRuntime(_options())
    sender = runtime.scoped(bot, CHAT_ID, None)

    async with sender.stream(typing=False) as live:
        for text in ("He", "Hell", "Hello"):
            live.update([{"text": text}])
            await asyncio.sleep(TICK)

    assert session.texts == ["Hello"]
    assert session.counted(SendMessage) == 1
    assert session.counted(EditMessageText) >= 1
    await _shutdown(runtime, bot)


async def test_shrinking_a_stream_deletes_the_extra_message(
    session: FakeTelegram, bot: Bot
) -> None:
    runtime = SenderRuntime(_options())
    sender = runtime.scoped(bot, CHAT_ID, None)

    async with sender.stream(typing=False) as live:
        live.update([{"text": "one"}, {"text": "two"}])
        await asyncio.sleep(TICK)
        live.update([{"text": "one"}])
        await asyncio.sleep(TICK)

    assert session.texts == ["one"]
    assert session.counted(DeleteMessage) == 1
    await _shutdown(runtime, bot)


async def test_thread_id_is_forwarded_to_telegram(
    session: FakeTelegram, bot: Bot
) -> None:
    runtime = SenderRuntime(_options())
    sender = runtime.scoped(bot, CHAT_ID, 17)

    async with sender.stream(typing=False) as live:
        live.update([{"text": "in topic"}])
        await asyncio.sleep(TICK)

    assert [item.thread_id for item in session.live] == [17]
    await _shutdown(runtime, bot)


async def test_flood_control_holds_then_delivers(
    session: FakeTelegram, bot: Bot
) -> None:
    events: list[Event] = []
    runtime = SenderRuntime(_options(), sink=events.append)
    session.fail(
        SendMessage,
        TelegramRetryAfter(
            method=SendMessage(chat_id=CHAT_ID, text="x"),
            message="Too Many Requests: retry after 0",
            retry_after=0,
        ),
    )

    async with runtime.scoped(bot, CHAT_ID, None).stream(typing=False) as live:
        live.update([{"text": "hi"}])
        await asyncio.sleep(TICK)

    assert session.texts == ["hi"]
    assert any(isinstance(event, ChatHold) for event in events)
    await _shutdown(runtime, bot)


async def test_blocked_bot_fails_the_stream(session: FakeTelegram, bot: Bot) -> None:
    runtime = SenderRuntime(_options())
    session.fail(
        SendMessage,
        TelegramForbiddenError(
            method=SendMessage(chat_id=CHAT_ID, text="x"),
            message="Forbidden: bot was blocked by the user",
        ),
    )
    live = runtime.scoped(bot, CHAT_ID, None).stream(typing=False)
    live.update([{"text": "hi"}])

    with pytest.raises(StreamFailedError, match="blocked"):
        await asyncio.wait_for(live.finish(), timeout=5.0)

    assert session.live == []
    await _shutdown(runtime, bot)


async def test_not_modified_is_treated_as_delivered(
    session: FakeTelegram, bot: Bot
) -> None:
    runtime = SenderRuntime(_options())

    async with runtime.scoped(bot, CHAT_ID, None).stream(typing=False) as live:
        live.update([{"text": "a"}])
        await asyncio.sleep(TICK)
        session.chat[session.next_message_id].text = "ab"
        live.update([{"text": "ab"}])
        await asyncio.sleep(TICK)

    assert session.texts == ["ab"]
    assert session.counted(EditMessageText) == 1
    await _shutdown(runtime, bot)


async def test_typing_action_is_sent_while_streaming(
    session: FakeTelegram, bot: Bot
) -> None:
    runtime = SenderRuntime(_options(typing_enabled=True))

    async with runtime.scoped(bot, CHAT_ID, None).stream() as live:
        await asyncio.sleep(TICK)
        live.update([{"text": "done"}])
        await asyncio.sleep(TICK)

    assert session.counted(SendChatAction) >= 1
    assert session.actions[0] == "typing"
    assert session.texts == ["done"]
    await _shutdown(runtime, bot)


async def test_plain_text_keeps_the_bot_default_parse_mode(
    session: FakeTelegram, bot: Bot
) -> None:
    runtime = SenderRuntime(_options())

    async with runtime.scoped(bot, CHAT_ID, None).stream(typing=False) as live:
        live.update([{"text": "plain"}])
        await asyncio.sleep(TICK)

    sent = session.calls[0]
    assert isinstance(sent, SendMessage)
    assert isinstance(sent.parse_mode, Default)
    await _shutdown(runtime, bot)


async def test_entities_disable_parse_mode(session: FakeTelegram, bot: Bot) -> None:
    runtime = SenderRuntime(_options())
    entity = {"type": "bold", "offset": 0, "length": 4}

    async with runtime.scoped(bot, CHAT_ID, None).stream(typing=False) as live:
        live.update([{"text": "bold", "entities": [entity]}])
        await asyncio.sleep(TICK)

    sent = session.calls[0]
    assert isinstance(sent, SendMessage)
    assert sent.parse_mode is None
    assert [item.entities for item in session.live] == [(entity,)]
    await _shutdown(runtime, bot)


async def test_explicit_parse_mode_is_forwarded(
    session: FakeTelegram, bot: Bot
) -> None:
    runtime = SenderRuntime(_options())

    async with runtime.scoped(bot, CHAT_ID, None).stream(typing=False) as live:
        live.update([{"text": "<b>x</b>", "parse_mode": "HTML"}])
        await asyncio.sleep(TICK)

    sent = session.calls[0]
    assert isinstance(sent, SendMessage)
    assert sent.parse_mode == "HTML"
    await _shutdown(runtime, bot)


async def test_two_chats_are_served_independently(
    session: FakeTelegram, bot: Bot
) -> None:
    runtime = SenderRuntime(_options())

    async def drive(chat_id: int, text: str) -> None:
        async with runtime.scoped(bot, chat_id, None).stream(typing=False) as live:
            live.update([{"text": text}])
            await asyncio.sleep(TICK)

    await asyncio.gather(drive(1, "first"), drive(2, "second"))

    assert sorted(session.texts) == ["first", "second"]
    await _shutdown(runtime, bot)


async def test_concurrent_streams_in_one_chat_both_deliver(
    session: FakeTelegram, bot: Bot
) -> None:
    runtime = SenderRuntime(_options())
    sender = runtime.scoped(bot, CHAT_ID, None)

    async def drive(text: str) -> None:
        async with sender.stream(typing=False) as live:
            for step in range(3):
                live.update([{"text": f"{text}{step}"}])
                await asyncio.sleep(TICK)

    _ = await asyncio.gather(drive("a"), drive("b"))

    assert sorted(session.texts) == ["a2", "b2"]
    await _shutdown(runtime, bot)


async def test_dispatcher_handler_streams_a_reply(
    session: FakeTelegram, bot: Bot
) -> None:
    runtime = SenderRuntime(_options())
    dispatcher = Dispatcher()
    dispatcher.message.middleware(SenderMiddleware(runtime))

    async def handle(message: Message, sender: ScopedSender) -> None:
        async with sender.stream(typing=False) as live:
            live.update([{"text": f"echo: {message.text}"}])
            await asyncio.sleep(TICK)

    dispatcher.message.register(handle)
    update = Update.model_validate(message_update(1, CHAT_ID, "ping"))
    _ = await dispatcher.feed_update(bot, update)

    assert session.texts == ["echo: ping"]
    await _shutdown(runtime, bot)


async def test_runtime_close_flushes_open_streams(
    session: FakeTelegram, bot: Bot
) -> None:
    runtime = SenderRuntime(_options())
    live = runtime.scoped(bot, CHAT_ID, None).stream(typing=False)
    live.update([{"text": "flushed"}])

    await runtime.aclose()

    assert session.texts == ["flushed"]
    await bot.session.close()
