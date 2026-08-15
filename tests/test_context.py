import asyncio
import contextvars

from aiogram.exceptions import TelegramBadRequest

from aiogram_stream_sender.chunk import Chunk
from aiogram_stream_sender.machine.action import Result, ScopedAction
from aiogram_stream_sender.message.intent import ActionKind, SendIntent
from aiogram_stream_sender.options import Options
from aiogram_stream_sender.runtime.runtime import SenderRuntime
from aiogram_stream_sender.transport.executor import TelegramExecutor
from tests.conftest import FakeClock, FakeExecutor

DEADLINE = 5.0

origin: contextvars.ContextVar[str] = contextvars.ContextVar("origin", default="none")


class ContextAwareExecutor(FakeExecutor):
    """Records the context each Telegram call actually ran in."""

    def __init__(self) -> None:
        super().__init__()
        self.contexts: list[str] = []

    async def execute(self, action: ScopedAction) -> Result:
        self.contexts.append(origin.get())
        return await super().execute(action)


class FakeBot:
    def __init__(self, bot_id: int = 1) -> None:
        self.id = bot_id


def build() -> tuple[SenderRuntime, ContextAwareExecutor]:
    executor = ContextAwareExecutor()
    runtime = SenderRuntime(
        Options(send_interval=0.0, edit_interval=0.0, raise_on_failure=False),
        clock=FakeClock(),
        executor_factory=lambda _bot, _chat: executor,
    )
    return runtime, executor


async def send_as(runtime: SenderRuntime, tag: str, text: str) -> None:
    token = origin.set(tag)
    try:
        stream = runtime.scoped(FakeBot(), 10, None).stream(typing=False)  # pyright: ignore[reportArgumentType]
        stream.update([{"text": text}])
        _ = await stream.finish()
    finally:
        origin.reset(token)


async def test_telegram_calls_run_in_the_context_of_their_stream() -> None:
    runtime, executor = build()

    async with asyncio.timeout(DEADLINE):
        await send_as(runtime, "first-turn", "hello")
        await runtime.aclose()

    assert executor.contexts
    assert set(executor.contexts) == {"first-turn"}


async def test_a_second_turn_does_not_inherit_the_first() -> None:
    # The worker is shared by the whole chat and outlives every stream, so
    # without a per-stream context every send would report under whichever
    # caller happened to open the worker.
    runtime, executor = build()

    async with asyncio.timeout(DEADLINE):
        await send_as(runtime, "first-turn", "hello")
        await send_as(runtime, "second-turn", "again")
        await runtime.aclose()

    assert executor.contexts[0] == "first-turn"
    assert executor.contexts[-1] == "second-turn"


async def test_the_worker_holds_no_caller_context_of_its_own() -> None:
    runtime, executor = build()

    async with asyncio.timeout(DEADLINE):
        await send_as(runtime, "opener", "hi")
        await send_as(runtime, "later", "again")
        await runtime.aclose()

    assert "opener" not in executor.contexts[-1:]


async def test_failed_actions_keep_the_exception_object() -> None:
    class Refusing:
        id = 1

        async def send_message(self, **_: object) -> object:
            raise TelegramBadRequest(
                method=object(),  # pyright: ignore[reportArgumentType]
                message="text must be non-empty",
            )

    executor = TelegramExecutor(Refusing(), 10)  # pyright: ignore[reportArgumentType]
    action = ScopedAction(
        stream_id=1,
        index=0,
        thread_id=None,
        intent=SendIntent(chunk=Chunk(text="hi")),
        kind=ActionKind.SEND,
    )

    result = await executor.execute(action)

    assert result.ok is False
    assert isinstance(result.error, TelegramBadRequest)


async def test_cancelling_the_worker_releases_a_waiting_finish() -> None:
    # finish() waits on an event with no timeout, so a worker that dies without
    # settling used to strand the caller for the life of the process.
    class Cancelling:
        async def execute(self, _action: ScopedAction) -> Result:
            raise asyncio.CancelledError

    runtime = SenderRuntime(
        Options(send_interval=0.0, edit_interval=0.0, raise_on_failure=False),
        clock=FakeClock(),
        executor_factory=lambda _bot, _chat: Cancelling(),
    )
    stream = runtime.scoped(FakeBot(), 10, None).stream(typing=False)  # pyright: ignore[reportArgumentType]
    stream.update([{"text": "hi"}])

    async with asyncio.timeout(DEADLINE):
        _ = await stream.finish(raise_on_failure=False)
