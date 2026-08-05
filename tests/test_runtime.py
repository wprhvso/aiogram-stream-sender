import asyncio

import pytest
from tests.conftest import FakeBot, FakeClock, FakeExecutor

from aiogram_stream_sender.errors import Failure, StreamFailedError
from aiogram_stream_sender.machine.action import Result
from aiogram_stream_sender.options import Options
from aiogram_stream_sender.runtime.runtime import SenderRuntime


def _runtime(
    clock: FakeClock, options: Options, executor: FakeExecutor
) -> SenderRuntime:
    return SenderRuntime(
        options, clock=clock, executor_factory=lambda bot, chat_id: executor
    )


async def test_two_streams_share_one_worker(clock: FakeClock, options: Options) -> None:
    executor = FakeExecutor()
    runtime = _runtime(clock, options, executor)
    bot = FakeBot()

    first = runtime.open_stream(bot, 10, None)
    second = runtime.open_stream(bot, 10, None)
    first.update([{"text": "a"}])
    second.update([{"text": "b"}])
    await asyncio.sleep(0)
    await clock.advance(0.0)
    assert len(executor.calls) == 1

    await clock.advance(1.0)
    ids_first, ids_second = await asyncio.gather(
        asyncio.wait_for(first.finish(), timeout=1.0),
        asyncio.wait_for(second.finish(), timeout=1.0),
    )
    assert len(ids_first) == 1
    assert len(ids_second) == 1
    await runtime.aclose()


async def test_separate_chats_run_in_parallel(
    clock: FakeClock, options: Options
) -> None:
    executor = FakeExecutor()
    runtime = _runtime(clock, options, executor)
    bot = FakeBot()

    first = runtime.open_stream(bot, 10, None)
    second = runtime.open_stream(bot, 11, None)
    first.update([{"text": "a"}])
    second.update([{"text": "b"}])
    await asyncio.sleep(0)
    await clock.advance(0.0)
    assert len(executor.calls) == 2

    await first.finish()
    await second.finish()
    await runtime.aclose()


async def test_failed_stream_raises(clock: FakeClock, options: Options) -> None:
    executor = FakeExecutor(
        lambda action: Result(ok=False, failure=Failure.STREAM_DEAD, reason="blocked")
    )
    runtime = _runtime(clock, options, executor)
    stream = runtime.open_stream(FakeBot(), 10, None)
    stream.update([{"text": "a"}])
    await asyncio.sleep(0)
    await clock.advance(0.0)

    with pytest.raises(StreamFailedError):
        await asyncio.wait_for(stream.finish(), timeout=1.0)
    await runtime.aclose()


async def test_context_manager_finishes(clock: FakeClock, options: Options) -> None:
    executor = FakeExecutor()
    runtime = _runtime(clock, options, executor)
    sender = runtime.scoped(FakeBot(), 10, None)

    async def drive() -> None:
        async with sender.stream() as live:
            live.update([{"text": "a"}])
            await asyncio.sleep(0)
            await clock.advance(0.0)

    await asyncio.wait_for(drive(), timeout=1.0)
    assert len(executor.calls) == 1
    await runtime.aclose()


async def test_aclose_flushes(clock: FakeClock, options: Options) -> None:
    executor = FakeExecutor()
    runtime = _runtime(clock, options, executor)
    stream = runtime.open_stream(FakeBot(), 10, None)
    stream.update([{"text": "a"}])
    await asyncio.sleep(0)
    await clock.advance(0.0)

    closing = asyncio.create_task(runtime.aclose())
    await asyncio.sleep(0)
    await clock.advance(10.0)
    await asyncio.wait_for(closing, timeout=1.0)
    assert len(executor.calls) == 1
