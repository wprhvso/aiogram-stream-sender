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


async def test_second_finish_is_idempotent(clock: FakeClock, options: Options) -> None:
    runtime = _runtime(clock, options, FakeExecutor())
    stream = runtime.open_stream(FakeBot(), 10, None)
    stream.update([{"text": "a"}])
    await asyncio.sleep(0)
    await clock.advance(0.0)

    first = await asyncio.wait_for(stream.finish(), timeout=1.0)
    second = await asyncio.wait_for(stream.finish(), timeout=1.0)

    assert first == second
    await runtime.aclose()


async def test_update_after_finish_is_ignored(
    clock: FakeClock, options: Options
) -> None:
    executor = FakeExecutor()
    runtime = _runtime(clock, options, executor)
    stream = runtime.open_stream(FakeBot(), 10, None)
    stream.update([{"text": "a"}])
    await asyncio.sleep(0)
    await clock.advance(0.0)
    await asyncio.wait_for(stream.finish(), timeout=1.0)

    stream.update([{"text": "b"}])
    await clock.advance(10.0)

    assert len(executor.calls) == 1
    await runtime.aclose()


async def test_failure_can_be_swallowed(clock: FakeClock, options: Options) -> None:
    executor = FakeExecutor(
        lambda action: Result(ok=False, failure=Failure.STREAM_DEAD, reason="blocked")
    )
    runtime = _runtime(clock, options, executor)
    stream = runtime.open_stream(FakeBot(), 10, None)
    stream.update([{"text": "a"}])
    await asyncio.sleep(0)
    await clock.advance(0.0)

    assert (
        await asyncio.wait_for(stream.finish(raise_on_failure=False), timeout=1.0) == []
    )
    await runtime.aclose()


async def test_raise_on_failure_option_is_default(clock: FakeClock) -> None:
    options = Options(backoff_jitter=0.0, typing_enabled=False, raise_on_failure=False)
    executor = FakeExecutor(
        lambda action: Result(ok=False, failure=Failure.STREAM_DEAD, reason="blocked")
    )
    runtime = _runtime(clock, options, executor)
    stream = runtime.open_stream(FakeBot(), 10, None)
    stream.update([{"text": "a"}])
    await asyncio.sleep(0)
    await clock.advance(0.0)

    assert await asyncio.wait_for(stream.finish(), timeout=1.0) == []
    await runtime.aclose()


async def test_exception_inside_context_does_not_mask(
    clock: FakeClock, options: Options
) -> None:
    executor = FakeExecutor(
        lambda action: Result(ok=False, failure=Failure.STREAM_DEAD, reason="blocked")
    )
    runtime = _runtime(clock, options, executor)
    sender = runtime.scoped(FakeBot(), 10, None)

    async def drive() -> None:
        async with sender.stream() as live:
            live.update([{"text": "a"}])
            await asyncio.sleep(0)
            await clock.advance(0.0)
            msg = "boom"
            raise RuntimeError(msg)

    with pytest.raises(RuntimeError):
        await asyncio.wait_for(drive(), timeout=1.0)
    await runtime.aclose()


async def test_context_manager_propagates_stream_failure(
    clock: FakeClock, options: Options
) -> None:
    executor = FakeExecutor(
        lambda action: Result(ok=False, failure=Failure.STREAM_DEAD, reason="blocked")
    )
    runtime = _runtime(clock, options, executor)
    sender = runtime.scoped(FakeBot(), 10, None)

    async def drive() -> None:
        async with sender.stream() as live:
            live.update([{"text": "a"}])
            await asyncio.sleep(0)
            await clock.advance(0.0)

    with pytest.raises(StreamFailedError):
        await asyncio.wait_for(drive(), timeout=1.0)
    await runtime.aclose()


async def test_thread_id_reaches_executor(clock: FakeClock, options: Options) -> None:
    executor = FakeExecutor()
    runtime = _runtime(clock, options, executor)
    sender = runtime.scoped(FakeBot(), 10, 7)
    stream = sender.stream()
    stream.update([{"text": "a"}])
    await asyncio.sleep(0)
    await clock.advance(0.0)

    assert executor.calls[0].thread_id == 7
    await asyncio.wait_for(stream.finish(), timeout=1.0)
    await runtime.aclose()
