import asyncio

from tests.conftest import FakeBot, FakeClock, FakeExecutor

from aiogram_stream_sender.options import Options
from aiogram_stream_sender.runtime.runtime import SenderRuntime


def _runtime(
    clock: FakeClock, options: Options, executor: FakeExecutor
) -> SenderRuntime:
    return SenderRuntime(
        options, clock=clock, executor_factory=lambda bot, chat_id: executor
    )


def test_defaults_are_usable() -> None:
    runtime = SenderRuntime()
    assert runtime.thread_of(1) is None


async def test_prune_drops_stopped_workers(clock: FakeClock) -> None:
    options = Options(
        backoff_jitter=0.0, typing_enabled=False, stream_ttl=1.0, machine_ttl=2.0
    )
    executor = FakeExecutor()
    runtime = _runtime(clock, options, executor)
    bot = FakeBot()

    stream = runtime.open_stream(bot, 10, None)
    stream.update([{"text": "a"}])
    await asyncio.sleep(0)
    await clock.advance(0.0)
    await asyncio.wait_for(stream.finish(), timeout=1.0)

    await clock.advance(10.0)
    await asyncio.sleep(0)
    runtime.prune()

    revived = runtime.open_stream(bot, 10, None)
    revived.update([{"text": "b"}])
    await asyncio.sleep(0)
    await clock.advance(10.0)
    await asyncio.wait_for(revived.finish(), timeout=1.0)

    assert len(executor.calls) == 2
    await runtime.aclose()


async def test_thread_of_tracks_open_streams(
    clock: FakeClock, options: Options
) -> None:
    runtime = _runtime(clock, options, FakeExecutor())
    bot = FakeBot()

    first = runtime.open_stream(bot, 10, 3)
    second = runtime.open_stream(bot, 10, None)
    await asyncio.sleep(0)

    assert runtime.thread_of(1) == 3
    assert runtime.thread_of(2) is None
    assert runtime.thread_of(999) is None

    await clock.advance(10.0)
    await asyncio.gather(first.finish(), second.finish())
    await runtime.aclose()


async def test_aclose_without_workers(options: Options, clock: FakeClock) -> None:
    runtime = _runtime(clock, options, FakeExecutor())
    await runtime.aclose()
    assert runtime.thread_of(1) is None
