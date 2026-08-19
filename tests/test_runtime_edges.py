import asyncio

from tests.conftest import FakeBot, FakeClock, FakeExecutor

from aiogram_stream_sender.options import Options
from aiogram_stream_sender.runtime.runtime import SenderRuntime
from aiogram_stream_sender.runtime.scoped import ScopedSender


def _runtime(
    clock: FakeClock, options: Options, executor: FakeExecutor
) -> SenderRuntime:
    return SenderRuntime(
        options, clock=clock, executor_factory=lambda bot, chat_id: executor
    )


def test_defaults_are_usable() -> None:
    runtime = SenderRuntime()
    assert isinstance(runtime.scoped(FakeBot(), 10, None), ScopedSender)


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


async def test_thread_id_reaches_every_action(
    clock: FakeClock, options: Options
) -> None:
    executor = FakeExecutor()
    runtime = _runtime(clock, options, executor)
    bot = FakeBot()

    first = runtime.open_stream(bot, 10, 3)
    second = runtime.open_stream(bot, 10, None)
    first.update([{"text": "a"}])
    second.update([{"text": "b"}])
    await asyncio.sleep(0)
    pending = asyncio.gather(first.finish(), second.finish())
    for _ in range(4):
        await clock.advance(1.0)
    _ = await asyncio.wait_for(pending, timeout=1.0)

    threads = {call.stream_id: call.thread_id for call in executor.calls}
    assert threads == {1: 3, 2: None}
    await runtime.aclose()


async def test_aclose_without_workers(options: Options, clock: FakeClock) -> None:
    runtime = _runtime(clock, options, FakeExecutor())
    await runtime.aclose()
    assert runtime.scoped(FakeBot(), 10, None) is not None
