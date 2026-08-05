import asyncio

from tests.conftest import FakeClock, FakeExecutor

from aiogram_stream_sender.chunk import Chunk
from aiogram_stream_sender.errors import Failure
from aiogram_stream_sender.machine.action import Result
from aiogram_stream_sender.machine.machine import SenderMachine
from aiogram_stream_sender.machine.timings import ChatTimings
from aiogram_stream_sender.message.intent import ActionKind
from aiogram_stream_sender.options import Options
from aiogram_stream_sender.runtime.worker import MachineWorker


def _worker(
    clock: FakeClock, options: Options, executor: FakeExecutor
) -> MachineWorker:
    machine = SenderMachine(1, 2, ChatTimings(), options, None)
    return MachineWorker(machine, executor, clock, options)


async def test_delivers_and_settles(clock: FakeClock, options: Options) -> None:
    executor = FakeExecutor()
    worker = _worker(clock, options, executor)
    settled = worker.register(1, None)
    worker.start()

    worker.update(1, (Chunk(text="a"),))
    await asyncio.sleep(0)
    await clock.advance(0.0)
    worker.finalize(1)
    await asyncio.wait_for(settled.wait(), timeout=1.0)

    assert worker.outcome(1)[0] == "ok"
    assert executor.calls[0].kind is ActionKind.SEND
    assert worker.task is not None
    worker.task.cancel()


async def test_update_interrupts_sleep(clock: FakeClock, options: Options) -> None:
    executor = FakeExecutor()
    worker = _worker(clock, options, executor)
    settled = worker.register(1, None)
    worker.start()
    await asyncio.sleep(0)

    worker.update(1, (Chunk(text="a"),))
    await asyncio.sleep(0)
    await clock.advance(0.0)
    assert len(executor.calls) == 1

    worker.finalize(1)
    await asyncio.wait_for(settled.wait(), timeout=1.0)
    assert worker.task is not None
    worker.task.cancel()


async def test_throttle_between_sends(clock: FakeClock, options: Options) -> None:
    executor = FakeExecutor()
    worker = _worker(clock, options, executor)
    settled = worker.register(1, None)
    worker.start()

    worker.update(1, (Chunk(text="a"), Chunk(text="b")))
    worker.finalize(1)
    await asyncio.sleep(0)
    await clock.advance(0.0)
    assert len(executor.calls) == 1

    await clock.advance(1.0)
    await asyncio.wait_for(settled.wait(), timeout=1.0)
    assert len(executor.calls) == 2
    assert worker.outcome(1)[1] == [101, 102]
    assert worker.task is not None
    worker.task.cancel()


async def test_terminal_failure_settles_as_failed(
    clock: FakeClock, options: Options
) -> None:
    executor = FakeExecutor(
        lambda action: Result(ok=False, failure=Failure.STREAM_DEAD, reason="blocked")
    )
    worker = _worker(clock, options, executor)
    settled = worker.register(1, None)
    worker.start()

    worker.update(1, (Chunk(text="a"),))
    await asyncio.sleep(0)
    await clock.advance(0.0)
    await asyncio.wait_for(settled.wait(), timeout=1.0)

    assert worker.outcome(1)[0] == "failed"
    assert worker.task is not None
    worker.task.cancel()


async def test_worker_stops_when_evictable(clock: FakeClock) -> None:
    options = Options(
        backoff_jitter=0.0, typing_enabled=False, stream_ttl=1.0, machine_ttl=2.0
    )
    executor = FakeExecutor()
    worker = _worker(clock, options, executor)
    settled = worker.register(1, None)
    worker.start()

    worker.update(1, (Chunk(text="a"),))
    worker.finalize(1)
    await asyncio.sleep(0)
    await clock.advance(0.0)
    await asyncio.wait_for(settled.wait(), timeout=1.0)
    worker.unregister(1)

    await clock.advance(10.0)
    await asyncio.wait_for(asyncio.shield(worker.task), timeout=1.0)
    assert worker.status == "stopping"
