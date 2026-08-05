import asyncio

import pytest
from tests.conftest import FakeClock, FakeExecutor

from aiogram_stream_sender.chunk import Chunk
from aiogram_stream_sender.machine.action import Result, ScopedAction
from aiogram_stream_sender.machine.machine import SenderMachine
from aiogram_stream_sender.machine.timings import ChatTimings
from aiogram_stream_sender.options import Options
from aiogram_stream_sender.runtime.worker import MachineWorker


def _worker(
    clock: FakeClock, options: Options, executor: FakeExecutor
) -> MachineWorker:
    machine = SenderMachine(1, 2, ChatTimings(), options, None)
    return MachineWorker(machine, executor, clock, options)


async def test_crash_kills_streams_and_stops(
    clock: FakeClock, options: Options
) -> None:
    def boom(action: ScopedAction) -> Result:
        raise RuntimeError(str(action.stream_id))

    worker = _worker(clock, options, FakeExecutor(boom))
    settled = worker.register(1, None)
    worker.start()

    worker.update(1, (Chunk(text="a"),))
    await asyncio.sleep(0)
    await clock.advance(0.0)
    await asyncio.wait_for(settled.wait(), timeout=1.0)

    assert worker.outcome(1)[0] == "failed"
    assert worker.status == "stopping"
    assert worker.task is not None
    with pytest.raises(RuntimeError):
        await worker.task


async def test_finalize_all_stops_worker(clock: FakeClock, options: Options) -> None:
    executor = FakeExecutor()
    worker = _worker(clock, options, executor)
    settled = worker.register(1, None)
    worker.start()

    worker.update(1, (Chunk(text="a"),))
    await asyncio.sleep(0)
    await clock.advance(0.0)
    worker.finalize_all()
    await asyncio.wait_for(settled.wait(), timeout=1.0)
    await asyncio.wait_for(asyncio.shield(worker.task), timeout=1.0)

    assert worker.status == "stopping"
    assert len(executor.calls) == 1


async def test_outcome_survives_unregister_only_until_cleared(
    clock: FakeClock, options: Options
) -> None:
    executor = FakeExecutor()
    worker = _worker(clock, options, executor)
    settled = worker.register(1, None)
    worker.start()

    worker.update(1, (Chunk(text="a"),))
    worker.finalize(1)
    await asyncio.sleep(0)
    await clock.advance(0.0)
    await asyncio.wait_for(settled.wait(), timeout=1.0)

    assert worker.outcome(1)[0] == "ok"
    worker.unregister(1)
    assert worker.outcome(1)[0] == "ok"
    assert worker.task is not None
    worker.task.cancel()


async def test_cancelled_worker_propagates(clock: FakeClock, options: Options) -> None:
    worker = _worker(clock, options, FakeExecutor())
    worker.register(1, None)
    worker.start()
    await asyncio.sleep(0)

    assert worker.task is not None
    worker.task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await worker.task
