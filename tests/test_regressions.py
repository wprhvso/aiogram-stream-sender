import asyncio
import contextlib
import math

import pytest
from aiogram.exceptions import TelegramBadRequest
from aiogram.methods import SendMessage
from tests.conftest import FakeBot, FakeClock, FakeExecutor

from aiogram_stream_sender.chunk import Chunk
from aiogram_stream_sender.errors import Failure, StreamFailedError
from aiogram_stream_sender.machine.action import Result
from aiogram_stream_sender.machine.machine import SenderMachine
from aiogram_stream_sender.machine.timings import ChatTimings
from aiogram_stream_sender.message.intent import SendIntent
from aiogram_stream_sender.options import Options
from aiogram_stream_sender.runtime.clock import MonotonicClock
from aiogram_stream_sender.runtime.runtime import SenderRuntime
from aiogram_stream_sender.runtime.worker import MachineWorker
from aiogram_stream_sender.stream.stream import SenderStream
from aiogram_stream_sender.transport.classify import classify


def _machine(options: Options) -> SenderMachine:
    return SenderMachine(1, 2, ChatTimings(), options)


def _worker(
    clock: FakeClock, options: Options, executor: FakeExecutor
) -> MachineWorker:
    return MachineWorker(_machine(options), executor, clock, options)


def _runtime(
    clock: FakeClock, options: Options, executor: FakeExecutor
) -> SenderRuntime:
    return SenderRuntime(
        options, clock=clock, executor_factory=lambda bot, chat_id: executor
    )


def test_shrinking_keeps_indices_of_in_flight_messages() -> None:
    stream = SenderStream(stream_id=1)
    stream.update([Chunk(text="a"), Chunk(text="b"), Chunk(text="c")])
    stream.apply_failure(1, terminal=True)
    stream.mark_in_flight(2, value=True)

    stream.update([Chunk(text="a")])

    assert len(stream.messages) == 3
    stream.mark_in_flight(2, value=False)
    assert not any(message.in_flight for message in stream.messages)


def test_settled_tail_is_compacted_once_nothing_is_in_flight() -> None:
    stream = SenderStream(stream_id=1)
    stream.update([Chunk(text="a"), Chunk(text="b")])
    stream.apply_success(1, SendIntent(chunk=Chunk(text="b")), 200)

    stream.update([Chunk(text="a")])

    assert len(stream.messages) == 2
    assert stream.messages[1].desired is None


def test_send_without_message_id_is_not_a_success() -> None:
    options = Options(typing_enabled=False, backoff_jitter=0.0, send_interval=0.0)
    machine = _machine(options)
    machine.add_stream(1, None, typing=False)
    machine.update(1, [Chunk(text="a")])

    action, _deadline = machine.plan(0.0)
    assert action is not None
    machine.apply(action, Result(ok=True, message_id=None), 0.0)

    assert machine.plan(0.5)[0] is None
    retried, _deadline = machine.plan(1.0)
    assert retried is not None
    assert retried.index == 0


def test_streams_take_turns_inside_one_chat() -> None:
    options = Options(
        typing_enabled=False, send_interval=0.0, edit_interval=0.0, backoff_jitter=0.0
    )
    machine = _machine(options)
    for stream_id in (1, 2):
        machine.add_stream(stream_id, None, typing=False)
        machine.update(stream_id, [Chunk(text="a"), Chunk(text="b")])

    served: list[int] = []
    message_id = 100
    for turn in range(4):
        action, _deadline = machine.plan(float(turn))
        assert action is not None
        served.append(action.stream_id)
        message_id += 1
        machine.apply(action, Result(ok=True, message_id=message_id), float(turn))

    assert served == [1, 2, 1, 2]


def test_backoff_does_not_leak_onto_a_reused_index() -> None:
    options = Options(typing_enabled=False, send_interval=0.0, backoff_jitter=0.0)
    machine = _machine(options)
    machine.add_stream(1, None, typing=False)
    machine.update(1, [Chunk(text="a"), Chunk(text="b")])

    first, _deadline = machine.plan(0.0)
    assert first is not None
    machine.apply(first, Result(ok=True, message_id=101), 0.0)

    second, _deadline = machine.plan(0.0)
    assert second is not None
    assert second.index == 1
    machine.apply(second, Result(ok=False, failure=Failure.TRANSIENT), 0.0)

    machine.update(1, [Chunk(text="a")])
    machine.update(1, [Chunk(text="a"), Chunk(text="c")])

    revived, _deadline = machine.plan(0.1)
    assert revived is not None
    assert revived.index == 1


async def test_infinite_deadline_only_blocks_on_positive_infinity() -> None:
    clock = MonotonicClock()
    await asyncio.wait_for(clock.sleep_until(-math.inf), timeout=1.0)


def test_chunk_is_hashable_with_markup() -> None:
    markup = {"inline_keyboard": [[{"text": "go", "callback_data": "go"}]]}
    first = Chunk(text="a", reply_markup=markup)
    second = Chunk(text="a", reply_markup=dict(markup))

    assert hash(first) == hash(second)
    assert len({first, second}) == 1


def test_chunk_hashes_values_json_cannot_encode() -> None:
    chunk = Chunk(text="a", link_preview={"url": object()})
    assert len(chunk.content_hash) == 64


def test_unknown_error_text_does_not_kill_the_stream() -> None:
    failure, retry_after, reason = classify(RuntimeError("chat not found in cache"))

    assert failure is Failure.TRANSIENT
    assert retry_after is None
    assert reason == "chat not found in cache"


def test_bad_request_still_kills_the_stream() -> None:
    error = TelegramBadRequest(
        method=SendMessage(chat_id=1, text="x"),
        message="Bad Request: chat not found",
    )

    assert classify(error)[0] is Failure.STREAM_DEAD


async def test_settled_without_worker_kills_only_the_caller(
    clock: FakeClock, options: Options
) -> None:
    worker = _worker(clock, options, FakeExecutor())
    _ = worker.register(1, None)
    _ = worker.register(2, None)
    worker.update(1, (Chunk(text="a"),))
    worker.update(2, (Chunk(text="b"),))

    await worker.settled(1)

    assert worker.outcome(1)[0] == "failed"
    assert worker.outcome(2)[0] == "ok"


async def test_finish_is_idempotent(clock: FakeClock, options: Options) -> None:
    runtime = _runtime(clock, options, FakeExecutor())
    stream = runtime.open_stream(FakeBot(), 10, None)
    stream.update([{"text": "a"}])
    await asyncio.sleep(0)
    await clock.advance(0.0)

    first = await asyncio.wait_for(stream.finish(), timeout=1.0)
    second = await stream.finish()

    assert first == second
    assert len(first) == 1
    await runtime.aclose()


async def test_finish_keeps_reporting_a_failure(
    clock: FakeClock, options: Options
) -> None:
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
    with pytest.raises(StreamFailedError):
        await stream.finish()

    assert await stream.finish(raise_on_failure=False) == []
    await runtime.aclose()


async def test_dead_worker_is_replaced_for_new_streams(
    clock: FakeClock, options: Options
) -> None:
    executor = FakeExecutor()
    runtime = _runtime(clock, options, executor)
    bot = FakeBot()

    orphan = runtime.open_stream(bot, 10, None)
    await asyncio.sleep(0)
    worker = runtime._workers[bot.id, 10]
    assert worker.task is not None
    _ = worker.task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await worker.task

    revived = runtime.open_stream(bot, 10, None)
    assert runtime._workers[bot.id, 10] is not worker

    revived.update([{"text": "b"}])
    await asyncio.sleep(0)
    await clock.advance(0.0)
    assert len(await asyncio.wait_for(revived.finish(), timeout=1.0)) == 1

    with pytest.raises(StreamFailedError):
        await orphan.finish()
    await runtime.aclose()
