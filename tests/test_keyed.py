import asyncio

from tests.conftest import FakeBot, FakeClock, FakeExecutor

from aiogram_stream_sender.chunk import Chunk
from aiogram_stream_sender.machine.action import Result
from aiogram_stream_sender.machine.machine import SenderMachine
from aiogram_stream_sender.machine.timings import ChatTimings
from aiogram_stream_sender.message.intent import (
    ActionKind,
    DropIntent,
    EditIntent,
    SendIntent,
    kind_of,
)
from aiogram_stream_sender.message.message import SenderMessage
from aiogram_stream_sender.options import Options
from aiogram_stream_sender.runtime.runtime import SenderRuntime
from aiogram_stream_sender.stream.stream import SenderStream


def _deliver(stream: SenderStream, message_id: int) -> None:
    found = stream.pending()
    assert found is not None
    index, intent = found
    stream.apply_success(index, intent, message_id)


def _sent(message_id: int) -> Result:
    return Result(ok=True, message_id=message_id)


def test_same_key_still_edits() -> None:
    stream = SenderStream(stream_id=1)
    stream.update([Chunk(text="a", key="1")])
    _deliver(stream, 10)
    stream.update([Chunk(text="b", key="1")])
    found = stream.pending()
    assert found is not None
    assert isinstance(found[1], EditIntent)


def test_new_key_drops_then_sends_again() -> None:
    stream = SenderStream(stream_id=1)
    stream.update([Chunk(text="a", key="1")])
    _deliver(stream, 10)

    stream.update([Chunk(text="a", key="2")])
    found = stream.pending()
    assert found is not None
    assert found[1] == DropIntent(message_id=10)
    assert kind_of(found[1]) is ActionKind.DELETE

    _deliver(stream, 10)
    assert stream.message_ids == []
    assert len(stream.messages) == 1

    found = stream.pending()
    assert found is not None
    assert isinstance(found[1], SendIntent)
    _deliver(stream, 11)
    assert stream.message_ids == [11]
    assert stream.pending() is None


def test_key_bumped_again_mid_flight_sends_latest_once() -> None:
    stream = SenderStream(stream_id=1)
    stream.update([Chunk(text="a", key="1")])
    _deliver(stream, 10)
    stream.update([Chunk(text="a", key="2")])
    _deliver(stream, 10)

    stream.update([Chunk(text="a", key="3")])
    found = stream.pending()
    assert found is not None
    assert isinstance(found[1], SendIntent)
    _deliver(stream, 11)
    assert stream.pending() is None


def test_dropped_message_is_not_settled() -> None:
    message = SenderMessage()
    message.set_desired(Chunk(text="a", key="1"))
    message.on_success(SendIntent(chunk=Chunk(text="a", key="1")), 10)
    assert message.is_settled

    message.set_desired(Chunk(text="a", key="2"))
    assert not message.is_settled
    message.on_success(DropIntent(message_id=10), 10)
    assert message.message_id is None
    assert message.state == "pending"
    assert not message.is_settled


def test_key_is_outside_the_content_hash() -> None:
    assert (
        Chunk(text="a", key="1").content_hash == Chunk(text="a", key="2").content_hash
    )


def test_machine_recreates_through_delete_and_send(options: Options) -> None:
    machine = SenderMachine(1, 10, ChatTimings(), options)
    machine.add_stream(1, None)
    machine.update(1, [Chunk(text="a", key="1")])

    action, _deadline = machine.plan(0.0)
    assert action is not None
    machine.apply(action, _sent(11), 0.0)

    machine.update(1, [Chunk(text="a", key="2")])
    action, _deadline = machine.plan(0.0)
    assert action is not None
    assert action.kind is ActionKind.DELETE
    machine.apply(action, _sent(11), 0.0)

    action, deadline = machine.plan(0.0)
    assert action is None
    assert deadline == options.send_interval

    action, _deadline = machine.plan(deadline)
    assert action is not None
    assert action.kind is ActionKind.SEND


async def test_stream_moves_its_message_to_the_bottom(
    clock: FakeClock, options: Options
) -> None:
    executor = FakeExecutor()
    runtime = SenderRuntime(
        options, clock=clock, executor_factory=lambda bot, chat_id: executor
    )
    stream = runtime.open_stream(FakeBot(), 10, None)

    stream.update([{"text": "prompt", "key": "1"}])
    await asyncio.sleep(0)
    await clock.advance(0.0)

    stream.update([{"text": "prompt", "key": "2"}])
    await clock.advance(options.send_interval)
    await clock.advance(options.send_interval)

    message_ids = await asyncio.wait_for(stream.finish(), timeout=1.0)

    kinds = [action.kind for action in executor.calls]
    assert kinds == [ActionKind.SEND, ActionKind.DELETE, ActionKind.SEND]
    assert message_ids == [102]
    await runtime.aclose()


def test_in_flight_tail_survives_a_shrinking_update(options: Options) -> None:
    machine = SenderMachine(1, 10, ChatTimings(), options)
    machine.add_stream(1, None)
    machine.update(1, [Chunk(text="a")])

    action, _deadline = machine.plan(0.0)
    assert action is not None

    machine.update(1, [])
    machine.finalize(1)
    assert not machine.is_settled(1)

    machine.apply(action, _sent(11), 0.0)
    assert not machine.is_settled(1)

    action, _deadline = machine.plan(options.delete_interval)
    assert action is not None
    assert action.kind is ActionKind.DELETE
    machine.apply(action, _sent(11), options.delete_interval)

    assert machine.is_settled(1)
    assert machine.outcome(1) == ("ok", [], "")


def test_throttled_action_does_not_stay_in_flight(options: Options) -> None:
    machine = SenderMachine(1, 10, ChatTimings(), options)
    machine.add_stream(1, None)
    machine.update(1, [Chunk(text="a")])

    action, _deadline = machine.plan(0.0)
    assert action is not None
    machine.apply(action, Result(ok=False, retry_after=5.0), 0.0)

    machine.finalize(1)
    action, _deadline = machine.plan(6.0)
    assert action is not None
    machine.apply(action, _sent(11), 6.0)

    assert machine.is_settled(1)
