from aiogram_stream_sender.chunk import Chunk
from aiogram_stream_sender.errors import Failure
from aiogram_stream_sender.events import Event, StreamFailed
from aiogram_stream_sender.machine.action import Result, ScopedAction
from aiogram_stream_sender.machine.machine import SenderMachine
from aiogram_stream_sender.machine.timings import ChatTimings
from aiogram_stream_sender.message.intent import ActionIntent, ActionKind, SendIntent
from aiogram_stream_sender.options import Options


def _machine(options: Options, events: list[Event] | None = None) -> SenderMachine:
    return SenderMachine(
        bot_id=1,
        chat_id=2,
        timings=ChatTimings(),
        options=options,
        sink=events.append if events is not None else None,
    )


def test_outcome_of_unknown_stream_is_empty(options: Options) -> None:
    machine = _machine(options)
    assert machine.outcome(99) == ("ok", [], "")
    assert machine.is_settled(99)


def test_update_and_finalize_ignore_unknown_stream(options: Options) -> None:
    machine = _machine(options)
    machine.update(99, [Chunk(text="a")])
    machine.finalize(99)
    assert machine.plan(0.0)[0] is None


def test_apply_ignores_unknown_stream(options: Options) -> None:
    machine = _machine(options)
    action = ScopedAction(
        stream_id=99,
        index=0,
        thread_id=None,
        intent=SendIntent(chunk=Chunk(text="a")),
        kind=ActionKind.SEND,
    )
    machine.apply(action, Result(ok=True, message_id=1), 0.0)
    assert machine.outcome(99) == ("ok", [], "")


def test_typing_result_only_moves_its_counter(options: Options) -> None:
    machine = _machine(options)
    machine.add_stream(1, None)
    action = ScopedAction(
        stream_id=1,
        index=-1,
        thread_id=None,
        intent=ActionIntent(),
        kind=ActionKind.ACTION,
    )
    machine.apply(action, Result(ok=True), 3.0)
    machine.update(1, [Chunk(text="a")])
    send, _deadline = machine.plan(3.0)
    assert send is not None
    assert send.kind is ActionKind.SEND


def test_backoff_applies_jitter() -> None:
    options = Options(
        typing_enabled=False,
        send_interval=0.0,
        backoff_base=1.0,
        backoff_jitter=0.5,
        max_attempts=5,
    )
    machine = _machine(options)
    machine.add_stream(1, None)
    machine.update(1, [Chunk(text="a")])
    action, _deadline = machine.plan(0.0)
    assert action is not None
    machine.apply(action, Result(ok=False, failure=Failure.TRANSIENT), 0.0)
    blocked, deadline = machine.plan(0.0)
    assert blocked is None
    assert 0.5 <= deadline <= 1.5


def test_partial_outcome_when_one_message_dies(options: Options) -> None:
    machine = _machine(options)
    machine.add_stream(1, None)
    machine.update(1, [Chunk(text="a"), Chunk(text="b")])
    first, _deadline = machine.plan(0.0)
    assert first is not None
    machine.apply(first, Result(ok=False, failure=Failure.MESSAGE_DEAD), 0.0)
    second, _deadline = machine.plan(10.0)
    assert second is not None
    machine.apply(second, Result(ok=True, message_id=55), 10.0)
    machine.finalize(1)
    assert machine.outcome(1) == ("partial", [55], "")


def test_kill_all_reports_live_streams_once(options: Options) -> None:
    events: list[Event] = []
    machine = _machine(options, events)
    machine.add_stream(1, None)
    machine.add_stream(2, None)
    machine.update(1, [Chunk(text="a")])
    action, _deadline = machine.plan(0.0)
    assert action is not None
    machine.apply(
        action, Result(ok=False, failure=Failure.STREAM_DEAD, reason="x"), 0.0
    )
    events.clear()

    machine.kill_all("shutdown")

    assert [event.stream_id for event in events if isinstance(event, StreamFailed)] == [
        2
    ]
    assert machine.outcome(2)[0] == "failed"


def test_sweep_on_empty_machine_starts_idle_at_now(options: Options) -> None:
    machine = _machine(options)
    machine.sweep(4.0)
    assert not machine.is_evictable(4.0)
    assert machine.is_evictable(4.0 + options.machine_ttl)


def test_sweep_keeps_unfinished_stream(options: Options) -> None:
    machine = _machine(options)
    machine.add_stream(1, None)
    machine.update(1, [Chunk(text="a")])
    machine.sweep(1000.0)
    assert not machine.is_evictable(1000.0)
    assert machine.outcome(1)[0] == "ok"
