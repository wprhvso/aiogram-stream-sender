from aiogram_stream_sender.chunk import Chunk
from aiogram_stream_sender.errors import Failure
from aiogram_stream_sender.events import ChatHold, Event, MessageFailed, StreamFailed
from aiogram_stream_sender.machine.action import Result
from aiogram_stream_sender.machine.machine import SenderMachine
from aiogram_stream_sender.machine.timings import ChatTimings
from aiogram_stream_sender.message.intent import ActionKind
from aiogram_stream_sender.options import Options


def _machine(options: Options, events: list[Event] | None = None) -> SenderMachine:
    return SenderMachine(
        bot_id=1,
        chat_id=2,
        timings=ChatTimings(),
        options=options,
        sink=events.append if events is not None else None,
    )


def test_send_then_settle(options: Options) -> None:
    machine = _machine(options)
    machine.add_stream(1, None)
    machine.update(1, [Chunk(text="a")])
    action, _deadline = machine.plan(0.0)
    assert action is not None
    machine.apply(action, Result(ok=True, message_id=42), 0.0)
    machine.finalize(1)
    assert machine.is_settled(1)
    assert machine.outcome(1) == ("ok", [42], "")


def test_retry_after_holds_chat_and_keeps_pending(options: Options) -> None:
    events: list[Event] = []
    machine = _machine(options, events)
    machine.add_stream(1, None)
    machine.update(1, [Chunk(text="a")])
    action, _deadline = machine.plan(0.0)
    assert action is not None
    machine.apply(action, Result(ok=False, retry_after=7.0), 0.0)
    assert isinstance(events[0], ChatHold)
    again, deadline = machine.plan(0.0)
    assert again is None
    assert deadline == 7.0
    resumed, _deadline = machine.plan(7.0)
    assert resumed is not None
    assert resumed.kind is ActionKind.SEND


def test_hold_is_shared_between_streams(options: Options) -> None:
    machine = _machine(options)
    machine.add_stream(1, None)
    machine.add_stream(2, None)
    machine.update(1, [Chunk(text="a")])
    machine.update(2, [Chunk(text="b")])
    action, _deadline = machine.plan(0.0)
    assert action is not None
    machine.apply(action, Result(ok=False, retry_after=5.0), 0.0)
    blocked, deadline = machine.plan(0.0)
    assert blocked is None
    assert deadline == 5.0


def test_transient_failure_backs_off(options: Options) -> None:
    machine = _machine(options)
    machine.add_stream(1, None)
    machine.update(1, [Chunk(text="a")])
    action, _deadline = machine.plan(0.0)
    assert action is not None
    machine.apply(action, Result(ok=False, failure=Failure.TRANSIENT), 0.0)
    blocked, deadline = machine.plan(0.0)
    assert blocked is None
    assert deadline == options.backoff_base


def test_max_attempts_kills_stream(options: Options) -> None:
    events: list[Event] = []
    tight = Options(
        backoff_jitter=0.0, typing_enabled=False, max_attempts=2, send_interval=0.0
    )
    machine = _machine(tight, events)
    machine.add_stream(1, None)
    machine.update(1, [Chunk(text="a")])
    now = 0.0
    for _ in range(2):
        action, _deadline = machine.plan(now)
        assert action is not None
        machine.apply(action, Result(ok=False, failure=Failure.TRANSIENT), now)
        now += 100.0
    assert machine.is_settled(1)
    assert machine.outcome(1)[0] == "failed"
    assert any(isinstance(event, StreamFailed) for event in events)


def test_message_dead_keeps_stream_alive(options: Options) -> None:
    events: list[Event] = []
    machine = _machine(options, events)
    machine.add_stream(1, None)
    machine.update(1, [Chunk(text="a"), Chunk(text="b")])
    first, _deadline = machine.plan(0.0)
    assert first is not None
    machine.apply(
        first, Result(ok=False, failure=Failure.MESSAGE_DEAD, reason="too long"), 0.0
    )
    assert any(isinstance(event, MessageFailed) for event in events)
    second, _deadline = machine.plan(10.0)
    assert second is not None
    assert second.index == 1


def test_stream_dead_stops_everything(options: Options) -> None:
    machine = _machine(options)
    machine.add_stream(1, None)
    machine.update(1, [Chunk(text="a")])
    action, _deadline = machine.plan(0.0)
    assert action is not None
    machine.apply(
        action, Result(ok=False, failure=Failure.STREAM_DEAD, reason="blocked"), 0.0
    )
    assert machine.outcome(1)[0] == "failed"
    assert machine.plan(100.0)[0] is None


def test_typing_does_not_touch_write_counters() -> None:
    options = Options(backoff_jitter=0.0, action_interval=1.0, send_interval=5.0)
    machine = _machine(options)
    machine.add_stream(1, None)
    action, _deadline = machine.plan(1.0)
    assert action is not None
    assert action.kind is ActionKind.ACTION
    machine.apply(action, Result(ok=True), 1.0)
    machine.update(1, [Chunk(text="a")])
    send, _deadline = machine.plan(1.0)
    assert send is not None
    assert send.kind is ActionKind.SEND


def test_sweep_evicts_finished_stream_and_machine() -> None:
    options = Options(
        backoff_jitter=0.0, typing_enabled=False, stream_ttl=5.0, machine_ttl=10.0
    )
    machine = _machine(options)
    machine.add_stream(1, None)
    machine.update(1, [Chunk(text="a")])
    action, _deadline = machine.plan(0.0)
    assert action is not None
    machine.apply(action, Result(ok=True, message_id=1), 0.0)
    machine.finalize(1)
    machine.plan(0.0)
    machine.sweep(6.0)
    assert not machine.is_evictable(6.0)
    machine.sweep(20.0)
    assert machine.is_evictable(20.0)


def test_a_dead_message_hands_over_the_exception(options: Options) -> None:
    events: list[Event] = []
    machine = _machine(options, events)
    machine.add_stream(1, None)
    machine.update(1, [Chunk(text="a")])
    action, _deadline = machine.plan(0.0)
    assert action is not None
    boom = ValueError("can't parse entities")

    machine.apply(
        action,
        Result(
            ok=False, failure=Failure.MESSAGE_DEAD, reason="can't parse", error=boom
        ),
        0.0,
    )

    failed = next(event for event in events if isinstance(event, MessageFailed))
    # The sink is the only place that can write this into a trace, and a
    # reason string has no stack trace behind it.
    assert failed.error is boom


def test_a_dead_stream_hands_over_the_exception(options: Options) -> None:
    events: list[Event] = []
    machine = _machine(options, events)
    machine.add_stream(1, None)
    machine.update(1, [Chunk(text="a")])
    action, _deadline = machine.plan(0.0)
    assert action is not None
    boom = PermissionError("bot was blocked by the user")

    machine.apply(
        action,
        Result(ok=False, failure=Failure.STREAM_DEAD, reason="blocked", error=boom),
        0.0,
    )

    failed = next(event for event in events if isinstance(event, StreamFailed))
    assert failed.error is boom


def test_giving_up_after_max_attempts_reports_the_last_exception() -> None:
    events: list[Event] = []
    tight = Options(
        backoff_jitter=0.0, typing_enabled=False, max_attempts=2, send_interval=0.0
    )
    machine = _machine(tight, events)
    machine.add_stream(1, None)
    machine.update(1, [Chunk(text="a")])
    now = 0.0
    boom = TimeoutError("telegram timed out")
    for _ in range(2):
        action, _deadline = machine.plan(now)
        assert action is not None
        machine.apply(
            action, Result(ok=False, failure=Failure.TRANSIENT, error=boom), now
        )
        now += 100.0

    failed = next(event for event in events if isinstance(event, StreamFailed))
    assert failed.error is boom


def test_a_failure_with_no_exception_still_reports_nothing_odd(
    options: Options,
) -> None:
    events: list[Event] = []
    machine = _machine(options, events)
    machine.add_stream(1, None)
    machine.update(1, [Chunk(text="a")])
    action, _deadline = machine.plan(0.0)
    assert action is not None

    machine.apply(
        action, Result(ok=False, failure=Failure.MESSAGE_DEAD, reason="gone"), 0.0
    )

    failed = next(event for event in events if isinstance(event, MessageFailed))
    assert failed.error is None
    assert failed.reason == "gone"
