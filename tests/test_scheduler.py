import math

from aiogram_stream_sender.chunk import Chunk
from aiogram_stream_sender.machine.scheduler import plan
from aiogram_stream_sender.machine.timings import ChatTimings
from aiogram_stream_sender.message.intent import ActionKind
from aiogram_stream_sender.options import Options
from aiogram_stream_sender.stream.stream import SenderStream


def _stream(stream_id: int, texts: list[str]) -> SenderStream:
    stream = SenderStream(stream_id=stream_id)
    stream.update([Chunk(text=text) for text in texts])
    return stream


def test_first_action_is_immediate() -> None:
    options = Options(typing_enabled=False)
    action, deadline = plan({1: _stream(1, ["a"])}, ChatTimings(), {}, options, 0.0)
    assert action is not None
    assert action.kind is ActionKind.SEND
    assert deadline == 0.0


def test_kind_counters_are_independent() -> None:
    options = Options(typing_enabled=False)
    timings = ChatTimings(last_at={ActionKind.SEND: 10.0})
    streams = {1: _stream(1, ["a"])}
    action, _deadline = plan(streams, timings, {}, options, 10.5)
    assert action is None
    timings.last_at[ActionKind.EDIT] = 10.0
    action, _deadline = plan(streams, timings, {}, options, 11.0)
    assert action is not None


def test_hold_blocks_everything() -> None:
    options = Options()
    timings = ChatTimings(hold_until=50.0)
    action, deadline = plan({1: _stream(1, ["a"])}, timings, {}, options, 10.0)
    assert action is None
    assert deadline == 50.0


def test_final_stream_wins_tiebreak() -> None:
    options = Options(typing_enabled=False)
    first = _stream(1, ["a"])
    second = _stream(2, ["b"])
    second.finalize()
    action, _deadline = plan({1: first, 2: second}, ChatTimings(), {}, options, 0.0)
    assert action is not None
    assert action.stream_id == 2


def test_final_stream_still_throttled() -> None:
    options = Options(typing_enabled=False, send_interval=1.0)
    stream = _stream(1, ["a", "b"])
    stream.finalize()
    timings = ChatTimings(last_at={ActionKind.SEND: 10.0})
    action, deadline = plan({1: stream}, timings, {}, options, 10.2)
    assert action is None
    assert deadline == 11.0


def test_retry_at_delays_candidate() -> None:
    options = Options(typing_enabled=False)
    action, deadline = plan(
        {1: _stream(1, ["a"])}, ChatTimings(), {(1, 0): 5.0}, options, 0.0
    )
    assert action is None
    assert deadline == 5.0


def test_typing_when_idle_and_not_final() -> None:
    options = Options(action_interval=4.0)
    stream = _stream(1, [])
    action, _deadline = plan({1: stream}, ChatTimings(), {}, options, 4.0)
    assert action is not None
    assert action.kind is ActionKind.ACTION


def test_no_typing_for_final_stream() -> None:
    options = Options(action_interval=4.0)
    stream = _stream(1, [])
    stream.finalize()
    action, deadline = plan({1: stream}, ChatTimings(), {}, options, 100.0)
    assert action is None
    assert math.isinf(deadline)


def test_typing_fills_throttle_gap() -> None:
    options = Options(action_interval=4.0, send_interval=10.0)
    timings = ChatTimings(last_at={ActionKind.SEND: 0.0})
    action, _deadline = plan({1: _stream(1, ["a"])}, timings, {}, options, 5.0)
    assert action is not None
    assert action.kind is ActionKind.ACTION
