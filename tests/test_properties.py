import math
from random import Random

from hypothesis import given, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, initialize, invariant, rule

from aiogram_stream_sender.chunk import Chunk
from aiogram_stream_sender.errors import Failure
from aiogram_stream_sender.machine.action import Result
from aiogram_stream_sender.machine.machine import SenderMachine
from aiogram_stream_sender.machine.scheduler import plan
from aiogram_stream_sender.machine.timings import ChatTimings
from aiogram_stream_sender.message.intent import ActionKind
from aiogram_stream_sender.message.message import SenderMessage
from aiogram_stream_sender.options import Options
from aiogram_stream_sender.stream.stream import SenderStream

_TEXTS = st.lists(st.text(min_size=1, max_size=4), max_size=4)
_INTERVAL = st.floats(min_value=0.0, max_value=5.0)
_MOMENT = st.floats(min_value=0.0, max_value=100.0)
_SPECS = st.lists(st.tuples(_TEXTS, st.booleans()), min_size=1, max_size=3)
_LAST_AT = st.dictionaries(st.sampled_from(list(ActionKind)), _MOMENT, max_size=4)
_HUGE = 1_000_000.0


def _streams(specs: list[tuple[list[str], bool]]) -> dict[int, SenderStream]:
    built: dict[int, SenderStream] = {}
    for offset, (texts, is_final) in enumerate(specs):
        stream = SenderStream(stream_id=offset + 1)
        stream.update([Chunk(text=text) for text in texts])
        if is_final:
            stream.finalize()
        built[stream.stream_id] = stream
    return built


def _options(*, typing_enabled: bool, interval: float) -> Options:
    return Options(
        send_interval=interval,
        edit_interval=interval,
        delete_interval=interval,
        action_interval=interval,
        typing_enabled=typing_enabled,
        backoff_jitter=0.0,
        stream_ttl=_HUGE,
        machine_ttl=_HUGE,
    )


@settings(deadline=None, max_examples=200)
@given(
    specs=_SPECS,
    last_at=_LAST_AT,
    hold_until=_MOMENT,
    now=_MOMENT,
    interval=_INTERVAL,
    typing_enabled=st.booleans(),
)
def test_nothing_becomes_ready_before_the_deadline(
    specs: list[tuple[list[str], bool]],
    last_at: dict[ActionKind, float],
    hold_until: float,
    now: float,
    interval: float,
    typing_enabled: bool,
) -> None:
    streams = _streams(specs)
    timings = ChatTimings(last_at=dict(last_at), hold_until=hold_until)
    options = _options(typing_enabled=typing_enabled, interval=interval)

    action, deadline = plan(streams, timings, {}, options, now)
    if action is not None or not math.isfinite(deadline) or deadline <= now:
        return

    midpoint = now + (deadline - now) / 2
    if midpoint <= now or midpoint >= deadline:
        return
    assert plan(streams, timings, {}, options, midpoint)[0] is None


@settings(deadline=None, max_examples=200)
@given(
    specs=_SPECS,
    last_at=_LAST_AT,
    now=_MOMENT,
    interval=_INTERVAL,
    typing_enabled=st.booleans(),
)
def test_finite_deadline_always_yields_an_action(
    specs: list[tuple[list[str], bool]],
    last_at: dict[ActionKind, float],
    now: float,
    interval: float,
    typing_enabled: bool,
) -> None:
    streams = _streams(specs)
    timings = ChatTimings(last_at=dict(last_at))
    options = _options(typing_enabled=typing_enabled, interval=interval)

    action, deadline = plan(streams, timings, {}, options, now)
    if action is not None or not math.isfinite(deadline):
        return

    assert plan(streams, timings, {}, options, deadline)[0] is not None


@settings(deadline=None, max_examples=100)
@given(
    specs=_SPECS,
    hold_until=st.floats(min_value=1.0, max_value=100.0),
    now=_MOMENT,
    interval=_INTERVAL,
)
def test_hold_suppresses_every_stream(
    specs: list[tuple[list[str], bool]],
    hold_until: float,
    now: float,
    interval: float,
) -> None:
    if now >= hold_until:
        return
    streams = _streams(specs)
    timings = ChatTimings(hold_until=hold_until)
    options = _options(typing_enabled=True, interval=interval)

    action, deadline = plan(streams, timings, {}, options, now)

    assert action is None
    assert deadline == hold_until


@settings(deadline=None, max_examples=200)
@given(updates=st.lists(_TEXTS, min_size=1, max_size=4))
def test_finalized_stream_converges_to_last_update(updates: list[list[str]]) -> None:
    options = _options(typing_enabled=False, interval=0.0)
    machine = SenderMachine(1, 2, ChatTimings(), options, None, Random(0))
    machine.add_stream(1, None)
    now = 0.0
    message_id = 1000

    for texts in updates:
        machine.update(1, [Chunk(text=text) for text in texts])
        for _ in range(64):
            action, _deadline = machine.plan(now)
            if action is None:
                break
            message_id += 1
            machine.apply(action, Result(ok=True, message_id=message_id), now)
            now += 1.0

    machine.finalize(1)
    for _ in range(64):
        action, _deadline = machine.plan(now)
        if action is None:
            break
        message_id += 1
        machine.apply(action, Result(ok=True, message_id=message_id), now)
        now += 1.0

    status, ids, _reason = machine.outcome(1)
    assert machine.is_settled(1)
    assert status == "ok"
    assert len(ids) == len(updates[-1])


@settings(deadline=None, max_examples=200)
@given(
    ops=st.lists(
        st.sampled_from(["desire", "delete", "succeed", "fail", "terminal"]),
        max_size=12,
    )
)
def test_message_state_never_regresses(ops: list[str]) -> None:
    message = SenderMessage(desired=Chunk(text="a"))
    was_dead = False
    attempts = 0
    message_id = 500

    for op in ops:
        if op == "desire":
            message.set_desired(Chunk(text="b"))
        elif op == "delete":
            message.mark_for_deletion()
        elif op == "succeed":
            intent = message.intent()
            if intent is not None:
                message_id += 1
                message.on_success(intent, message_id)
        elif op == "fail":
            message.on_failure(terminal=False)
        else:
            message.on_failure(terminal=True)

        if was_dead:
            assert message.state == "dead"
            assert message.intent() is None
        was_dead = message.state == "dead"
        if op == "fail" and not was_dead:
            assert message.attempts >= attempts
        attempts = message.attempts


class SenderMachineRules(RuleBasedStateMachine):
    def __init__(self) -> None:
        super().__init__()
        self.options = _options(typing_enabled=False, interval=1.0)
        self.machine = SenderMachine(1, 2, ChatTimings(), self.options)
        self.now = 0.0
        self.alive: list[int] = []
        self.finalized: set[int] = set()
        self.settled: set[int] = set()
        self.failed: set[int] = set()
        self.evicted: set[int] = set()
        self.next_stream = 1
        self.next_message = 1000

    @initialize(
        interval=_INTERVAL,
        typing_enabled=st.booleans(),
        jitter=st.floats(min_value=0.0, max_value=0.5),
        max_attempts=st.integers(min_value=1, max_value=3),
        stream_ttl=st.floats(min_value=0.5, max_value=10.0),
        machine_ttl=st.floats(min_value=0.5, max_value=10.0),
        seed=st.integers(min_value=0, max_value=1024),
    )
    def configure(
        self,
        interval: float,
        typing_enabled: bool,
        jitter: float,
        max_attempts: int,
        stream_ttl: float,
        machine_ttl: float,
        seed: int,
    ) -> None:
        self.options = Options(
            send_interval=interval,
            edit_interval=interval,
            delete_interval=interval,
            action_interval=interval,
            typing_enabled=typing_enabled,
            backoff_jitter=jitter,
            max_attempts=max_attempts,
            stream_ttl=stream_ttl,
            machine_ttl=machine_ttl,
        )
        self.machine = SenderMachine(
            1, 2, ChatTimings(), self.options, None, Random(seed)
        )

    def _observe(self, stream_id: int) -> None:
        status, ids, _reason = self.machine.outcome(stream_id)
        settled = self.machine.is_settled(stream_id)
        if stream_id in self.evicted:
            return
        if stream_id in self.settled and not settled:
            self.evicted.add(stream_id)
            return
        if stream_id in self.failed and status != "failed":
            self.evicted.add(stream_id)
            return
        if settled:
            self.settled.add(stream_id)
        if status == "failed":
            self.failed.add(stream_id)
        assert len(ids) == len(set(ids))

    @rule(thread=st.one_of(st.none(), st.integers(min_value=1, max_value=3)))
    def open_stream(self, thread: int | None) -> None:
        if len(self.alive) >= 3:
            return
        stream_id = self.next_stream
        self.next_stream += 1
        self.machine.add_stream(stream_id, thread)
        self.alive.append(stream_id)

    @rule(index=st.integers(min_value=0, max_value=2), texts=_TEXTS)
    def update(self, index: int, texts: list[str]) -> None:
        writable = [sid for sid in self.alive if sid not in self.finalized]
        if not writable:
            return
        stream_id = writable[index % len(writable)]
        self.machine.update(stream_id, [Chunk(text=text) for text in texts])

    @rule(index=st.integers(min_value=0, max_value=2))
    def finalize(self, index: int) -> None:
        writable = [sid for sid in self.alive if sid not in self.finalized]
        if not writable:
            return
        stream_id = writable[index % len(writable)]
        self.machine.finalize(stream_id)
        self.finalized.add(stream_id)

    @rule(
        kind=st.sampled_from(
            ["ok", "transient", "message_dead", "stream_dead", "hold"]
        ),
        retry=st.floats(min_value=0.1, max_value=5.0),
    )
    def step(self, kind: str, retry: float) -> None:
        action, _deadline = self.machine.plan(self.now)
        if action is None:
            return
        if kind == "ok":
            self.next_message += 1
            result = Result(ok=True, message_id=self.next_message)
        elif kind == "transient":
            result = Result(ok=False, failure=Failure.TRANSIENT, reason="flaky")
        elif kind == "message_dead":
            result = Result(ok=False, failure=Failure.MESSAGE_DEAD, reason="too long")
        elif kind == "stream_dead":
            result = Result(ok=False, failure=Failure.STREAM_DEAD, reason="blocked")
        else:
            result = Result(ok=False, retry_after=retry)
        self.machine.apply(action, result, self.now)

    @rule(delta=st.floats(min_value=0.0, max_value=20.0))
    def advance(self, delta: float) -> None:
        for stream_id in self.alive:
            self._observe(stream_id)
        self.now += delta
        self.machine.sweep(self.now)

    @invariant()
    def deadline_is_a_lower_bound_on_work(self) -> None:
        action, deadline = self.machine.plan(self.now)
        if action is not None:
            assert deadline == self.now
            return
        if not math.isfinite(deadline) or deadline <= self.now:
            return
        midpoint = self.now + (deadline - self.now) / 2
        if midpoint <= self.now or midpoint >= deadline:
            return
        assert self.machine.plan(midpoint)[0] is None

    @invariant()
    def outcomes_are_stable_until_eviction(self) -> None:
        for stream_id in self.alive:
            self._observe(stream_id)

    @invariant()
    def eviction_implies_settlement(self) -> None:
        assert self.evicted <= self.settled

    @invariant()
    def idle_machine_needs_time_to_expire(self) -> None:
        if self.machine.is_evictable(self.now):
            assert self.now >= self.options.machine_ttl


TestSenderMachineRules = SenderMachineRules.TestCase
TestSenderMachineRules.settings = settings(
    deadline=None, max_examples=100, stateful_step_count=30
)
