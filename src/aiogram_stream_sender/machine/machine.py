import math
from collections.abc import Sequence
from random import Random
from typing import Final

from aiogram_stream_sender.chunk import Chunk
from aiogram_stream_sender.errors import Failure
from aiogram_stream_sender.events import (
    ChatHold,
    EventSink,
    MessageFailed,
    StreamFailed,
    emit,
)
from aiogram_stream_sender.machine.action import Result, ScopedAction
from aiogram_stream_sender.machine.scheduler import plan
from aiogram_stream_sender.machine.timings import ChatTimings
from aiogram_stream_sender.message.intent import ActionKind, SendIntent
from aiogram_stream_sender.options import Options
from aiogram_stream_sender.stream.stream import SenderStream


class SenderMachine:
    def __init__(
        self,
        bot_id: int,
        chat_id: int,
        timings: ChatTimings,
        options: Options,
        sink: EventSink | None = None,
        rng: Random | None = None,
    ) -> None:
        self.bot_id: Final = bot_id
        self.chat_id: Final = chat_id
        self._timings: Final = timings
        self._options: Final = options
        self._sink: Final = sink
        self._rng: Final = rng or Random()
        self._streams: dict[int, SenderStream] = {}
        self._retry_at: dict[tuple[int, int], float] = {}
        self._done_at: dict[int, float] = {}
        self._served: dict[int, int] = {}
        self._turn = 0
        self._idle_since: float | None = None

    def add_stream(
        self, stream_id: int, thread_id: int | None, *, typing: bool = True
    ) -> None:
        self._streams[stream_id] = SenderStream(
            stream_id=stream_id, thread_id=thread_id, typing=typing
        )
        self._idle_since = None

    def update(self, stream_id: int, chunks: Sequence[Chunk]) -> None:
        stream = self._streams.get(stream_id)
        if stream is None:
            return
        stream.update(chunks)
        size = len(stream.messages)
        stale = [
            address
            for address in self._retry_at
            if address[0] == stream_id and address[1] >= size
        ]
        for address in stale:
            self._retry_at.pop(address, None)

    def finalize(self, stream_id: int) -> None:
        stream = self._streams.get(stream_id)
        if stream is not None:
            stream.finalize()

    def finalize_all(self) -> None:
        for stream in self._streams.values():
            stream.finalize()

    def is_settled(self, stream_id: int) -> bool:
        stream = self._streams.get(stream_id)
        return stream is None or stream.is_done

    def outcome(self, stream_id: int) -> tuple[str, list[int], str]:
        stream = self._streams.get(stream_id)
        if stream is None:
            return "ok", [], ""
        if stream.state == "dead":
            return "failed", stream.message_ids, stream.reason or "stream failed"
        if stream.has_dead_messages:
            return "partial", stream.message_ids, ""
        return "ok", stream.message_ids, ""

    def plan(self, now: float) -> tuple[ScopedAction | None, float]:
        for stream in self._streams.values():
            self._note_done(stream, now)
        action, deadline = plan(
            self._streams,
            self._timings,
            self._retry_at,
            self._served,
            self._options,
            now,
        )
        if action is not None:
            self._turn += 1
            self._served[action.stream_id] = self._turn
            stream = self._streams.get(action.stream_id)
            if stream is not None:
                stream.mark_in_flight(action.index, value=True)
            return action, deadline
        return None, min(deadline, self._ttl_deadline())

    def apply(self, action: ScopedAction, result: Result, now: float) -> None:
        stream = self._streams.get(action.stream_id)
        if stream is not None:
            stream.mark_in_flight(action.index, value=False)

        if result.retry_after is not None:
            self._timings.hold_until = now + result.retry_after
            emit(
                self._sink,
                ChatHold(self.bot_id, self.chat_id, self._timings.hold_until),
            )
            return

        self._timings.last_at[action.kind] = now
        if action.kind is ActionKind.ACTION or stream is None:
            return
        address = (action.stream_id, action.index)

        if result.ok and not self._is_void_send(action, result):
            self._retry_at.pop(address, None)
            stream.apply_success(action.index, action.intent, result.message_id)
            self._note_done(stream, now)
            return

        if result.failure is Failure.STREAM_DEAD:
            stream.kill(result.reason, result.error)
            emit(
                self._sink,
                StreamFailed(
                    self.bot_id,
                    self.chat_id,
                    action.stream_id,
                    result.reason,
                    result.error,
                ),
            )
            self._note_done(stream, now)
            return

        if result.failure is Failure.MESSAGE_DEAD:
            self._retry_at.pop(address, None)
            stream.apply_failure(action.index, terminal=True)
            emit(
                self._sink,
                MessageFailed(
                    self.bot_id,
                    self.chat_id,
                    action.stream_id,
                    action.index,
                    result.reason,
                    result.error,
                ),
            )
            self._note_done(stream, now)
            return

        stream.apply_failure(action.index, terminal=False)
        attempts = stream.attempts_at(action.index)
        if attempts >= self._options.max_attempts:
            stream.kill(result.reason or "max attempts exceeded", result.error)
            emit(
                self._sink,
                StreamFailed(
                    self.bot_id,
                    self.chat_id,
                    action.stream_id,
                    stream.reason or "",
                    stream.error,
                ),
            )
            self._note_done(stream, now)
            return
        self._retry_at[address] = now + self._backoff(attempts)

    def sweep(self, now: float) -> None:
        for stream in self._streams.values():
            self._note_done(stream, now)
        expired = [
            stream_id
            for stream_id, done_at in self._done_at.items()
            if now - done_at >= self._options.stream_ttl
        ]
        vacated = (
            max(
                (self._done_at[stream_id] for stream_id in expired),
                default=now,
            )
            + self._options.stream_ttl
        )
        for stream_id in expired:
            self._streams.pop(stream_id, None)
            self._done_at.pop(stream_id, None)
            self._served.pop(stream_id, None)
            for address in [key for key in self._retry_at if key[0] == stream_id]:
                self._retry_at.pop(address, None)
        if not self._streams:
            if self._idle_since is None:
                self._idle_since = min(vacated, now)
        else:
            self._idle_since = None

    def is_evictable(self, now: float) -> bool:
        return (
            not self._streams
            and self._idle_since is not None
            and now - self._idle_since >= self._options.machine_ttl
        )

    def kill(self, stream_id: int, reason: str) -> None:
        stream = self._streams.get(stream_id)
        if stream is None or stream.is_done:
            return
        stream.kill(reason)
        emit(self._sink, StreamFailed(self.bot_id, self.chat_id, stream_id, reason))

    def kill_all(self, reason: str) -> None:
        for stream in self._streams.values():
            if not stream.is_done:
                stream.kill(reason)
                emit(
                    self._sink,
                    StreamFailed(self.bot_id, self.chat_id, stream.stream_id, reason),
                )

    def _is_void_send(self, action: ScopedAction, result: Result) -> bool:
        return isinstance(action.intent, SendIntent) and result.message_id is None

    def _note_done(self, stream: SenderStream, now: float) -> None:
        if stream.is_done and stream.stream_id not in self._done_at:
            self._done_at[stream.stream_id] = now

    def _ttl_deadline(self) -> float:
        deadline = math.inf
        for done_at in self._done_at.values():
            deadline = min(deadline, done_at + self._options.stream_ttl)
        if not self._streams and self._idle_since is not None:
            deadline = min(deadline, self._idle_since + self._options.machine_ttl)
        return deadline

    def _backoff(self, attempts: int) -> float:
        if attempts <= 0:
            return 0.0
        base = min(
            self._options.backoff_base * 2 ** (attempts - 1),
            self._options.backoff_max,
        )
        jitter = self._options.backoff_jitter
        if jitter <= 0:
            return base
        return base * (1.0 + self._rng.uniform(-jitter, jitter))
