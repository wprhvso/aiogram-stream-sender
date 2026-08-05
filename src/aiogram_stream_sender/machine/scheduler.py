import math
from collections.abc import Mapping

from aiogram_stream_sender.machine.action import ScopedAction
from aiogram_stream_sender.machine.timings import ChatTimings
from aiogram_stream_sender.message.intent import ActionIntent, ActionKind, kind_of
from aiogram_stream_sender.options import Options
from aiogram_stream_sender.stream.stream import SenderStream

_INTERVALS = {
    ActionKind.SEND: "send_interval",
    ActionKind.EDIT: "edit_interval",
    ActionKind.DELETE: "delete_interval",
    ActionKind.ACTION: "action_interval",
}


def interval_for(options: Options, kind: ActionKind) -> float:
    value: float = getattr(options, _INTERVALS[kind])
    return value


def _next_at(timings: ChatTimings, options: Options, kind: ActionKind) -> float:
    last_at = timings.last_at.get(kind)
    if last_at is None:
        return -math.inf
    return last_at + interval_for(options, kind)


def plan(
    streams: Mapping[int, SenderStream],
    timings: ChatTimings,
    retry_at: Mapping[tuple[int, int], float],
    options: Options,
    now: float,
) -> tuple[ScopedAction | None, float]:
    if now < timings.hold_until:
        return None, timings.hold_until

    best_key: tuple[float, int, int] | None = None
    best_action: ScopedAction | None = None
    typing_stream: SenderStream | None = None

    for stream_id in sorted(streams):
        stream = streams[stream_id]
        if stream.is_done:
            continue
        if not stream.is_final and typing_stream is None:
            typing_stream = stream
        found = stream.pending()
        if found is None:
            continue
        index, intent = found
        kind = kind_of(intent)
        ready = max(
            _next_at(timings, options, kind),
            retry_at.get((stream_id, index), -math.inf),
        )
        key = (ready, 0 if stream.is_final else 1, stream_id)
        if best_key is None or key < best_key:
            best_key = key
            best_action = ScopedAction(
                stream_id=stream_id,
                index=index,
                thread_id=stream.thread_id,
                intent=intent,
                kind=kind,
            )

    typing_at = math.inf
    if options.typing_enabled and typing_stream is not None:
        typing_at = _next_at(timings, options, ActionKind.ACTION)

    if best_key is not None and now >= best_key[0] and best_action is not None:
        return best_action, now

    if now >= typing_at and typing_stream is not None:
        return (
            ScopedAction(
                stream_id=typing_stream.stream_id,
                index=-1,
                thread_id=typing_stream.thread_id,
                intent=ActionIntent(),
                kind=ActionKind.ACTION,
            ),
            now,
        )

    ready_min = best_key[0] if best_key is not None else math.inf
    return None, min(ready_min, typing_at)
