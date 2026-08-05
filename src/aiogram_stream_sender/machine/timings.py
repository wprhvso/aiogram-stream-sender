from collections import OrderedDict
from dataclasses import dataclass, field

from aiogram_stream_sender.message.intent import ActionKind


@dataclass(slots=True)
class ChatTimings:
    last_at: dict[ActionKind, float] = field(default_factory=dict)
    hold_until: float = 0.0


class TimingsCache:
    def __init__(self, capacity: int) -> None:
        self._capacity = capacity
        self._items: OrderedDict[tuple[int, int], ChatTimings] = OrderedDict()

    def get(self, scope: tuple[int, int]) -> ChatTimings:
        timings = self._items.get(scope)
        if timings is None:
            timings = ChatTimings()
            self._items[scope] = timings
        self._items.move_to_end(scope)
        while len(self._items) > self._capacity:
            self._items.popitem(last=False)
        return timings
