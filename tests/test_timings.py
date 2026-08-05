from aiogram_stream_sender.machine.timings import TimingsCache
from aiogram_stream_sender.message.intent import ActionKind


def test_same_scope_returns_same_object() -> None:
    cache = TimingsCache(4)
    first = cache.get((1, 2))
    first.last_at[ActionKind.SEND] = 5.0
    assert cache.get((1, 2)) is first


def test_capacity_evicts_least_recently_used() -> None:
    cache = TimingsCache(2)
    oldest = cache.get((1, 1))
    cache.get((1, 2))
    cache.get((1, 1))
    cache.get((1, 3))
    assert cache.get((1, 1)) is oldest
    assert cache.get((1, 2)) is not None
