import asyncio

import pytest

from aiogram_stream_sender.runtime.clock import MonotonicClock


async def test_now_is_monotonic() -> None:
    clock = MonotonicClock()
    first = clock.now()
    await asyncio.sleep(0)
    assert clock.now() >= first


async def test_past_deadline_returns_immediately() -> None:
    clock = MonotonicClock()
    await asyncio.wait_for(clock.sleep_until(clock.now() - 1.0), timeout=1.0)


async def test_future_deadline_sleeps() -> None:
    clock = MonotonicClock()
    start = clock.now()
    await clock.sleep_until(start + 0.01)
    assert clock.now() >= start + 0.01


async def test_infinite_deadline_blocks() -> None:
    clock = MonotonicClock()
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(clock.sleep_until(float("inf")), timeout=0.01)
