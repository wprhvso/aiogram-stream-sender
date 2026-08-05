import asyncio
import math
import time
from typing import Protocol


class Clock(Protocol):
    def now(self) -> float: ...

    async def sleep_until(self, deadline: float) -> None: ...


class MonotonicClock:
    def now(self) -> float:
        return time.monotonic()

    async def sleep_until(self, deadline: float) -> None:
        if math.isinf(deadline):
            await asyncio.Event().wait()
            return
        delay = deadline - self.now()
        if delay > 0:
            await asyncio.sleep(delay)
