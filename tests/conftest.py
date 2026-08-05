import asyncio
import math
from collections.abc import Callable

import pytest

from aiogram_stream_sender.machine.action import Result, ScopedAction
from aiogram_stream_sender.message.intent import ActionKind
from aiogram_stream_sender.options import Options


class FakeClock:
    def __init__(self, start: float = 0.0) -> None:
        self.value = start
        self._sleepers: list[tuple[float, asyncio.Future[None]]] = []

    def now(self) -> float:
        return self.value

    async def sleep_until(self, deadline: float) -> None:
        if math.isinf(deadline):
            await asyncio.Event().wait()
            return
        if deadline <= self.value:
            return
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        self._sleepers.append((deadline, future))
        await future

    async def advance(self, delta: float) -> None:
        self.value += delta
        remaining: list[tuple[float, asyncio.Future[None]]] = []
        for deadline, future in self._sleepers:
            if deadline <= self.value and not future.done():
                future.set_result(None)
            elif not future.done():
                remaining.append((deadline, future))
        self._sleepers = remaining
        await asyncio.sleep(0)
        await asyncio.sleep(0)


class FakeExecutor:
    def __init__(
        self, responder: Callable[[ScopedAction], Result] | None = None
    ) -> None:
        self.calls: list[ScopedAction] = []
        self._responder = responder
        self._next_id = 100

    async def execute(self, action: ScopedAction) -> Result:
        self.calls.append(action)
        if self._responder is not None:
            return self._responder(action)
        if action.kind is ActionKind.SEND:
            self._next_id += 1
            return Result(ok=True, message_id=self._next_id)
        return Result(ok=True)


class FakeBot:
    def __init__(self, bot_id: int = 1) -> None:
        self.id = bot_id


@pytest.fixture
def options() -> Options:
    return Options(backoff_jitter=0.0, typing_enabled=False, machine_ttl=1000.0)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()
