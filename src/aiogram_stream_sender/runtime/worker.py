import asyncio
import logging
from typing import Final, Literal

from aiogram_stream_sender.chunk import Chunk
from aiogram_stream_sender.machine.machine import SenderMachine
from aiogram_stream_sender.options import Options
from aiogram_stream_sender.runtime.clock import Clock
from aiogram_stream_sender.transport.executor import Executor

log = logging.getLogger(__name__)

WorkerStatus = Literal["running", "stopping"]
Outcome = tuple[str, list[int], str]


class MachineWorker:
    def __init__(
        self,
        machine: SenderMachine,
        executor: Executor,
        clock: Clock,
        options: Options,
    ) -> None:
        self._machine: Final = machine
        self._executor: Final = executor
        self._clock: Final = clock
        self._options: Final = options
        self._wakeup: Final = asyncio.Event()
        self._waiters: Final[dict[int, asyncio.Event]] = {}
        self._outcomes: Final[dict[int, Outcome]] = {}
        self._closing = False
        self.status: WorkerStatus = "running"
        self.task: asyncio.Task[None] | None = None

    def start(self) -> None:
        self.task = asyncio.create_task(self.run())

    def register(self, stream_id: int, thread_id: int | None) -> asyncio.Event:
        self._machine.add_stream(stream_id, thread_id)
        event = asyncio.Event()
        self._waiters[stream_id] = event
        return event

    def unregister(self, stream_id: int) -> None:
        self._waiters.pop(stream_id, None)
        self._outcomes.pop(stream_id, None)

    def outcome(self, stream_id: int) -> Outcome:
        return self._outcomes.get(stream_id) or self._machine.outcome(stream_id)

    def update(self, stream_id: int, chunks: tuple[Chunk, ...]) -> None:
        self._machine.update(stream_id, chunks)
        self._wakeup.set()

    def finalize(self, stream_id: int) -> None:
        self._machine.finalize(stream_id)
        self._wakeup.set()

    def finalize_all(self) -> None:
        self._closing = True
        self._machine.finalize_all()
        self._wakeup.set()

    async def run(self) -> None:
        try:
            while True:
                self._wakeup.clear()
                now = self._clock.now()
                action, deadline = self._machine.plan(now)

                if action is not None:
                    result = await self._executor.execute(action)
                    self._machine.apply(action, result, self._clock.now())
                    self._settle()
                    continue

                self._machine.sweep(now)
                self._settle()
                if self._closing:
                    self.status = "stopping"
                    return
                if self._machine.is_evictable(now) and not self._waiters:
                    self.status = "stopping"
                    return

                await self._sleep(deadline)
        except asyncio.CancelledError:
            raise
        except BaseException:
            log.exception("worker crashed")
            self._machine.kill_all("worker crashed")
            self._settle()
            self.status = "stopping"
            raise

    def _settle(self) -> None:
        for stream_id, event in list(self._waiters.items()):
            if event.is_set():
                continue
            if self._machine.is_settled(stream_id):
                self._outcomes[stream_id] = self._machine.outcome(stream_id)
                event.set()

    async def _sleep(self, deadline: float) -> None:
        if self._wakeup.is_set():
            return
        sleeper = asyncio.create_task(self._clock.sleep_until(deadline))
        waker = asyncio.create_task(self._wakeup.wait())
        try:
            await asyncio.wait({sleeper, waker}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            sleeper.cancel()
            waker.cancel()
