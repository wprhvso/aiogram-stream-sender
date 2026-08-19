import asyncio
import contextvars
import logging
from typing import Final, Literal

from aiogram_stream_sender.chunk import Chunk
from aiogram_stream_sender.machine.action import Result, ScopedAction
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
        self._contexts: Final[dict[int, contextvars.Context]] = {}
        self._closing = False
        self.status: WorkerStatus = "running"
        self.task: asyncio.Task[None] | None = None

    @property
    def is_alive(self) -> bool:
        return (
            self.status == "running" and self.task is not None and not self.task.done()
        )

    def start(self) -> None:
        task = asyncio.create_task(self.run(), context=contextvars.Context())
        task.add_done_callback(self._retrieve)
        self.task = task

    @staticmethod
    def _retrieve(task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        if (error := task.exception()) is not None:
            log.error("sender worker stopped", exc_info=error)

    def register(
        self,
        stream_id: int,
        thread_id: int | None,
        *,
        typing: bool = True,
        context: contextvars.Context | None = None,
    ) -> asyncio.Event:
        self._machine.add_stream(stream_id, thread_id, typing=typing)
        event = asyncio.Event()
        self._waiters[stream_id] = event
        if context is not None:
            self._contexts[stream_id] = context
        return event

    def unregister(self, stream_id: int) -> None:
        self._waiters.pop(stream_id, None)
        self._outcomes.pop(stream_id, None)
        self._contexts.pop(stream_id, None)

    def outcome(self, stream_id: int) -> Outcome:
        return self._outcomes.get(stream_id) or self._machine.outcome(stream_id)

    async def settled(self, stream_id: int) -> None:
        event = self._waiters.get(stream_id)
        if event is None:
            return
        task = self.task
        if task is None or task.done():
            if not event.is_set():
                self._machine.kill(stream_id, "worker stopped")
                self._settle()
            return

        waiter = asyncio.ensure_future(event.wait())
        try:
            _ = await asyncio.wait({waiter, task}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            _ = waiter.cancel()

        if not event.is_set():
            self._machine.kill(stream_id, "worker stopped")
            self._settle()

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

    async def _execute(self, action: ScopedAction) -> Result:
        context = self._contexts.get(action.stream_id)
        if context is None:
            return await self._executor.execute(action)

        task = asyncio.create_task(self._executor.execute(action), context=context)
        try:
            return await task
        except asyncio.CancelledError:
            _ = task.cancel()
            raise

    def _apply(self, action: ScopedAction, result: Result) -> None:
        context = self._contexts.get(action.stream_id)
        now = self._clock.now()
        if context is None:
            self._machine.apply(action, result, now)
            return
        context.run(self._machine.apply, action, result, now)

    async def run(self) -> None:
        try:
            while True:
                self._wakeup.clear()
                now = self._clock.now()
                action, deadline = self._machine.plan(now)

                if action is not None:
                    result = await self._execute(action)
                    self._apply(action, result)
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
            self._machine.kill_all("worker cancelled")
            self._settle()
            self.status = "stopping"
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
            done, _pending = await asyncio.wait(
                {sleeper, waker}, return_when=asyncio.FIRST_COMPLETED
            )
            if sleeper in done and not sleeper.cancelled():
                sleeper.result()
        finally:
            _ = sleeper.cancel()
            _ = waker.cancel()
