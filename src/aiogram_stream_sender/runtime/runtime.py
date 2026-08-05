import asyncio
import contextlib
import itertools
from collections.abc import Callable
from typing import Final

from aiogram import Bot

from aiogram_stream_sender.events import EventSink
from aiogram_stream_sender.machine.machine import SenderMachine
from aiogram_stream_sender.machine.timings import TimingsCache
from aiogram_stream_sender.options import Options
from aiogram_stream_sender.runtime.clock import Clock, MonotonicClock
from aiogram_stream_sender.runtime.scoped import LiveStream, ScopedSender
from aiogram_stream_sender.runtime.worker import MachineWorker
from aiogram_stream_sender.transport.executor import Executor, TelegramExecutor

Scope = tuple[int, int]
ExecutorFactory = Callable[[Bot, int], Executor]


class SenderRuntime:
    def __init__(
        self,
        options: Options | None = None,
        *,
        clock: Clock | None = None,
        sink: EventSink | None = None,
        executor_factory: ExecutorFactory | None = None,
    ) -> None:
        self._options: Final = options or Options()
        self._clock: Final = clock or MonotonicClock()
        self._sink: Final = sink
        self._executor_factory: Final[ExecutorFactory] = (
            executor_factory or TelegramExecutor
        )
        self._timings: Final = TimingsCache(self._options.timings_capacity)
        self._workers: dict[Scope, MachineWorker] = {}
        self._ids: Final = itertools.count(1)
        self._threads: dict[int, int | None] = {}

    def scoped(self, bot: Bot, chat_id: int, thread_id: int | None) -> ScopedSender:
        return ScopedSender(self, bot, chat_id, thread_id)

    def thread_of(self, stream_id: int) -> int | None:
        return self._threads.get(stream_id)

    def open_stream(self, bot: Bot, chat_id: int, thread_id: int | None) -> LiveStream:
        scope: Scope = (bot.id, chat_id)
        worker = self._workers.get(scope)
        if worker is None or worker.status != "running":
            machine = SenderMachine(
                bot_id=bot.id,
                chat_id=chat_id,
                timings=self._timings.get(scope),
                options=self._options,
                sink=self._sink,
            )
            executor = self._executor_factory(bot, chat_id)
            worker = MachineWorker(machine, executor, self._clock, self._options)
            self._workers[scope] = worker
            worker.start()
        stream_id = next(self._ids)
        self._threads[stream_id] = thread_id
        return LiveStream(
            self,
            worker,
            stream_id,
            raise_on_failure=self._options.raise_on_failure,
        )

    def prune(self) -> None:
        for scope, worker in list(self._workers.items()):
            if worker.status == "stopping" and (
                worker.task is None or worker.task.done()
            ):
                self._workers.pop(scope, None)

    async def aclose(self) -> None:
        for worker in self._workers.values():
            worker.finalize_all()
        tasks = [
            worker.task for worker in self._workers.values() if worker.task is not None
        ]
        if tasks:
            _done, pending = await asyncio.wait(
                tasks, timeout=self._options.shutdown_timeout
            )
            for task in pending:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        self._workers.clear()
        self._threads.clear()
