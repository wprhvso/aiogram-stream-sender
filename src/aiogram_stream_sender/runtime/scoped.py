from collections.abc import Mapping, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any, Final, Self

from aiogram import Bot

from aiogram_stream_sender.chunk import Chunk
from aiogram_stream_sender.errors import StreamFailedError
from aiogram_stream_sender.runtime.worker import MachineWorker

if TYPE_CHECKING:
    from aiogram_stream_sender.runtime.runtime import SenderRuntime


class LiveStream:
    def __init__(
        self,
        runtime: "SenderRuntime",
        worker: MachineWorker,
        stream_id: int,
        *,
        raise_on_failure: bool,
    ) -> None:
        self._runtime: Final = runtime
        self._worker: Final = worker
        self._stream_id: Final = stream_id
        self._raise_on_failure: Final = raise_on_failure
        self._settled: Final = worker.register(stream_id, runtime.thread_of(stream_id))
        self._closed = False

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if exc is not None:
            await self.finish(raise_on_failure=False)
            return
        _ = await self.finish()

    def update(self, chunks: Sequence[Mapping[str, Any]]) -> None:
        if self._closed:
            return
        self._worker.update(
            self._stream_id, tuple(Chunk.from_mapping(chunk) for chunk in chunks)
        )

    async def finish(self, *, raise_on_failure: bool | None = None) -> list[int]:
        should_raise = (
            self._raise_on_failure if raise_on_failure is None else raise_on_failure
        )
        if self._closed:
            status, message_ids, reason = self._worker.outcome(self._stream_id)
            return message_ids
        self._closed = True
        self._worker.finalize(self._stream_id)
        await self._settled.wait()
        status, message_ids, reason = self._worker.outcome(self._stream_id)
        self._worker.unregister(self._stream_id)
        if status == "failed" and should_raise:
            raise StreamFailedError(reason, message_ids)
        return message_ids


class ScopedSender:
    def __init__(
        self,
        runtime: "SenderRuntime",
        bot: Bot,
        chat_id: int,
        thread_id: int | None,
    ) -> None:
        self._runtime: Final = runtime
        self._bot: Final = bot
        self._chat_id: Final = chat_id
        self._thread_id: Final = thread_id

    def stream(self) -> LiveStream:
        return self._runtime.open_stream(self._bot, self._chat_id, self._thread_id)
