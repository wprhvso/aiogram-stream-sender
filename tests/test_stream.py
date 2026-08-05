import pytest

from aiogram_stream_sender.chunk import Chunk
from aiogram_stream_sender.errors import StreamFinalizedError
from aiogram_stream_sender.message.intent import DeleteIntent, EditIntent, SendIntent
from aiogram_stream_sender.stream.stream import SenderStream


def _deliver(stream: SenderStream, message_id: int) -> None:
    found = stream.pending()
    assert found is not None
    index, intent = found
    stream.apply_success(index, intent, message_id)


def test_sends_in_order() -> None:
    stream = SenderStream(stream_id=1)
    stream.update([Chunk(text="a"), Chunk(text="b")])
    found = stream.pending()
    assert found is not None
    assert found[0] == 0
    _deliver(stream, 10)
    found = stream.pending()
    assert found is not None
    assert found[0] == 1
    assert isinstance(found[1], SendIntent)


def test_edit_preferred_over_tail_delete() -> None:
    stream = SenderStream(stream_id=1)
    stream.update([Chunk(text="a"), Chunk(text="b")])
    _deliver(stream, 10)
    _deliver(stream, 11)
    stream.update([Chunk(text="a2")])
    found = stream.pending()
    assert found is not None
    assert found[0] == 0
    assert isinstance(found[1], EditIntent)
    _deliver(stream, 10)
    found = stream.pending()
    assert found is not None
    assert found[0] == 1
    assert isinstance(found[1], DeleteIntent)


def test_delete_pops_tail() -> None:
    stream = SenderStream(stream_id=1)
    stream.update([Chunk(text="a"), Chunk(text="b")])
    _deliver(stream, 10)
    _deliver(stream, 11)
    stream.update([Chunk(text="a")])
    _deliver(stream, 11)
    assert len(stream.messages) == 1
    assert stream.message_ids == [10]


def test_unsent_tail_dropped_without_delete() -> None:
    stream = SenderStream(stream_id=1)
    stream.update([Chunk(text="a"), Chunk(text="b")])
    stream.update([Chunk(text="a")])
    assert len(stream.messages) == 1
    assert stream.pending() is not None


def test_finalize_settles_when_idle() -> None:
    stream = SenderStream(stream_id=1)
    stream.update([Chunk(text="a")])
    _deliver(stream, 10)
    stream.finalize()
    assert stream.state == "finished"
    assert stream.is_done


def test_update_after_finalize_raises() -> None:
    stream = SenderStream(stream_id=1)
    stream.finalize()
    with pytest.raises(StreamFinalizedError):
        stream.update([Chunk(text="a")])


def test_kill_marks_everything_dead() -> None:
    stream = SenderStream(stream_id=1)
    stream.update([Chunk(text="a")])
    _deliver(stream, 10)
    stream.kill("blocked")
    assert stream.state == "dead"
    assert stream.pending() is None
    assert stream.message_ids == []
