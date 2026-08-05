from aiogram_stream_sender.chunk import Chunk
from aiogram_stream_sender.message.intent import DeleteIntent, SendIntent
from aiogram_stream_sender.stream.stream import SenderStream


def test_out_of_range_indices_are_ignored() -> None:
    stream = SenderStream(stream_id=1)
    stream.update([Chunk(text="a")])
    stream.apply_success(9, SendIntent(chunk=Chunk(text="a")), 10)
    stream.apply_failure(9, terminal=True)
    assert stream.attempts_at(9) == 0
    assert stream.message_ids == []


def test_update_on_dead_stream_is_ignored() -> None:
    stream = SenderStream(stream_id=1)
    stream.kill("blocked")
    stream.update([Chunk(text="a")])
    assert stream.messages == []
    assert stream.state == "dead"


def test_finalize_does_not_revive_dead_stream() -> None:
    stream = SenderStream(stream_id=1)
    stream.update([Chunk(text="a")])
    stream.kill("blocked")
    stream.finalize()
    assert stream.state == "dead"
    assert stream.is_done


def test_delete_in_middle_keeps_slot() -> None:
    stream = SenderStream(stream_id=1)
    stream.update([Chunk(text="a"), Chunk(text="b"), Chunk(text="c")])
    for message_id in (10, 11, 12):
        found = stream.pending()
        assert found is not None
        stream.apply_success(found[0], found[1], message_id)
    stream.update([Chunk(text="a")])
    found = stream.pending()
    assert found is not None
    assert isinstance(found[1], DeleteIntent)
    assert found[0] == 2


def test_empty_stream_finalizes_immediately() -> None:
    stream = SenderStream(stream_id=1)
    stream.finalize()
    assert stream.state == "finished"
    assert stream.pending() is None
    assert not stream.has_dead_messages
