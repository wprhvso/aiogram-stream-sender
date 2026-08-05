from aiogram_stream_sender.chunk import Chunk
from aiogram_stream_sender.message.intent import (
    ActionIntent,
    ActionKind,
    DeleteIntent,
    EditIntent,
    SendIntent,
    kind_of,
)


def test_kind_of_covers_every_intent() -> None:
    chunk = Chunk(text="a")
    assert kind_of(SendIntent(chunk=chunk)) is ActionKind.SEND
    assert kind_of(EditIntent(message_id=1, chunk=chunk)) is ActionKind.EDIT
    assert kind_of(DeleteIntent(message_id=1)) is ActionKind.DELETE
    assert kind_of(ActionIntent()) is ActionKind.ACTION
