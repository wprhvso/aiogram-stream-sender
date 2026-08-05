from aiogram_stream_sender.chunk import Chunk
from aiogram_stream_sender.message.intent import DeleteIntent, EditIntent, SendIntent
from aiogram_stream_sender.message.message import SenderMessage


def test_lifecycle_send_edit_delete() -> None:
    message = SenderMessage(desired=Chunk(text="a"))
    intent = message.intent()
    assert isinstance(intent, SendIntent)

    message.on_success(intent, message_id=7)
    assert message.intent() is None
    assert message.state == "live"

    message.set_desired(Chunk(text="b"))
    intent = message.intent()
    assert isinstance(intent, EditIntent)
    message.on_success(intent, message_id=7)
    assert message.intent() is None

    message.mark_for_deletion()
    intent = message.intent()
    assert isinstance(intent, DeleteIntent)
    message.on_success(intent, message_id=7)
    assert message.state == "dead"
    assert message.intent() is None


def test_unsent_message_marked_for_deletion_has_no_intent() -> None:
    message = SenderMessage(desired=Chunk(text="a"))
    message.mark_for_deletion()
    assert message.intent() is None


def test_failures_accumulate_and_terminal_kills() -> None:
    message = SenderMessage(desired=Chunk(text="a"))
    message.on_failure(terminal=False)
    message.on_failure(terminal=False)
    assert message.attempts == 2
    message.on_failure(terminal=True)
    assert message.state == "dead"
    assert message.intent() is None


def test_success_resets_attempts() -> None:
    message = SenderMessage(desired=Chunk(text="a"))
    message.on_failure(terminal=False)
    intent = message.intent()
    assert intent is not None
    message.on_success(intent, message_id=1)
    assert message.attempts == 0
