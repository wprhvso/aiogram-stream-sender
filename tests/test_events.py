import logging

from aiogram_stream_sender.events import ChatHold, Event, MessageFailed, emit


def test_emit_without_sink_is_noop() -> None:
    emit(None, ChatHold(1, 2, 3.0))


def test_emit_delivers_event() -> None:
    seen: list[Event] = []
    event = MessageFailed(1, 2, 3, 0, "boom")
    emit(seen.append, event)
    assert seen == [event]


def test_emit_swallows_sink_errors(caplog: logging.LogCaptureFixture) -> None:
    def broken(event: Event) -> None:
        raise RuntimeError(str(event))

    with caplog.at_level(logging.ERROR):
        emit(broken, ChatHold(1, 2, 3.0))

    assert "event sink failed" in caplog.text
