from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramRetryAfter,
)
from aiogram.methods import SendMessage

from aiogram_stream_sender.errors import Failure
from aiogram_stream_sender.transport.classify import classify, is_not_modified


def _method() -> SendMessage:
    return SendMessage(chat_id=1, text="x")


def _bad(message: str) -> TelegramBadRequest:
    return TelegramBadRequest(method=_method(), message=message)


def test_retry_after_is_transient_with_delay() -> None:
    error = TelegramRetryAfter(method=_method(), message="flood", retry_after=12)
    failure, retry_after, _reason = classify(error)
    assert failure is Failure.TRANSIENT
    assert retry_after == 12.0


def test_not_modified_detected() -> None:
    assert is_not_modified(_bad("Bad Request: message is not modified"))


def test_message_level_errors() -> None:
    for text in (
        "message to edit not found",
        "message is too long",
        "can't parse entities",
    ):
        failure, _retry, _reason = classify(_bad(text))
        assert failure is Failure.MESSAGE_DEAD


def test_stream_level_errors() -> None:
    for text in (
        "bot was blocked by the user",
        "chat not found",
        "have no rights to send",
    ):
        failure, _retry, _reason = classify(_bad(text))
        assert failure is Failure.STREAM_DEAD


def test_forbidden_is_stream_level() -> None:
    error = TelegramForbiddenError(method=_method(), message="forbidden")
    failure, _retry, _reason = classify(error)
    assert failure is Failure.STREAM_DEAD


def test_unknown_defaults_to_transient() -> None:
    failure, _retry, _reason = classify(RuntimeError("boom"))
    assert failure is Failure.TRANSIENT
