from aiogram.exceptions import TelegramNetworkError, TelegramServerError
from aiogram.methods import SendMessage

from aiogram_stream_sender.errors import Failure
from aiogram_stream_sender.transport.classify import classify, is_not_modified


def _method() -> SendMessage:
    return SendMessage(chat_id=1, text="x")


def test_network_error_is_transient() -> None:
    failure, retry_after, _reason = classify(
        TelegramNetworkError(method=_method(), message="timeout")
    )
    assert failure is Failure.TRANSIENT
    assert retry_after is None


def test_server_error_is_transient() -> None:
    failure, _retry, _reason = classify(
        TelegramServerError(method=_method(), message="bad gateway")
    )
    assert failure is Failure.TRANSIENT


def test_stream_markers_win_over_bad_request() -> None:
    failure, _retry, _reason = classify(RuntimeError("topic_closed"))
    assert failure is Failure.STREAM_DEAD


def test_not_modified_requires_bad_request() -> None:
    assert not is_not_modified(RuntimeError("message is not modified"))
