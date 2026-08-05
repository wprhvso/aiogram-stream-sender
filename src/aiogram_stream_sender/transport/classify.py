from aiogram.exceptions import (
    TelegramAPIError,
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramNetworkError,
    TelegramRetryAfter,
)

from aiogram_stream_sender.errors import Failure

NOT_MODIFIED = "message is not modified"

_MESSAGE_DEAD = (
    "message to edit not found",
    "message can't be edited",
    "message to delete not found",
    "message_id_invalid",
    "message identifier is not specified",
    "message is too long",
    "can't parse entities",
    "text must be non-empty",
)

_STREAM_DEAD = (
    "bot was blocked",
    "bot was kicked",
    "chat not found",
    "user is deactivated",
    "not enough rights",
    "have no rights to send",
    "peer_id_invalid",
    "topic_closed",
    "message thread not found",
)


def classify(error: Exception) -> tuple[Failure, float | None, str]:
    text = str(error)
    lowered = text.lower()

    if isinstance(error, TelegramRetryAfter):
        return Failure.TRANSIENT, float(error.retry_after), text

    if isinstance(error, TelegramForbiddenError):
        return Failure.STREAM_DEAD, None, text

    if any(marker in lowered for marker in _STREAM_DEAD):
        return Failure.STREAM_DEAD, None, text

    if isinstance(error, TelegramBadRequest) and any(
        marker in lowered for marker in _MESSAGE_DEAD
    ):
        return Failure.MESSAGE_DEAD, None, text

    if isinstance(error, (TelegramNetworkError, TelegramAPIError)):
        return Failure.TRANSIENT, None, text

    return Failure.TRANSIENT, None, text


def is_not_modified(error: Exception) -> bool:
    return isinstance(error, TelegramBadRequest) and NOT_MODIFIED in str(error).lower()
