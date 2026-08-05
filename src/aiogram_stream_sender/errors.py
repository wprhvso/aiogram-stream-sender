from enum import Enum, auto


class SenderError(Exception):
    pass


class StreamFinalizedError(SenderError):
    pass


class StreamFailedError(SenderError):
    def __init__(self, reason: str, message_ids: list[int]) -> None:
        super().__init__(reason)
        self.reason = reason
        self.message_ids = message_ids


class Failure(Enum):
    TRANSIENT = auto()
    MESSAGE_DEAD = auto()
    STREAM_DEAD = auto()
