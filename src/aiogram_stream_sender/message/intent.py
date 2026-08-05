from dataclasses import dataclass
from enum import Enum, auto

from aiogram_stream_sender.chunk import Chunk


class ActionKind(Enum):
    SEND = auto()
    EDIT = auto()
    DELETE = auto()
    ACTION = auto()


@dataclass(frozen=True, slots=True)
class SendIntent:
    chunk: Chunk


@dataclass(frozen=True, slots=True)
class EditIntent:
    message_id: int
    chunk: Chunk


@dataclass(frozen=True, slots=True)
class DeleteIntent:
    message_id: int


@dataclass(frozen=True, slots=True)
class ActionIntent:
    pass


Intent = SendIntent | EditIntent | DeleteIntent | ActionIntent


def kind_of(intent: Intent) -> ActionKind:
    if isinstance(intent, SendIntent):
        return ActionKind.SEND
    if isinstance(intent, EditIntent):
        return ActionKind.EDIT
    if isinstance(intent, DeleteIntent):
        return ActionKind.DELETE
    return ActionKind.ACTION
