import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Self


@dataclass(frozen=True, slots=True)
class Chunk:
    text: str
    entities: tuple[Mapping[str, Any], ...] = ()
    reply_markup: Mapping[str, Any] | None = None
    link_preview: Mapping[str, Any] | None = None
    reply_to: int | None = None
    parse_mode: str | None = None
    key: str | None = None
    content_hash: str = field(default="", compare=False, repr=False)

    def __post_init__(self) -> None:
        payload = json.dumps(
            [
                self.text,
                [dict(entity) for entity in self.entities],
                self.reply_markup,
                self.link_preview,
                self.reply_to,
                self.parse_mode,
            ],
            ensure_ascii=False,
            sort_keys=True,
        )
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        object.__setattr__(self, "content_hash", digest)

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> Self:
        entities: Sequence[Mapping[str, Any]] = data.get("entities", ())
        return cls(
            text=data["text"],
            entities=tuple(dict(entity) for entity in entities),
            reply_markup=data.get("reply_markup"),
            link_preview=data.get("link_preview"),
            reply_to=data.get("reply_to"),
            parse_mode=data.get("parse_mode"),
            key=data.get("key"),
        )
