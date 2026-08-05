import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Self


@dataclass(frozen=True, slots=True)
class Chunk:
    text: str
    entities: tuple[Mapping[str, Any], ...] = ()
    content_hash: str = field(default="", compare=False, repr=False)

    def __post_init__(self) -> None:
        payload = json.dumps(
            [self.text, [dict(entity) for entity in self.entities]],
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
        )
