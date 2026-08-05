from aiogram_stream_sender.chunk import Chunk


def test_hash_is_stable_and_content_sensitive() -> None:
    first = Chunk(text="hi", entities=({"type": "bold", "offset": 0, "length": 2},))
    second = Chunk(text="hi", entities=({"length": 2, "offset": 0, "type": "bold"},))
    third = Chunk(text="hi!", entities=())
    assert first.content_hash == second.content_hash
    assert first.content_hash != third.content_hash


def test_hash_excluded_from_equality() -> None:
    assert Chunk(text="a") == Chunk(text="a")


def test_from_mapping_copies_entities() -> None:
    source = {"text": "a", "entities": [{"type": "code"}]}
    chunk = Chunk.from_mapping(source)
    source["entities"][0]["type"] = "bold"
    assert chunk.entities[0]["type"] == "code"
