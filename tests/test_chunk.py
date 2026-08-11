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
    entities = [{"type": "code"}]
    chunk = Chunk.from_mapping({"text": "a", "entities": entities})
    entities[0]["type"] = "bold"
    assert chunk.entities[0]["type"] == "code"


def test_markup_and_preview_change_the_hash() -> None:
    plain = Chunk(text="a")
    with_markup = Chunk(text="a", reply_markup={"inline_keyboard": []})
    with_preview = Chunk(text="a", link_preview={"is_disabled": True})
    with_reply = Chunk(text="a", reply_to=7)
    with_mode = Chunk(text="a", parse_mode="HTML")

    hashes = {
        chunk.content_hash
        for chunk in (plain, with_markup, with_preview, with_reply, with_mode)
    }

    assert len(hashes) == 5


def test_from_mapping_reads_every_field() -> None:
    chunk = Chunk.from_mapping(
        {
            "text": "a",
            "reply_markup": {"inline_keyboard": []},
            "link_preview": {"is_disabled": True},
            "reply_to": 7,
            "parse_mode": "HTML",
            "key": "3",
        }
    )

    assert chunk.reply_markup == {"inline_keyboard": []}
    assert chunk.link_preview == {"is_disabled": True}
    assert chunk.reply_to == 7
    assert chunk.parse_mode == "HTML"
    assert chunk.key == "3"
