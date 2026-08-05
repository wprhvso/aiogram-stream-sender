import asyncio
import os

from aiogram import Bot, Dispatcher
from aiogram.types import Message

from aiogram_stream_sender import ScopedSender, SenderMiddleware, SenderRuntime

dp = Dispatcher()


@dp.message()
async def echo(message: Message, sender: ScopedSender) -> None:
    words = (message.text or "").split()
    if not words:
        return
    async with sender.stream() as stream:
        for count in range(1, len(words) + 1):
            stream.update([{"text": " ".join(words[:count])}])
            await asyncio.sleep(0.25)


async def main() -> None:
    runtime = SenderRuntime()
    dp.message.middleware(SenderMiddleware(runtime))
    bot = Bot(os.environ["BOT_TOKEN"])
    try:
        await dp.start_polling(bot)
    finally:
        await runtime.aclose()


if __name__ == "__main__":
    asyncio.run(main())
