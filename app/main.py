import os
import asyncio
import logging

import asyncpg
from aiogram import Bot, Dispatcher
from dotenv import load_dotenv

from app.db.middleware import DbMiddleware
from app.bot.handlers import router

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_DSN = os.getenv("DB_DSN")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set in .env")
if not DB_DSN:
    raise RuntimeError("DB_DSN is not set in .env")


async def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting bot...")

    pool = await asyncpg.create_pool(DB_DSN, min_size=1, max_size=5)

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()

    dp.update.middleware(DbMiddleware(pool))
    dp.include_router(router)

    try:
        await dp.start_polling(bot)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())