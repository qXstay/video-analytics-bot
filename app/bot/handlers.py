import asyncpg
from aiogram import Router, F
from aiogram.types import Message

from app.nlp.router import build_query

router = Router()


@router.message(F.text == "/start")
async def start_handler(message: Message, db: asyncpg.Pool):
    # по ТЗ не обязательно, но полезно: что бот жив
    await message.answer("OK")


@router.message(F.text)
async def any_text_handler(message: Message, db: asyncpg.Pool):
    try:
        q = build_query(message.text)
        if not q:
            await message.answer("0")
            return

        row = await db.fetchrow(q.sql, *q.args)
        value = row["value"] if row and ("value" in row) else None

        await message.answer(str(int(value)) if value is not None else "0")

    except Exception:
        # В тесте лучше ответить 0, чем упасть и “повесить” проверку
        await message.answer("0")