from typing import Any, Awaitable, Callable, Dict

import asyncpg
from aiogram import BaseMiddleware
from aiogram.types import TelegramObject


class DbMiddleware(BaseMiddleware):
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        data["db"] = self.pool  # это попадёт параметром db в хэндлер
        return await handler(event, data)