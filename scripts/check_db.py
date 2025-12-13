import asyncpg
import asyncio

async def main():
    conn = await asyncpg.connect(
        user="app",
        password="app",
        database="video_analytics",
        host="127.0.0.1",
        port=55432,
    )

    rows = await conn.fetch("SELECT COUNT(*) FROM videos;")
    print(rows)

    await conn.close()

asyncio.run(main())