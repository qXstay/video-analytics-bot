import os
import asyncio
from datetime import datetime
import asyncpg
import ijson

DB_DSN = os.getenv("DB_DSN")  # postgresql://app:app@127.0.0.1:55432/video_analytics
JSON_PATH = os.getenv("JSON_PATH", "data/videos.json")

BATCH_VIDEOS = 1000
BATCH_SNAPSHOTS = 5000


def parse_dt(s: str | None):
    if not s:
        return None
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


async def flush_videos(conn: asyncpg.Connection, rows: list[tuple]) -> int:
    if not rows:
        return 0
    await conn.executemany(
        """
        INSERT INTO videos(
            id, creator_id, video_created_at,
            views_count, likes_count, comments_count, reports_count,
            created_at, updated_at
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        ON CONFLICT (id) DO NOTHING
        """,
        rows,
    )
    return len(rows)


async def flush_snapshots(conn: asyncpg.Connection, rows: list[tuple]) -> int:
    if not rows:
        return 0
    await conn.executemany(
        """
        INSERT INTO video_snapshots(
            id, video_id,
            views_count, likes_count, comments_count, reports_count,
            delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count,
            created_at, updated_at
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
        ON CONFLICT (id) DO NOTHING
        """,
        rows,
    )
    return len(rows)


async def main():
    if not DB_DSN:
        raise RuntimeError("DB_DSN is not set. Example: postgresql://app:app@127.0.0.1:55432/video_analytics")

    conn = await asyncpg.connect(DB_DSN)

    videos_rows: list[tuple] = []
    snapshots_rows: list[tuple] = []
    v_count = 0
    s_count = 0

    with open(JSON_PATH, "rb") as f:
        for video in ijson.items(f, "videos.item"):
            videos_rows.append((
                video["id"],
                video["creator_id"],
                parse_dt(video["video_created_at"]),
                int(video["views_count"]),
                int(video["likes_count"]),
                int(video["comments_count"]),
                int(video["reports_count"]),
                parse_dt(video["created_at"]),
                parse_dt(video["updated_at"]),
            ))

            for snap in video.get("snapshots", []):
                snapshots_rows.append((
                    snap["id"],
                    snap["video_id"],
                    int(snap["views_count"]),
                    int(snap["likes_count"]),
                    int(snap["comments_count"]),
                    int(snap["reports_count"]),
                    int(snap["delta_views_count"]),
                    int(snap["delta_likes_count"]),
                    int(snap["delta_comments_count"]),
                    int(snap["delta_reports_count"]),
                    parse_dt(snap["created_at"]),
                    parse_dt(snap["updated_at"]),
                ))

            # Ключевой момент: когда набрался батч видео — сначала пишем videos, потом snapshots
            if len(videos_rows) >= BATCH_VIDEOS:
                v_count += await flush_videos(conn, videos_rows)
                videos_rows.clear()

                # после записи видео безопасно писать снапшоты
                while len(snapshots_rows) >= BATCH_SNAPSHOTS:
                    chunk = snapshots_rows[:BATCH_SNAPSHOTS]
                    s_count += await flush_snapshots(conn, chunk)
                    del snapshots_rows[:BATCH_SNAPSHOTS]

                print(f"Inserted videos: {v_count}, snapshots: {s_count}...")

    # финальный flush: сначала videos, затем все snapshots
    v_count += await flush_videos(conn, videos_rows)
    videos_rows.clear()

    s_count += await flush_snapshots(conn, snapshots_rows)
    snapshots_rows.clear()

    await conn.close()
    print(f"DONE. Inserted videos: {v_count}, snapshots: {s_count}")


if __name__ == "__main__":
    asyncio.run(main())