import asyncio
import asyncpg
import ijson
from pathlib import Path

DB = dict(
    user="app",
    password="app",
    database="video_analytics",
    host="127.0.0.1",
    port=55432,
)

VIDEO_SQL = """
INSERT INTO videos (
    id, creator_id, video_created_at,
    views_count, likes_count, comments_count, reports_count,
    created_at, updated_at
)
VALUES (
    $1,$2,$3,
    $4,$5,$6,$7,
    $8,$9
)
ON CONFLICT (id) DO UPDATE SET
    creator_id=EXCLUDED.creator_id,
    video_created_at=EXCLUDED.video_created_at,
    views_count=EXCLUDED.views_count,
    likes_count=EXCLUDED.likes_count,
    comments_count=EXCLUDED.comments_count,
    reports_count=EXCLUDED.reports_count,
    created_at=EXCLUDED.created_at,
    updated_at=EXCLUDED.updated_at
"""

SNAPSHOT_SQL = """
INSERT INTO video_snapshots (
    id, video_id,
    views_count, likes_count, comments_count, reports_count,
    delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count,
    created_at, updated_at
)
VALUES (
    $1,$2,
    $3,$4,$5,$6,
    $7,$8,$9,$10,
    $11,$12
)
ON CONFLICT (id) DO UPDATE SET
    video_id=EXCLUDED.video_id,
    views_count=EXCLUDED.views_count,
    likes_count=EXCLUDED.likes_count,
    comments_count=EXCLUDED.comments_count,
    reports_count=EXCLUDED.reports_count,
    delta_views_count=EXCLUDED.delta_views_count,
    delta_likes_count=EXCLUDED.delta_likes_count,
    delta_comments_count=EXCLUDED.delta_comments_count,
    delta_reports_count=EXCLUDED.delta_reports_count,
    created_at=EXCLUDED.created_at,
    updated_at=EXCLUDED.updated_at
"""

def to_int(x):
    return 0 if x is None else int(x)

async def main(json_path: str):
    path = Path(json_path)
    if not path.exists():
        raise FileNotFoundError(path)

    conn = await asyncpg.connect(**DB)

    video_batch = []
    snap_batch = []

    videos_inserted = 0
    snaps_inserted = 0

    # ijson: парсим items массива videos: data["videos"][*]
    with path.open("rb") as f:
        for video in ijson.items(f, "videos.item"):
            video_id = video["id"]

            video_batch.append((
                video_id,
                video.get("creator_id"),
                video.get("video_created_at"),
                to_int(video.get("views_count")),
                to_int(video.get("likes_count")),
                to_int(video.get("comments_count")),
                to_int(video.get("reports_count")),
                video.get("created_at"),
                video.get("updated_at"),
            ))

            for s in video.get("snapshots", []):
                snap_batch.append((
                    s["id"],
                    s.get("video_id") or video_id,
                    to_int(s.get("views_count")),
                    to_int(s.get("likes_count")),
                    to_int(s.get("comments_count")),
                    to_int(s.get("reports_count")),
                    to_int(s.get("delta_views_count")),
                    to_int(s.get("delta_likes_count")),
                    to_int(s.get("delta_comments_count")),
                    to_int(s.get("delta_reports_count")),
                    s.get("created_at"),
                    s.get("updated_at"),
                ))

            # батчи — чтобы было быстро, но без перегруза
            if len(video_batch) >= 500:
                await conn.executemany(VIDEO_SQL, video_batch)
                videos_inserted += len(video_batch)
                video_batch.clear()

            if len(snap_batch) >= 2000:
                await conn.executemany(SNAPSHOT_SQL, snap_batch)
                snaps_inserted += len(snap_batch)
                snap_batch.clear()

    # добиваем хвосты
    if video_batch:
        await conn.executemany(VIDEO_SQL, video_batch)
        videos_inserted += len(video_batch)

    if snap_batch:
        await conn.executemany(SNAPSHOT_SQL, snap_batch)
        snaps_inserted += len(snap_batch)

    # контрольные числа
    v_cnt = await conn.fetchval("SELECT COUNT(*) FROM videos;")
    s_cnt = await conn.fetchval("SELECT COUNT(*) FROM video_snapshots;")

    await conn.close()

    print(f"Inserted (approx): videos={videos_inserted}, snapshots={snaps_inserted}")
    print(f"DB counts: videos={v_cnt}, snapshots={s_cnt}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python scripts/import_json.py path/to/data.json")
        raise SystemExit(2)
    asyncio.run(main(sys.argv[1]))