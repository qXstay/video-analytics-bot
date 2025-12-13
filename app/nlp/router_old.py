import re
from dataclasses import dataclass

@dataclass
class Query:
    sql: str
    args: tuple = ()

def build_query(text: str) -> Query | None:
    t = (text or "").strip().lower()

    # 1) сколько всего видео
    if "сколько" in t and "видео" in t and ("всего" in t or "в системе" in t):
        return Query("SELECT COUNT(*)::bigint AS value FROM videos")

    # 2) сколько видео у автора <id>
    m = re.search(r"автор(?:а)?\s+([a-z0-9_\-]+)", t)
    if "сколько" in t and "видео" in t and m:
        creator_id = m.group(1)
        return Query(
            "SELECT COUNT(*)::bigint AS value FROM videos WHERE creator_id=$1",
            (creator_id,),
        )

    # 3) сколько видео с просмотрами >= N (больше/не меньше/от/>=)
    m = re.search(r"(?:просмотр\w*).*(?:от|>=|не меньше|больше|более)\s*(\d+)", t)
    if "сколько" in t and "видео" in t and m:
        n = int(m.group(1))
        return Query(
            "SELECT COUNT(*)::bigint AS value FROM videos WHERE views_count >= $1",
            (n,),
        )

    # 4) топ N авторов по количеству видео
    m = re.search(r"топ\s*(\d+)", t)
    if m and "автор" in t and not any(x in t for x in ["просмотр", "лайк", "коммент", "репорт", "жалоб"]):
        n = int(m.group(1))
        return Query(
            """
            SELECT string_agg(creator_id || ': ' || cnt::text, E'\n') AS value
            FROM (
                SELECT creator_id, COUNT(*) AS cnt
                FROM videos
                GROUP BY creator_id
                ORDER BY cnt DESC
                LIMIT $1
            ) t
            """,
            (n,),
        )

    # 5) сколько просмотров всего
    if "сколько" in t and "просмотр" in t and ("всего" in t or "в системе" in t):
        return Query("SELECT COALESCE(SUM(views_count),0)::bigint AS value FROM videos")

    # 6) сколько просмотров у автора <id>
    m = re.search(r"автор(?:а)?\s+([a-z0-9_\-]+)", t)
    if "сколько" in t and "просмотр" in t and m:
        creator_id = m.group(1)
        return Query(
            "SELECT COALESCE(SUM(views_count),0)::bigint AS value FROM videos WHERE creator_id=$1",
            (creator_id,),
        )

    # 7) топ N видео по просмотрам
    m = re.search(r"топ\s*(\d+)", t)
    if m and "видео" in t and "просмотр" in t:
        n = int(m.group(1))
        return Query(
            """
            SELECT string_agg(id::text || ': ' || views_count::text, E'\n') AS value
            FROM (
                SELECT id, views_count
                FROM videos
                ORDER BY views_count DESC
                LIMIT $1
            ) t
            """,
            (n,),
        )

    # топ N авторов по просмотрам (сумма views_count)
    m = re.search(r"топ\s*(\d+)", t)
    if m and "автор" in t and "просмотр" in t:
        n = int(m.group(1))
        return Query(
            """
            SELECT string_agg(creator_id || ': ' || total_views::text, E'\n') AS value
            FROM (
                SELECT creator_id, SUM(views_count)::bigint AS total_views
                FROM videos
                GROUP BY creator_id
                ORDER BY total_views DESC
                LIMIT $1
            ) t
            """,
            (n,),
        )

    # ===== ЛАЙКИ =====

    # 8) сколько лайков всего в системе
    if "сколько" in t and ("лайк" in t or "лайков" in t) and ("всего" in t or "в системе" in t):
        return Query("SELECT COALESCE(SUM(likes_count),0)::bigint AS value FROM videos")

        # 9) сколько лайков у автора <id>
    m = re.search(r"автор(?:а)?\s+([a-z0-9_\-]+)", t)
    if "сколько" in t and ("лайк" in t or "лайков" in t) and m:
        creator_id = m.group(1)
        return Query(
            "SELECT COALESCE(SUM(likes_count),0)::bigint AS value FROM videos WHERE creator_id=$1",
            (creator_id,),
        )

    # 10) топ N видео по лайкам
    m = re.search(r"топ\s*(\d+)", t)
    if m and "видео" in t and ("лайк" in t or "лайков" in t):
        n = int(m.group(1))
        return Query(
            """
            SELECT string_agg(id::text || ': ' || likes_count::text, E'\n') AS value
            FROM (
                SELECT id, likes_count
                FROM videos
                ORDER BY likes_count DESC
                LIMIT $1
            ) t
            """,
            (n,),
        )

    # 11) топ N авторов по лайкам (сумма likes_count)
    m = re.search(r"топ\s*(\d+)", t)
    if m and "автор" in t and ("лайк" in t or "лайков" in t):
        n = int(m.group(1))
        return Query(
            """
            SELECT string_agg(creator_id || ': ' || total_likes::text, E'\n') AS value
            FROM (
                SELECT creator_id, SUM(likes_count)::bigint AS total_likes
                FROM videos
                GROUP BY creator_id
                ORDER BY total_likes DESC
                LIMIT $1
            ) t
            """,
            (n,),
        )

    # ===== КОММЕНТАРИИ =====

    # сколько комментариев всего
    if "сколько" in t and "коммент" in t and ("всего" in t or "в системе" in t):
        return Query("SELECT COALESCE(SUM(comments_count),0)::bigint AS value FROM videos")

        # сколько комментариев у автора <id>
    m = re.search(r"автор(?:а)?\s+([a-z0-9_\-]+)", t)
    if "сколько" in t and "коммент" in t and m:
        creator_id = m.group(1)
        return Query(
            "SELECT COALESCE(SUM(comments_count),0)::bigint AS value FROM videos WHERE creator_id=$1",
            (creator_id,),
        )

    # топ N видео по комментариям
    m = re.search(r"топ\s*(\d+)", t)
    if m and "видео" in t and "коммент" in t:
        n = int(m.group(1))
        return Query(
            """
            SELECT string_agg(id::text || ': ' || comments_count::text, E'\n') AS value
            FROM (
                SELECT id, comments_count
                FROM videos
                ORDER BY comments_count DESC
                LIMIT $1
            ) t
            """,
            (n,),
        )

    # топ N авторов по комментариям
    m = re.search(r"топ\s*(\d+)", t)
    if m and "автор" in t and "коммент" in t:
        n = int(m.group(1))
        return Query(
            """
            SELECT string_agg(creator_id || ': ' || total_comments::text, E'\n') AS value
            FROM (
                SELECT creator_id, SUM(comments_count)::bigint AS total_comments
                FROM videos
                GROUP BY creator_id
                ORDER BY total_comments DESC
                LIMIT $1
            ) t
            """,
            (n,),
        )

    # ===== РЕПОРТЫ / ЖАЛОБЫ =====

    # сколько репортов всего
    if "сколько" in t and ("репорт" in t or "жалоб" in t) and ("всего" in t or "в системе" in t):
        return Query("SELECT COALESCE(SUM(reports_count),0)::bigint AS value FROM videos")

        # сколько репортов у автора <id>
    m = re.search(r"автор(?:а)?\s+([a-z0-9_\-]+)", t)
    if "сколько" in t and ("репорт" in t or "жалоб" in t) and m:
        creator_id = m.group(1)
        return Query(
            "SELECT COALESCE(SUM(reports_count),0)::bigint AS value FROM videos WHERE creator_id=$1",
            (creator_id,),
        )

    # топ N видео по репортам
    m = re.search(r"топ\s*(\d+)", t)
    if m and "видео" in t and ("репорт" in t or "жалоб" in t):
        n = int(m.group(1))
        return Query(
            """
            SELECT string_agg(id::text || ': ' || reports_count::text, E'\n') AS value
            FROM (
                SELECT id, reports_count
                FROM videos
                ORDER BY reports_count DESC
                LIMIT $1
            ) t
            """,
            (n,),
        )

    # топ N авторов по репортам
    m = re.search(r"топ\s*(\d+)", t)
    if m and "автор" in t and ("репорт" in t or "жалоб" in t):
        n = int(m.group(1))
        return Query(
            """
            SELECT string_agg(creator_id || ': ' || total_reports::text, E'\n') AS value
            FROM (
                SELECT creator_id, SUM(reports_count)::bigint AS total_reports
                FROM videos
                GROUP BY creator_id
                ORDER BY total_reports DESC
                LIMIT $1
            ) t
            """,
            (n,),
        )

    return None