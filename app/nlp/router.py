import re
from dataclasses import dataclass
from datetime import date
from typing import Optional, Tuple

@dataclass
class Query:
    sql: str
    args: tuple = ()

MONTHS = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4, "мая": 5, "июня": 6,
    "июля": 7, "августа": 8, "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
}

def _to_int(num_text: str) -> int:
    # "100 000" -> 100000
    return int(re.sub(r"\s+", "", num_text))

def _parse_ru_date(s: str) -> Optional[date]:
    """
    Ищет дату формата "28 ноября 2025" (год обязателен).
    """
    m = re.search(r"\b(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\s+(\d{4})\b", s)
    if not m:
        return None
    d = int(m.group(1))
    mon = MONTHS[m.group(2)]
    y = int(m.group(3))
    return date(y, mon, d)

def _parse_ru_date_range(text: str) -> Optional[Tuple[date, date]]:
    """
    Поддержка:
    - "с 1 ноября 2025 по 5 ноября 2025"
    - "с 1 по 5 ноября 2025"
    Возвращает (start_date, end_date).
    """
    t = text.lower()

    # вариант 1: "с 1 ноября 2025 по 5 ноября 2025"
    m = re.search(
        r"с\s+(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\s+(\d{4})\s+по\s+(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\s+(\d{4})",
        t
    )
    if m:
        d1, m1, y1 = int(m.group(1)), MONTHS[m.group(2)], int(m.group(3))
        d2, m2, y2 = int(m.group(4)), MONTHS[m.group(5)], int(m.group(6))
        return date(y1, m1, d1), date(y2, m2, d2)

    # вариант 2: "с 1 по 5 ноября 2025"
    m = re.search(
        r"с\s+(\d{1,2})\s+по\s+(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\s+(\d{4})",
        t
    )
    if m:
        d1, d2 = int(m.group(1)), int(m.group(2))
        mon = MONTHS[m.group(3)]
        y = int(m.group(4))
        return date(y, mon, d1), date(y, mon, d2)

    return None

def build_query(text: str) -> Query | None:
    t = (text or "").strip().lower()

    # ===== БАЗА =====

    # Сколько всего видео
    if "сколько" in t and "видео" in t and ("всего" in t or "в системе" in t):
        return Query("SELECT COUNT(*)::bigint AS value FROM videos")

    # ===== ВИДЕО У АВТОРА С ДАТАМИ ПУБЛИКАЦИИ =====
    # "Сколько видео у креатора/автора <id> вышло с ... по ... включительно?"
    m_id = re.search(
        r"(?:автор(?:а)?|креатор(?:а)?)(?:\s+с)?(?:\s+id)?\s+([a-z0-9_\-]+)",
        t
    )
    dr = _parse_ru_date_range(t)
    if "сколько" in t and "видео" in t and m_id and dr and ("вышло" in t or "опублик" in t):
        creator_id = m_id.group(1)
        d1, d2 = dr
        return Query(
            """
            SELECT COUNT(*)::bigint AS value
            FROM videos
            WHERE creator_id = $1
              AND video_created_at::date >= $2
              AND video_created_at::date <= $3
            """,
            (creator_id, d1, d2),
        )

    # ===== ПОРОГИ (просмотры/лайки/комменты/репорты) ПО videos.*_count =====
    # "Сколько видео набрало больше 100 000 просмотров за всё время?"
    m_thr = re.search(r"(?:больше|более|>=|не меньше|от)\s*([\d\s]+)", t)
    if "сколько" in t and "видео" in t and m_thr:
        n = _to_int(m_thr.group(1))
        if "просмотр" in t:
            return Query("SELECT COUNT(*)::bigint AS value FROM videos WHERE views_count >= $1", (n,))
        if "лайк" in t:
            return Query("SELECT COUNT(*)::bigint AS value FROM videos WHERE likes_count >= $1", (n,))
        if "коммент" in t:
            return Query("SELECT COUNT(*)::bigint AS value FROM videos WHERE comments_count >= $1", (n,))
        if "репорт" in t or "жалоб" in t:
            return Query("SELECT COUNT(*)::bigint AS value FROM videos WHERE reports_count >= $1", (n,))

    # ===== СУММЫ И ТОПЫ (итоговые) =====

    if "сколько" in t and "просмотр" in t and ("всего" in t or "в системе" in t):
        return Query("SELECT COALESCE(SUM(views_count),0)::bigint AS value FROM videos")

    if "сколько" in t and ("лайк" in t or "лайков" in t) and ("всего" in t or "в системе" in t):
        return Query("SELECT COALESCE(SUM(likes_count),0)::bigint AS value FROM videos")

    if "сколько" in t and "коммент" in t and ("всего" in t or "в системе" in t):
        return Query("SELECT COALESCE(SUM(comments_count),0)::bigint AS value FROM videos")

    if "сколько" in t and ("репорт" in t or "жалоб" in t) and ("всего" in t or "в системе" in t):
        return Query("SELECT COALESCE(SUM(reports_count),0)::bigint AS value FROM videos")

    # ===== ДНЕВНЫЕ МЕТРИКИ ПО SNAPSHOTS (прирост / сколько разных видео получали прирост) =====

    day = _parse_ru_date(t)  # "28 ноября 2025"
    if day:
        # "На сколько <метрика> в сумме выросли все видео 28 ноября 2025?"
        if ("на сколько" in t or "насколько" in t) and "в сумме" in t and "вырос" in t and "видео" in t:
            if "просмотр" in t:
                return Query(
                    "SELECT COALESCE(SUM(delta_views_count),0)::bigint AS value FROM video_snapshots WHERE created_at::date = $1",
                    (day,),
                )
            if "лайк" in t:
                return Query(
                    "SELECT COALESCE(SUM(delta_likes_count),0)::bigint AS value FROM video_snapshots WHERE created_at::date = $1",
                    (day,),
                )
            if "коммент" in t:
                return Query(
                    "SELECT COALESCE(SUM(delta_comments_count),0)::bigint AS value FROM video_snapshots WHERE created_at::date = $1",
                    (day,),
                )
            if "репорт" in t or "жалоб" in t:
                return Query(
                    "SELECT COALESCE(SUM(delta_reports_count),0)::bigint AS value FROM video_snapshots WHERE created_at::date = $1",
                    (day,),
                )

        # "Сколько разных видео получали новые <метрика> 27 ноября 2025?"
        if "сколько" in t and "разных" in t and "видео" in t and ("получал" in t or "получали" in t or "получало" in t) and ("нов" in t):
            if "просмотр" in t:
                return Query(
                    """
                    SELECT COUNT(DISTINCT video_id)::bigint AS value
                    FROM video_snapshots
                    WHERE created_at::date = $1 AND delta_views_count > 0
                    """,
                    (day,),
                )
            if "лайк" in t:
                return Query(
                    """
                    SELECT COUNT(DISTINCT video_id)::bigint AS value
                    FROM video_snapshots
                    WHERE created_at::date = $1 AND delta_likes_count > 0
                    """,
                    (day,),
                )
            if "коммент" in t:
                return Query(
                    """
                    SELECT COUNT(DISTINCT video_id)::bigint AS value
                    FROM video_snapshots
                    WHERE created_at::date = $1 AND delta_comments_count > 0
                    """,
                    (day,),
                )
            if "репорт" in t or "жалоб" in t:
                return Query(
                    """
                    SELECT COUNT(DISTINCT video_id)::bigint AS value
                    FROM video_snapshots
                    WHERE created_at::date = $1 AND delta_reports_count > 0
                    """,
                    (day,),
                )

    return None
