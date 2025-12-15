import re
from dataclasses import dataclass
from datetime import date, time
from typing import Optional, Tuple

@dataclass
class Query:
    sql: str
    args: tuple = ()

MONTHS = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4, "мая": 5, "июня": 6,
    "июля": 7, "августа": 8, "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
}

MONTHS_LOC = {
    "январе": 1, "феврале": 2, "марте": 3, "апреле": 4, "мае": 5, "июне": 6,
    "июле": 7, "августе": 8, "сентябре": 9, "октябре": 10, "ноябре": 11, "декабре": 12,
}

def _to_int(num_text: str) -> Optional[int]:
    # Берём только цифры. Если цифр нет — None.
    digits = re.sub(r"\D+", "", num_text or "")
    return int(digits) if digits else None

def _parse_threshold(t: str) -> Optional[tuple[str, int]]:
    """
    Возвращает (op, n) где op: '>' или '>='.
    Ищет конструкции:
      больше|более|не меньше|>=|от  + число (с пробелами)
    """
    m = re.search(r"(больше|более|не меньше|>=|от)\s*([\d][\d\s]*)", t)
    if not m:
        return None

    word = m.group(1)
    n = _to_int(m.group(2))
    if n is None:
        return None

    op = ">=" if word in ("не меньше", ">=", "от") else ">"
    return op, n

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

def _parse_ru_month_year(text: str) -> Optional[tuple[int, int]]:
    t = text.lower()
    m = re.search(
        r"\bв\s+(январе|феврале|марте|апреле|мае|июне|июле|августе|сентябре|октябре|ноябре|декабре)\s+(\d{4})\b",
        t
    )
    if not m:
        return None
    mon = MONTHS_LOC[m.group(1)]
    year = int(m.group(2))
    return year, mon

def _parse_ru_time_range(text: str) -> Optional[Tuple[time, time]]:
    """
    Поддержка:
      - "с 10:00 до 15:00"
      - "между 10:00 и 15:00"
      - "10:00-15:00"
      - "с 10 до 15"
    Возвращает (t_from, t_to).
    """
    t = (text or "").lower()

    patterns = [
        r"(?:с)\s*(\d{1,2})(?::(\d{2}))?\s*(?:до|по|-)\s*(\d{1,2})(?::(\d{2}))?",
        r"(?:между)\s*(\d{1,2})(?::(\d{2}))?\s*(?:и|-)\s*(\d{1,2})(?::(\d{2}))?",
        r"\b(\d{1,2})(?::(\d{2}))?\s*-\s*(\d{1,2})(?::(\d{2}))?\b",
    ]

    for p in patterns:
        m = re.search(p, t)
        if not m:
            continue

        h1 = int(m.group(1))
        m1 = int(m.group(2) or 0)
        h2 = int(m.group(3))
        m2 = int(m.group(4) or 0)

        if not (0 <= h1 <= 23 and 0 <= m1 <= 59 and 0 <= h2 <= 23 and 0 <= m2 <= 59):
            return None

        return time(h1, m1), time(h2, m2)

    return None

def build_query(text: str) -> Query | None:
    t = (text or "").strip().lower()

    # заранее парсим то, что часто нужно
    m_id = re.search(r"(?:автор(?:а)?|креатор(?:а)?)(?:\s+с)?(?:\s+id)?\s+([a-z0-9_\-]+)", t)
    thr = _parse_threshold(t)
    dr = _parse_ru_date_range(t)
    my = _parse_ru_month_year(t)
    day = _parse_ru_date(t)
    tr = _parse_ru_time_range(t)

    def _metric_value_column() -> Optional[str]:
        if "просмотр" in t:
            return "views_count"
        if "лайк" in t:
            return "likes_count"
        if "коммент" in t:
            return "comments_count"
        if "репорт" in t or "жалоб" in t:
            return "reports_count"
        return None

    def _metric_delta_column() -> Optional[str]:
        if "просмотр" in t:
            return "delta_views_count"
        if "лайк" in t:
            return "delta_likes_count"
        if "коммент" in t:
            return "delta_comments_count"
        if "репорт" in t or "жалоб" in t:
            return "delta_reports_count"
        return None

    # ===== НЕГАТИВНЫЙ ПРИРОСТ (замеры/снапшоты) =====
    if (
        "сколько" in t
        and ("замер" in t or "снапшот" in t or "сним" in t or "почас" in t or "статистик" in t)
        and ("отриц" in t or "меньше" in t or "уменьш" in t)
        and "просмотр" in t
    ):
        return Query("SELECT COUNT(*)::bigint AS value FROM video_snapshots WHERE delta_views_count < 0")

    # ===== СКОЛЬКО ВСЕГО ВИДЕО =====
    if "сколько" in t and "видео" in t and ("всего" in t or "в системе" in t):
        return Query("SELECT COUNT(*)::bigint AS value FROM videos")

    # ===== ВИДЕО У КРЕАТОРА В ДИАПАЗОНЕ ДАТ (published) =====
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

    # ===== ПОРОГИ ДЛЯ КРЕАТОРА (views/likes/comments/reports) =====
    if "сколько" in t and "видео" in t and m_id and thr:
        creator_id = m_id.group(1)
        op, n = thr
        col = _metric_value_column()
        if col:
            return Query(
                f"""
                SELECT COUNT(*)::bigint AS value
                FROM videos
                WHERE creator_id = $1
                  AND {col} {op} $2
                """,
                (creator_id, n),
            )

    # ===== ПОРОГИ (без креатора) =====
    if "сколько" in t and "видео" in t and thr and not m_id:
        op, n = thr
        col = _metric_value_column()
        if col:
            return Query(
                f"SELECT COUNT(*)::bigint AS value FROM videos WHERE {col} {op} $1",
                (n,),
            )

    # ===== СУММА ПРОСМОТРОВ ПО МЕСЯЦУ =====
    if my and "видео" in t and "просмотр" in t:
        y, m = my
        start = date(y, m, 1)
        end = date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1)
        return Query(
            """
            SELECT COALESCE(SUM(views_count),0)::bigint AS value
            FROM videos
            WHERE video_created_at::date >= $1
              AND video_created_at::date <  $2
            """,
            (start, end),
        )

    # ===== СУММЫ ПО ВСЕЙ БАЗЕ =====
    if "сколько" in t and "просмотр" in t and ("всего" in t or "в системе" in t):
        return Query("SELECT COALESCE(SUM(views_count),0)::bigint AS value FROM videos")
    if "сколько" in t and ("лайк" in t or "лайков" in t) and ("всего" in t or "в системе" in t):
        return Query("SELECT COALESCE(SUM(likes_count),0)::bigint AS value FROM videos")
    if "сколько" in t and "коммент" in t and ("всего" in t or "в системе" in t):
        return Query("SELECT COALESCE(SUM(comments_count),0)::bigint AS value FROM videos")
    if "сколько" in t and ("репорт" in t or "жалоб" in t) and ("всего" in t or "в системе" in t):
        return Query("SELECT COALESCE(SUM(reports_count),0)::bigint AS value FROM videos")

    # ===== РОСТ ПО SNAPSHOTS: ДАТА + ВРЕМЕННОЙ ИНТЕРВАЛ (ключевой тест) =====
    # Важно: считаем сумму delta_* по тем замерам, которые попали в интервал.
    if day and tr and ("на сколько" in t or "насколько" in t) and ("вырос" in t or "увелич" in t):
        col = _metric_delta_column()
        if col:
            t_from, t_to = tr
            # интервал делаем полуоткрытым: [from, to)
            if m_id:
                creator_id = m_id.group(1)
                return Query(
                    f"""
                    SELECT COALESCE(SUM(s.{col}),0)::bigint AS value
                    FROM video_snapshots s
                    JOIN videos v ON v.id = s.video_id
                    WHERE v.creator_id = $1
                      AND s.created_at::date = $2
                      AND s.created_at::time >= $3
                      AND s.created_at::time <  $4
                    """,
                    (creator_id, day, t_from, t_to),
                )
            return Query(
                f"""
                SELECT COALESCE(SUM({col}),0)::bigint AS value
                FROM video_snapshots
                WHERE created_at::date = $1
                  AND created_at::time >= $2
                  AND created_at::time <  $3
                """,
                (day, t_from, t_to),
            )

    # ===== ДНЕВНЫЕ МЕТРИКИ ПО SNAPSHOTS (без интервала времени) =====
    if day:
        if ("на сколько" in t or "насколько" in t) and ("в сумме" in t or "суммар" in t) and ("вырос" in t or "увелич" in t):
            col = _metric_delta_column()
            if col:
                if m_id:
                    creator_id = m_id.group(1)
                    return Query(
                        f"""
                        SELECT COALESCE(SUM(s.{col}),0)::bigint AS value
                        FROM video_snapshots s
                        JOIN videos v ON v.id = s.video_id
                        WHERE v.creator_id = $1
                          AND s.created_at::date = $2
                        """,
                        (creator_id, day),
                    )
                return Query(
                    f"SELECT COALESCE(SUM({col}),0)::bigint AS value FROM video_snapshots WHERE created_at::date = $1",
                    (day,),
                )

        if "сколько" in t and "разных" in t and "видео" in t and ("получал" in t or "получали" in t or "получало" in t) and "нов" in t:
            col = _metric_delta_column()
            if col:
                if m_id:
                    creator_id = m_id.group(1)
                    return Query(
                        f"""
                        SELECT COUNT(DISTINCT s.video_id)::bigint AS value
                        FROM video_snapshots s
                        JOIN videos v ON v.id = s.video_id
                        WHERE v.creator_id = $1
                          AND s.created_at::date = $2
                          AND s.{col} > 0
                        """,
                        (creator_id, day),
                    )
                return Query(
                    f"""
                    SELECT COUNT(DISTINCT video_id)::bigint AS value
                    FROM video_snapshots
                    WHERE created_at::date = $1 AND {col} > 0
                    """,
                    (day,),
                )

    return None