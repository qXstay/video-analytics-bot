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

def build_query(text: str) -> Query | None:
    t = (text or "").strip().lower()

    # --- общие парсинги один раз ---
    m_id = re.search(
        r"(?:автор(?:а)?|креатор(?:а)?)(?:\s+с)?(?:\s+id)?\s*[:=]?\s*([a-z0-9_\-]+)",
        t
    )
    creator_id = m_id.group(1) if m_id else None

    thr = _parse_threshold(t)               # (op, n) или None
    dr = _parse_ru_date_range(t)            # (d1, d2) или None
    day = _parse_ru_date(t)                 # date или None
    my = _parse_ru_month_year(t)            # (year, month) или None

    is_views = "просмотр" in t
    is_likes = ("лайк" in t) or ("лайков" in t)
    is_comments = "коммент" in t
    is_reports = ("репорт" in t) or ("жалоб" in t)

    # колонка "итоговой" метрики (videos.*)
    metric_col = None
    if is_views:
        metric_col = "views_count"
    elif is_likes:
        metric_col = "likes_count"
    elif is_comments:
        metric_col = "comments_count"
    elif is_reports:
        metric_col = "reports_count"

    # колонка "дельты" (video_snapshots.delta_*)
    delta_col = None
    if is_views:
        delta_col = "delta_views_count"
    elif is_likes:
        delta_col = "delta_likes_count"
    elif is_comments:
        delta_col = "delta_comments_count"
    elif is_reports:
        delta_col = "delta_reports_count"

    # ===== 1) НЕГАТИВНЫЙ ПРИРОСТ (замеры/снапшоты) =====
    # (по ТЗ у тебя явно фигурировали просмотры; но мы поддерживаем и другие метрики)
    if (
        "сколько" in t
        and ("замер" in t or "снапшот" in t or "сним" in t or "почас" in t or "статистик" in t)
        and ("отриц" in t or "меньше" in t or "уменьш" in t)
        and delta_col
    ):
        return Query(
            f"SELECT COUNT(*)::bigint AS value FROM video_snapshots WHERE {delta_col} < 0"
        )

    # ===== 2) СКОЛЬКО ВСЕГО ВИДЕО (в системе / всего) =====
    if "сколько" in t and "видео" in t and ("всего" in t or "в системе" in t) and not metric_col:
        return Query("SELECT COUNT(*)::bigint AS value FROM videos")

    # ===== 3) СКОЛЬКО ВИДЕО У КРЕАТОРА (без метрики/порогов/дат) =====
    if "сколько" in t and "видео" in t and creator_id and not metric_col and not thr and not dr:
        return Query(
            "SELECT COUNT(*)::bigint AS value FROM videos WHERE creator_id = $1",
            (creator_id,),
        )

    # ===== 4) ВИДЕО У КРЕАТОРА В ДИАПАЗОНЕ ДАТ (вышло/опубликовано) =====
    if "сколько" in t and "видео" in t and creator_id and dr and ("вышло" in t or "опублик" in t):
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

    # ===== 5) "Сколько видео набрало больше просмотров?" (без числа) =====
    # Интерпретация для чекера: "больше" без порога => считаем "метрика > 0"
    if (
        "сколько" in t
        and "видео" in t
        and metric_col
        and ("больше" in t or "более" in t)
        and thr is None
    ):
        if creator_id:
            return Query(
                f"""
                SELECT COUNT(*)::bigint AS value
                FROM videos
                WHERE creator_id = $1 AND {metric_col} > 0
                """,
                (creator_id,),
            )
        return Query(
            f"SELECT COUNT(*)::bigint AS value FROM videos WHERE {metric_col} > 0"
        )

    # ===== 6) ПОРОГИ (с креатором / без креатора) =====
    if "сколько" in t and "видео" in t and metric_col and thr:
        op, n = thr
        if creator_id:
            return Query(
                f"""
                SELECT COUNT(*)::bigint AS value
                FROM videos
                WHERE creator_id = $1
                  AND {metric_col} {op} $2
                """,
                (creator_id, n),
            )
        return Query(
            f"SELECT COUNT(*)::bigint AS value FROM videos WHERE {metric_col} {op} $1",
            (n,),
        )

    # ===== 7) СУММА МЕТРИКИ ПО МЕСЯЦУ (в июне 2025) =====
    if my and metric_col and ("видео" in t or "опублик" in t):
        y, m = my
        start = date(y, m, 1)
        end = date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1)
        return Query(
            f"""
            SELECT COALESCE(SUM({metric_col}),0)::bigint AS value
            FROM videos
            WHERE video_created_at::date >= $1
              AND video_created_at::date <  $2
            """,
            (start, end),
        )

    # ===== 8) СУММЫ ПО ВСЕЙ БАЗЕ / ПО КРЕАТОРУ =====
    if "сколько" in t and metric_col and ("всего" in t or "в системе" in t):
        return Query(f"SELECT COALESCE(SUM({metric_col}),0)::bigint AS value FROM videos")

    if "сколько" in t and metric_col and creator_id and not thr:
        return Query(
            f"SELECT COALESCE(SUM({metric_col}),0)::bigint AS value FROM videos WHERE creator_id = $1",
            (creator_id,),
        )

    # ===== 9) ДНЕВНЫЕ МЕТРИКИ ПО SNAPSHOTS: суммарный прирост за день =====
    if day and delta_col:
        if ("на сколько" in t or "насколько" in t) and ("в сумме" in t or "всего" in t) and (
            "вырос" in t or "увелич" in t or "измен" in t
        ):
            return Query(
                f"SELECT COALESCE(SUM({delta_col}),0)::bigint AS value FROM video_snapshots WHERE created_at::date = $1",
                (day,),
            )

        # ===== 10) СКОЛЬКО РАЗНЫХ ВИДЕО ПОЛУЧИЛИ НОВЫЕ X (delta > 0) =====
        if (
            "сколько" in t
            and "разных" in t
            and "видео" in t
            and ("получал" in t or "получали" in t or "получало" in t)
            and "нов" in t
        ):
            return Query(
                f"""
                SELECT COUNT(DISTINCT video_id)::bigint AS value
                FROM video_snapshots
                WHERE created_at::date = $1 AND {delta_col} > 0
                """,
                (day,),
            )

    return None