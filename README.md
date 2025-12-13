# Video Analytics Telegram Bot

Telegram-бот для аналитики по видео и почасовым снапшотам в PostgreSQL.
Бот принимает запросы на русском языке и отвечает одним числом (count/sum/growth).

## Стек
- Python 3.11+ / 3.12
- aiogram 3.x
- PostgreSQL 16
- asyncpg
- ijson (потоковый разбор JSON)

## Данные и схема
Используются две таблицы:

### `videos` — итоговая статистика по видео
- `id` (UUID)
- `creator_id` (TEXT)
- `video_created_at` (TIMESTAMPTZ)
- `views_count`, `likes_count`, `comments_count`, `reports_count` (INTEGER)
- `created_at`, `updated_at` (TIMESTAMPTZ)

### `video_snapshots` — почасовые замеры по видео
- `id` (UUID)
- `video_id` (UUID → videos.id)
- `views_count`, `likes_count`, `comments_count`, `reports_count` (INTEGER)
- `delta_views_count`, `delta_likes_count`, `delta_comments_count`, `delta_reports_count` (INTEGER)
- `created_at`, `updated_at` (TIMESTAMPTZ)

Схема: `db/schema.sql`.

## Как работает “распознавание естественного языка”
Используется детерминированный rule-based подход:
- нормализация текста (lower/strip)
- regex-выделение сущностей (creator_id, числа, даты и диапазоны дат)
- выбор шаблона SQL запроса и подстановка аргументов (`$1`, `$2`, ...)

Это обеспечивает предсказуемость и отсутствие “галлюцинаций” при автопроверке.

## Запуск (Docker + локальный Python)

### 1) Поднять PostgreSQL и применить схему
```bash
docker compose up -d
```
Схема применяется автоматически (mount db/schema.sql в initdb).

Проверка подключения:
```bash
psql "host=127.0.0.1 port=55432 user=app dbname=video_analytics"
```
2) Установить зависимости
```bash
python -m venv .venv
```
Windows:
```bash
.venv\Scripts\activate
pip install -r requirements.txt
```
Linux/Mac:
```bash
source .venv/bin/activate
pip install -r requirements.txt
```
3) Загрузить JSON в базу
По умолчанию используется data/videos.json.

Windows PowerShell:
```bash
$env:DB_DSN="postgresql://app:app@127.0.0.1:55432/video_analytics"
$env:JSON_PATH="data\videos.json"
python scripts/load_json.py
```
Linux/Mac:
```bash
export DB_DSN="postgresql://app:app@127.0.0.1:55432/video_analytics"
export JSON_PATH="data/videos.json"
python scripts/load_json.py
```
4) Запустить бота
Создать .env:
```bash
BOT_TOKEN=your_token
DB_DSN=postgresql://app:app@127.0.0.1:55432/video_analytics
```
Запуск:
```bash
python -m app.main
```

## Примеры запросов
- Сколько всего видео есть в системе?
- Сколько видео у креатора с id <id> вышло с 1 ноября 2025 по 5 ноября 2025 включительно?
- Сколько видео набрало больше 100 000 просмотров за всё время?
- На сколько просмотров в сумме выросли все видео 28 ноября 2025?
- Сколько разных видео получали новые просмотры 27 ноября 2025?

### Автопроверка

Открыть @rlt_test_checker_bot и выполнить:

/check @yourbotnickname https://github.com/yourrepo