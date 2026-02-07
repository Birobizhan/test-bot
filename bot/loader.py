import json
import asyncio
import asyncpg
import os
import logging
from datetime import datetime


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_date(date_str):
    """Преобразует строку в datetime объект"""
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str)
    except ValueError:
        logger.warning(f"Некорректная дата: {date_str}")
        return None


async def load_data():
    db_user = os.getenv("DB_USER", "postgres")
    db_pass = os.getenv("DB_PASSWORD", "postgres")
    db_name = os.getenv("DB_NAME", "analytics_db")
    db_host = os.getenv("DB_HOST", "db")

    dsn = f"postgresql://{db_user}:{db_pass}@{db_host}:5432/{db_name}"

    conn = await asyncpg.connect(dsn)

    try:
        count = await conn.fetchval("SELECT COUNT(*) FROM videos")
        if count > 0:
            logger.info(f"В базе уже есть {count} записей, пропуск загрузки")
            return

        logger.info("База данных пуста, начало заполнения данными")

        if not os.path.exists('videos.json'):
            logger.error("Файл с данными не найден")
            return

        with open('videos.json', 'r', encoding='utf-8') as f:
            data = json.load(f)

        videos_data = []
        snapshots_data = []

        for v in data['videos']:
            videos_data.append((
                v['id'],
                v['creator_id'],
                parse_date(v['video_created_at']),
                v['views_count'],
                v['likes_count'],
                v['comments_count'],
                v['reports_count'],
                parse_date(v['created_at']),
                parse_date(v['updated_at'])
            ))

            for s in v.get('snapshots', []):
                snapshots_data.append((
                    s['id'],
                    s['video_id'],
                    s['views_count'],
                    s['likes_count'],
                    s['comments_count'],
                    s['reports_count'],
                    s['delta_views_count'],
                    s['delta_likes_count'],
                    s['delta_comments_count'],
                    s['delta_reports_count'],
                    parse_date(s['created_at']),
                    parse_date(s['updated_at'])
                ))

        logger.info(f"Вставка в базу данных: {len(videos_data)} видео, {len(snapshots_data)} снапшотов")

        await conn.executemany('''insert into videos (id, creator_id, video_created_at, views_count, likes_count, 
        comments_count, reports_count, created_at, updated_at) values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        on conflict (id) do nothing''', videos_data)

        await conn.executemany('''insert into video_snapshots (id, video_id, views_count, likes_count, comments_count, 
        reports_count, delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count, created_at, updated_at)
            values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            on conflict (id) do nothing''', snapshots_data)

        logger.info("Загрузка данных успешно завершена")

    except Exception as e:
        logger.error(f"Ошибка при загрузке данных: {e}")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(load_data())
