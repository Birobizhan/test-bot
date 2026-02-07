import asyncio
import logging
import os
import asyncpg
from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart
from openai import AsyncOpenAI

BOT_TOKEN = os.getenv("BOT_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASSWORD", "postgres")
DB_NAME = os.getenv("DB_NAME", "analytics_db")
DB_HOST = os.getenv("DB_HOST", "db")

DB_DSN = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"

SYSTEM_PROMPT = """
Ты — эксперт по SQL (PostgreSQL). Твоя задача — преобразовать вопрос пользователя на русском языке в SQL-запрос.
Отвечай ТОЛЬКО SQL-кодом, без кавычек markdown (```sql ... ```), без пояснений.
Результатом выполнения SQL должно быть ВСЕГДА одно число (int или float). Пиши только запросы с select.
Не допусти вредоносных запросов со вставкой данных или удалением таблиц или подобным

Схема базы данных:
1. Таблица `videos` (общая статистика видео):
   - id (UUID), creator_id (uuid)
   - video_created_at (timestamp) - дата публикации видео
   - views_count, likes_count, comments_count (int) - финальные показатели
2. Таблица `video_snapshots` (почасовая динамика):
   - video_id (UUID), created_at (timestamp) - время замера
   - delta_views_count, delta_likes_count... (int) - прирост за час (важно для вопросов про "рост" или "новые просмотры")

Правила логики:
1. Если спрашивают "Сколько всего видео..." -> count (*) from videos.
2. Если спрашивают про "опубликовано с... по..." -> фильтруй по videos.video_created_at.
3. Если спрашивают про "На сколько выросли просмотры/лайки за [ДАТА]" -> используй sum(delta_...) из таблицы video_snapshots, где date(created_at) = 'YYYY-MM-DD'.
4. Если спрашивают "Сколько видео получили новые просмотры/лайки за [ДАТА]" -> считай count(distinct video_id) из video_snapshots, где delta_... > 0 и дата совпадает.
5. Для вопросов "больше 100 000 просмотров за всё время" -> используй videos.views_count.
6. Даты из запроса (например "28 ноября 2025") переводи в формат 'YYYY-MM-DD'.
"""

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
openai_client = AsyncOpenAI(base_url="https://openrouter.ai/api/v1", api_key=OPENAI_API_KEY)


async def get_sql_from_llm(user_text: str) -> str | None:
    try:
        response = await openai_client.chat.completions.create(
            model="arcee-ai/trinity-large-preview:free",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_text}
            ],
            temperature=0
        )
        sql = response.choices[0].message.content.strip()
        sql = sql.replace("```sql", "").replace("```", "").strip()
        return sql
    except Exception as e:
        logging.error(f"Ошибка LLM: {e}")
        return None


async def execute_sql(pool: asyncpg.Pool, sql: str):
    async with pool.acquire() as conn:
        try:
            result = await conn.fetchval(sql)
            return result if result is not None else 0
        except Exception as e:
            logging.error(f"Ошибка выполнения запроса: {e}\nЗапрос: {sql}")
            return "Ошибка выполнения запроса"


@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    await message.answer("Привет! Я готов отвечать на вопросы")


@dp.message()
async def handle_message(message: types.Message, db_pool: asyncpg.Pool):
    user_text = message.text
    sql_query = await get_sql_from_llm(user_text)
    if not sql_query:
        await message.answer("Не удалось понять запрос")
        return

    logging.info(f"Запрос: {user_text}, SQL скрипт: {sql_query}")
    result = await execute_sql(db_pool, sql_query)
    await message.answer(str(result))


async def main():
    logging.basicConfig(level=logging.INFO)

    try:

        pool = await asyncpg.create_pool(DB_DSN)
        logging.info("Успешное подключение к БД")
    except Exception as e:
        logging.error(f"Ошибка подключения: {e}")
        return

    try:
        await dp.start_polling(bot, db_pool=pool)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
