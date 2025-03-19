import sys
import asyncio
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
from pymongo.errors import PyMongoError, BulkWriteError
import time
from settings import DB_USER, DB_PASSWORD, DB_HOST, DB_NAME, KEY_COUNT, DB_PORT

# Настройка логирования
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Подключение к MongoDB
mongo_client = AsyncIOMotorClient(
    host=DB_HOST,
    port=int(DB_PORT),
    username=DB_USER,
    password=DB_PASSWORD,
    authSource=DB_NAME
)
db = mongo_client[DB_NAME]
ranges_collection = db["ranges"]

# Параметры Puzzle 69
START = 1 << 68  # 2^68
END = (1 << 69) - 1  # 2^69 - 1
TOTAL_KEYS = END - START + 1
RANGE_SIZE = KEY_COUNT  # 50B из settings


# Проверка подключения к MongoDB
async def check_db_connection():
    try:
        await db.command("ping")
        logger.info("База данных доступна.")
    except PyMongoError as e:
        logger.error("Не удалось подключиться к базе данных: %s", e)
        sys.exit(1)


async def get_last_range():
    """Получение последнего записанного участка"""
    last_doc = await ranges_collection.find_one(sort=[("start", -1)])  # Последний по start
    if last_doc:
        last_start = int(last_doc["start"], 16)
        logger.info("Найден последний участок: start=%s", hex(last_start))
        return last_start
    logger.info("База пуста, начинаем с начала")
    return START - RANGE_SIZE  # Чтобы первый цикл начался с START


async def seed_ranges():
    num_ranges = (TOTAL_KEYS + RANGE_SIZE - 1) // RANGE_SIZE
    logger.info("Всего требуется %d участков по %d ключей", num_ranges, RANGE_SIZE)

    # Проверяем текущее состояние базы
    start_time = time.time()
    total_docs = await ranges_collection.count_documents({})
    end_time = time.time()
    logger.debug("Подсчёт общего количества документов выполнен за %.3f мс", (end_time - start_time) * 1000)

    if total_docs >= num_ranges:
        logger.info("Все %d участков уже засеяны", num_ranges)
        return

    last_start = await get_last_range()
    start_index = (last_start - START) // RANGE_SIZE + 1 if last_start >= START else 0
    logger.info("Найдено %d участков, продолжаем с индекса %d (%s)", total_docs, start_index,
                hex(START + start_index * RANGE_SIZE))

    batch_size = 100000
    operations = []

    for i in range(start_index, num_ranges):
        start = START + i * RANGE_SIZE
        end = min(start + RANGE_SIZE - 1, END)
        doc = {
            "start": hex(start),
            "end": hex(end),
            "status": "pending",
            "host": None,
            "processed_at": None
        }
        operations.append(UpdateOne({"start": hex(start)}, {"$setOnInsert": doc}, upsert=True))

        if len(operations) >= batch_size:
            try:
                result = await ranges_collection.bulk_write(operations, ordered=False)
                logger.info("Записан батч из %d участков, добавлено: %d, пропущено: %d, последний: %s",
                            len(operations), result.upserted_count, len(operations) - result.upserted_count, hex(start))
                operations = []
            except BulkWriteError as e:
                logger.error("Ошибка при записи батча: %s", e.details)
                sys.exit(1)

    if operations:
        try:
            result = await ranges_collection.bulk_write(operations, ordered=False)
            logger.info("Записан последний батч из %d участков, добавлено: %d, пропущено: %d",
                        len(operations), result.upserted_count, len(operations) - result.upserted_count)
        except BulkWriteError as e:
            logger.error("Ошибка при записи последнего батча: %s", e.details)
            sys.exit(1)

    # Проверяем итоговое количество
    final_count = await ranges_collection.count_documents({})
    logger.info("Засев завершён, всего в базе %d участков из %d", final_count, num_ranges)


async def ensure_unique_index():
    """Проверка и создание уникального индекса на start"""
    try:
        await ranges_collection.create_index([("start", 1)], unique=True)
        logger.info("Уникальный индекс на поле start успешно создан или уже существует")
    except PyMongoError as e:
        if "duplicate key error" in str(e):
            logger.error("Обнаружены дубликаты в базе: %s", e)
            logger.info("Рекомендуется очистить базу командой `db.ranges.drop()` и перезапустить скрипт")
        else:
            logger.error("Ошибка при создании индекса: %s", e)
        sys.exit(1)


async def main():
    await check_db_connection()
    await ensure_unique_index()  # Проверяем индекс перед началом
    await seed_ranges()


if __name__ == "__main__":
    asyncio.run(main())