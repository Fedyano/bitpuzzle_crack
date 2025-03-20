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

# Параметры Puzzle 69
START = 1 << 68  # 2^68
END = (1 << 69) - 1  # 2^69 - 1
TOTAL_KEYS = END - START + 1
RANGE_SIZE = KEY_COUNT  # 50B из settings
NUM_COLLECTIONS = 20  # 5% на коллекцию
STEP_PERCENT = 0.05  # 5% за проход

# Проверка подключения к MongoDB
async def check_db_connection():
    try:
        start_time = time.time()
        await db.command("ping")
        logger.info("База данных доступна, проверка выполнена за %.3f мс", (time.time() - start_time) * 1000)
    except PyMongoError as e:
        logger.error("Не удалось подключиться к базе данных: %s", e)
        sys.exit(1)

async def get_last_range(collection):
    """Получение последнего записанного участка в коллекции"""
    logger.info("Начало get_last_range для коллекции %s", collection.name)
    start_time = time.time()
    last_doc = await collection.find_one(sort=[("start", -1)])
    if last_doc:
        last_start = int(last_doc["start"], 16)
        logger.info("Найден последний участок: start=%s, выполнено за %.3f мс", hex(last_start), (time.time() - start_time) * 1000)
        return last_start
    logger.info("Коллекция пуста, начинаем с начала, выполнено за %.3f мс", (time.time() - start_time) * 1000)
    return START - RANGE_SIZE

async def update_stats(total_docs, num_ranges, step_start_time):
    """Обновление статистики коллекций"""
    logger.info("Начало update_stats")
    start_time = time.time()
    stats_collection = db["stats"]
    stats = []

    ranges_per_collection = num_ranges // NUM_COLLECTIONS + (1 if num_ranges % NUM_COLLECTIONS else 0)

    for col_idx in range(NUM_COLLECTIONS):
        collection = db[f"ranges_{col_idx}"]
        col_start_time = time.time()
        count = await collection.count_documents({})
        col_end_time = time.time()

        estimated_time = (ranges_per_collection - count) * 0.02  # 20 мс на запись одной записи
        stats.append({
            "collection": f"ranges_{col_idx}",
            "filled_count": count,
            "total_expected": ranges_per_collection,
            "percent_filled": (count / ranges_per_collection) * 100,
            "estimated_time_remaining_sec": estimated_time,
            "time_spent_sec": col_end_time - step_start_time,
            "last_updated": time.time()
        })

    if stats:
        await stats_collection.drop()
        await stats_collection.insert_many(stats)
        logger.info("Статистика обновлена: %d коллекций, выполнено за %.3f мс", len(stats), (time.time() - start_time) * 1000)

async def seed_ranges():
    num_ranges = (TOTAL_KEYS + RANGE_SIZE - 1) // RANGE_SIZE
    ranges_per_collection = num_ranges // NUM_COLLECTIONS + (1 if num_ranges % NUM_COLLECTIONS else 0)
    step_size = int(num_ranges * STEP_PERCENT / NUM_COLLECTIONS)  # Участков на коллекцию за шаг
    logger.info("Всего требуется %d участков по %d ключей, %d участков на коллекцию, шаг %d участков на коллекцию",
                num_ranges, RANGE_SIZE, ranges_per_collection, step_size)

    collections = [db[f"ranges_{i}"] for i in range(NUM_COLLECTIONS)]
    last_starts = await asyncio.gather(*(get_last_range(col) for col in collections))
    start_indices = [(max(0, (last_start - START) // RANGE_SIZE + 1) if last_start >= START else 0) for last_start in last_starts]

    total_docs = sum(await asyncio.gather(*(col.count_documents({}) for col in collections)))
    if total_docs >= num_ranges:
        logger.info("Все %d участков уже засеяны", num_ranges)
        await update_stats(total_docs, num_ranges, time.time())
        return

    logger.info("Найдено %d участков, начинаем заполнение с шагом %d на коллекцию", total_docs, step_size)
    batch_size = 10000

    while total_docs < num_ranges:
        step_start_time = time.time()
        for col_idx in range(NUM_COLLECTIONS):
            collection = collections[col_idx]
            col_start = START + col_idx * ranges_per_collection * RANGE_SIZE
            col_end = min(col_start + ranges_per_collection * RANGE_SIZE - 1, END)
            current_index = start_indices[col_idx]

            operations = []
            for i in range(step_size):
                idx = current_index + i
                if idx >= ranges_per_collection:
                    break
                start = col_start + idx * RANGE_SIZE
                if start > col_end:
                    break
                end = min(start + RANGE_SIZE - 1, col_end)
                doc = {
                    "start": hex(start),
                    "end": hex(end),
                    "status": "pending",
                    "host": None,
                    "processed_at": None
                }
                operations.append(UpdateOne({"start": hex(start)}, {"$setOnInsert": doc}, upsert=True))

                if len(operations) >= batch_size or i == step_size - 1:
                    try:
                        result = await collection.bulk_write(operations, ordered=False)
                        logger.info("Коллекция ranges_%d: записан батч из %d участков, добавлено: %d, пропущено: %d",
                                    col_idx, len(operations), result.upserted_count, len(operations) - result.upserted_count)
                        operations = []
                    except BulkWriteError as e:
                        logger.error("Ошибка при записи батча в ranges_%d: %s", col_idx, e.details)
                        sys.exit(1)

            start_indices[col_idx] += step_size

        total_docs = sum(await asyncio.gather(*(col.count_documents({}) for col in collections)))
        await update_stats(total_docs, num_ranges, step_start_time)
        logger.info("Шаг завершён: заполнено %d/%d участков, время шага %.3f сек", total_docs, num_ranges, time.time() - step_start_time)

    logger.info("Засев всех коллекций завершён")

async def ensure_unique_index():
    logger.info("Начало ensure_unique_index")
    start_time = time.time()
    for col_idx in range(NUM_COLLECTIONS):
        collection = db[f"ranges_{col_idx}"]
        try:
            await collection.create_index([("start", 1)], unique=True)
            logger.info("Уникальный индекс на start создан для ranges_%d", col_idx)
        except PyMongoError as e:
            logger.error("Ошибка при создании индекса для ranges_%d: %s", col_idx, e)
            sys.exit(1)
    end_time = time.time()
    logger.info("Конец ensure_unique_index: выполнено за %.3f мс", (end_time - start_time) * 1000)

async def main():
    await check_db_connection()
    await ensure_unique_index()
    await seed_ranges()

if __name__ == "__main__":
    asyncio.run(main())