import sys
import asyncio
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
from pymongo.errors import PyMongoError

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
    last_doc = await ranges_collection.find_one(sort=[("start", -1)])  # Сортировка по убыванию start
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
    last_start = await get_last_range()
    start_index = (last_start - START) // RANGE_SIZE + 1  # Индекс следующего участка
    if start_index >= num_ranges:
        logger.info("Все участки уже засеяны")
        return

    logger.info("Продолжаем с индекса %d (%s)", start_index, hex(START + start_index * RANGE_SIZE))

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
                await ranges_collection.bulk_write(operations, ordered=False)
                logger.info("Записан батч из %d участков, последний: %s", len(operations), hex(start))
                operations = []
            except PyMongoError as e:
                logger.error("Ошибка при записи батча: %s", e)
                sys.exit(1)

    if operations:
        try:
            await ranges_collection.bulk_write(operations, ordered=False)
            logger.info("Записан последний батч из %d участков", len(operations))
        except PyMongoError as e:
            logger.error("Ошибка при записи последнего батча: %s", e)
            sys.exit(1)

    logger.info("Засев участков завершён")

async def main():
    await check_db_connection()
    await seed_ranges()

if __name__ == "__main__":
    asyncio.run(main())