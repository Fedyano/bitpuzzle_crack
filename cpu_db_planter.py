import sys
import asyncio
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
from pymongo.errors import PyMongoError

from settings import DB_USER, DB_PASSWORD, DB_HOST, DB_NAME, KEY_COUNT, DB_PORT

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

mongo_client = AsyncIOMotorClient(
    host=DB_HOST,
    port=int(DB_PORT),
    username=DB_USER,
    password=DB_PASSWORD,
    authSource=DB_NAME
)
db = mongo_client[DB_NAME]
ranges_collection = db["ranges"]

START = 1 << 68
END = (1 << 69) - 1
TOTAL_KEYS = END - START + 1
RANGE_SIZE = KEY_COUNT

# Проверка подключения к MongoDB
async def check_db_connection():
    try:
        await db.command("ping")
        logger.info("База данных доступна.")
    except PyMongoError as e:
        logger.error("Не удалось подключиться к базе данных: %s", e)
        sys.exit(1)

async def seed_ranges():
    num_ranges = (TOTAL_KEYS + RANGE_SIZE - 1) // RANGE_SIZE
    logger.info("Засев %d участков по %d ключей", num_ranges, RANGE_SIZE)

    batch_size = 100000
    operations = []

    for i in range(num_ranges):
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
            await ranges_collection.bulk_write(operations, ordered=False)
            logger.info("Записан батч из %d участков", len(operations))
            operations = []

    if operations:
        await ranges_collection.bulk_write(operations, ordered=False)
        logger.info("Записан последний батч из %d участков", len(operations))

    logger.info("Засев участков завершён")

async def main():
    await check_db_connection()
    await seed_ranges()

if __name__ == "__main__":

    asyncio.run(main())