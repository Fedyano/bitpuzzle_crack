import asyncio
import logging
import time
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
from settings import DB_USER, DB_PASSWORD, DB_HOST, DB_NAME, DB_PORT

# Настройка логирования
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Параметры Puzzle 69
START_FULL = 1 << 68  # 2^68
END_FULL = (1 << 69) - 1  # 2^69 - 1
TOTAL_KEYS_FULL = END_FULL - START_FULL + 1  # 2^68
MICRO_RANGE_SIZE = 50_000_000_000  # 50B ключей
NUM_COLLECTIONS = 20  # 5% на коллекцию
TEST_PERCENT = 0.005  # 0.5% для теста

# Подключение к MongoDB
mongo_client = AsyncIOMotorClient(
    host=DB_HOST,
    port=int(DB_PORT),
    username=DB_USER,
    password=DB_PASSWORD,
    authSource=DB_NAME
)
db = mongo_client[DB_NAME]

async def measure_write_speed(num_test_docs=100_000):
    """Измерение скорости записи в базу данных"""
    logger.info("Начало измерения скорости записи")
    test_collection = db["test_ranges"]
    await test_collection.drop()

    start_time = time.time()
    operations = []
    for i in range(num_test_docs):
        start = i * MICRO_RANGE_SIZE
        end = start + MICRO_RANGE_SIZE - 1
        doc = {
            "start": hex(start),
            "end": hex(end),
            "status": "pending",
            "host": None,
            "processed_at": None
        }
        operations.append(UpdateOne({"start": hex(start)}, {"$setOnInsert": doc}, upsert=True))

    await test_collection.bulk_write(operations, ordered=False)
    end_time = time.time()

    write_time = end_time - start_time
    write_speed = num_test_docs / write_time  # участков/сек
    logger.info("Скорость записи: %.2f участков/сек, время %.3f сек для %d записей", write_speed, write_time, num_test_docs)
    return write_speed

async def calculate_seeding(puzzle_start, puzzle_end, micro_range_size, num_collections, gpu_configs, write_speed, test_percent=None):
    """Расчёт параметров засеивания"""
    logger.info("Начало расчёта засеивания")
    total_keys = puzzle_end - puzzle_start + 1
    if test_percent:
        total_keys = int(total_keys * test_percent)
        puzzle_end = puzzle_start + total_keys - 1
        logger.info("Тестовый режим: %.2f%% от диапазона, %d ключей", test_percent * 100, total_keys)

    num_micro_ranges = (total_keys + micro_range_size - 1) // micro_range_size
    ranges_per_collection = num_micro_ranges // num_collections + (1 if num_micro_ranges % num_collections else 0)

    logger.info("Общее число ключей: %d", total_keys)
    logger.info("Число микроучастков: %d", num_micro_ranges)
    logger.info("Число коллекций: %d", num_collections)
    logger.info("Микроучастков на коллекцию: %d", ranges_per_collection)

    # Оценка времени записи
    total_write_time = num_micro_ranges / write_speed  # сек
    logger.info("Ожидаемое время записи: %.2f сек (%.2f часов)", total_write_time, total_write_time / 3600)

    # Оценка времени обработки GPU
    gpu_speeds = {name: config["speed"] for name, config in gpu_configs.items()}
    total_gpu_speed = sum(gpu_speeds.values())  # keys/s
    micro_range_time = micro_range_size / total_gpu_speed  # сек на микроучасток
    total_process_time = num_micro_ranges * micro_range_time  # сек

    logger.info("Скорость GPU: %s", {k: f"{v/1e9:.2f} Bkeys/s" for k, v in gpu_speeds.items()})
    logger.info("Общая скорость GPU: %.2f Bkeys/s", total_gpu_speed / 1e9)
    logger.info("Время на микроучасток: %.2f сек", micro_range_time)
    logger.info("Ожидаемое время обработки: %.2f сек (%.2f дней)", total_process_time, total_process_time / 86400)

    stats = {
        "puzzle_start": hex(puzzle_start),
        "puzzle_end": hex(puzzle_end),
        "total_keys": total_keys,
        "micro_range_size": micro_range_size,
        "num_micro_ranges": num_micro_ranges,
        "num_collections": num_collections,
        "ranges_per_collection": ranges_per_collection,
        "write_speed": write_speed,
        "total_write_time_sec": total_write_time,
        "gpu_speeds": gpu_speeds,
        "total_gpu_speed": total_gpu_speed,
        "micro_range_time_sec": micro_range_time,
        "total_process_time_sec": total_process_time,
        "timestamp": time.time()
    }
    await db["seeding_plan"].insert_one(stats)
    logger.info("Статистика засеивания записана в seeding_plan")

    return stats

async def main():
    # Замер скорости записи
    write_speed = await measure_write_speed()

    # Полные параметры Puzzle 69
    logger.info("Расчёт для Puzzle 69 (полный диапазон)")
    await calculate_seeding(
        puzzle_start=START_FULL,
        puzzle_end=END_FULL,
        micro_range_size=MICRO_RANGE_SIZE,
        num_collections=NUM_COLLECTIONS,
        gpu_configs=GPU_CONFIGS,
        write_speed=write_speed
    )

    # Тестовый пазл (Puzzle 32, известный ключ: 0x0000000000000000000000000000000000000000000000000000000000001234)
    logger.info("Расчёт для тестового Puzzle 32 (0.5% от известного ключа)")
    puzzle_32_start = 1 << 31
    puzzle_32_end = (1 << 32) - 1
    known_key = 0x1234
    test_range = int((puzzle_32_end - puzzle_32_start + 1) * TEST_PERCENT)
    test_start = max(puzzle_32_start, known_key - test_range // 2)
    test_end = min(puzzle_32_end, test_start + test_range - 1)

    await calculate_seeding(
        puzzle_start=test_start,
        puzzle_end=test_end,
        micro_range_size=MICRO_RANGE_SIZE,
        num_collections=NUM_COLLECTIONS,
        gpu_configs=GPU_CONFIGS,
        write_speed=write_speed,
        test_percent=TEST_PERCENT
    )

if __name__ == "__main__":
    asyncio.run(main())