import asyncio
import logging
import grpc
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne, ASCENDING
import random
from concurrent import futures
import smalltalk_pb2
import smalltalk_pb2_grpc
import psutil
import time

from settings import DB_USER, DB_PASSWORD, DB_HOST, DB_NAME, DB_PORT

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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

pending_ranges = []
completed_ranges = []
MAX_MEMORY_PERCENT = 0.5  # До 50% доступной RAM
BATCH_SIZE = 10000  # Порция данных
LOAD_TIMEOUT = 5  # Таймаут загрузки в секундах


async def get_available_memory():
    """Получение доступной памяти в байтах"""
    logger.info("Начало get_available_memory")
    start_time = time.time()
    memory = psutil.virtual_memory()
    available = memory.available * MAX_MEMORY_PERCENT
    end_time = time.time()
    logger.info("Конец get_available_memory: доступно %.2f MB, выполнено за %.3f мс", available / 1024 / 1024,
                 (end_time - start_time) * 1000)
    return available


async def estimate_doc_size(collection):
    """Оценка размера одной записи в байтах"""
    logger.info("Начало estimate_doc_size для %s", collection.name)
    start_time = time.time()
    sample_doc = await collection.find_one({"status": "pending"})
    if sample_doc:
        import bson
        size = len(bson.BSON.encode(sample_doc))
        end_time = time.time()
        logger.info("Конец estimate_doc_size: размер записи %d байт, выполнено за %.3f мс", size,
                     (end_time - start_time) * 1000)
        return size
    end_time = time.time()
    logger.info("Конец estimate_doc_size: нет данных, использую дефолт 500 байт, выполнено за %.3f мс",
                 (end_time - start_time) * 1000)
    return 500


async def get_collection_names():
    """Получение списка всех коллекций ranges_X"""
    logger.info("Начало get_collection_names")
    start_time = time.time()
    all_collections = await db.list_collection_names()
    ranges_collections = [col for col in all_collections if col.startswith("ranges_") and col != "ranges"]
    end_time = time.time()
    logger.info("Конец get_collection_names: найдено %d коллекций, выполнено за %.3f мс", len(ranges_collections),
                 (end_time - start_time) * 1000)
    return ranges_collections


async def load_ranges():
    """Загрузка начальной порции данных из всех коллекций асинхронно"""
    global pending_ranges
    logger.info("Начало load_ranges")
    start_time = time.time()

    logger.info("Запрос доступной памяти")
    mem_start = time.time()
    available_memory = await get_available_memory()
    mem_end = time.time()
    logger.info("Получено %.2f MB доступной памяти за %.3f мс", available_memory / 1024 / 1024,
                 (mem_end - mem_start) * 1000)

    collections = await get_collection_names()
    if not collections:
        logger.warning("Нет коллекций ranges_X в базе данных")
        return

    # Оценка размера документа из первой коллекции
    collection = db[collections[0]]
    logger.info("Оценка размера документа для %s", collection.name)
    doc_start = time.time()
    doc_size = await estimate_doc_size(collection)
    doc_end = time.time()
    logger.info("Размер документа %d байт, оценено за %.3f мс", doc_size, (doc_end - doc_start) * 1000)

    max_docs = int(available_memory // doc_size)
    load_size_per_collection = max(1, min(BATCH_SIZE // len(collections), max_docs // (2 * len(collections))))
    logger.info("Рассчитано: max_docs=%d, load_size_per_collection=%d", max_docs, load_size_per_collection)

    logger.info("Начало асинхронной загрузки из %d коллекций", len(collections))
    query_start = time.time()
    tasks = [db[col_name].find({"status": "pending"}).sort("_id", ASCENDING).limit(load_size_per_collection).to_list(
        length=load_size_per_collection)
             for col_name in collections]
    try:
        results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=LOAD_TIMEOUT)
    except asyncio.TimeoutError:
        logger.warning("Загрузка данных превысила таймаут %d сек, продолжаем с доступными результатами", LOAD_TIMEOUT)
        results = [task.result() for task in tasks if task.done()]

    query_end = time.time()
    logger.info("Асинхронная загрузка выполнена за %.3f мс", (query_end - query_start) * 1000)

    logger.info("Обработка результатов запроса")
    process_start = time.time()
    for col_name, pending_docs in zip(collections, results):
        if pending_docs:
            pending_ranges.extend(
                [{"start": doc["start"], "end": doc["end"], "col_name": col_name} for doc in pending_docs])
            logger.info("Загружено %d записей из %s", len(pending_docs), col_name)

    process_end = time.time()
    logger.info("Обработка завершена за %.3f мс, добавлено %d записей", (process_end - process_start) * 1000,
                 len(pending_ranges))

    end_time = time.time()
    logger.info("Конец load_ranges: загружено %d участков из %d коллекций за %.3f сек, RAM: %.2f MB",
                len(pending_ranges), len(collections), end_time - start_time,
                len(pending_ranges) * doc_size / 1024 / 1024)


async def fetch_more_ranges():
    """Подгрузка дополнительных данных из всех коллекций асинхронно"""
    global pending_ranges
    logger.info("Начало fetch_more_ranges")
    start_time = time.time()

    logger.info("Запрос доступной памяти")
    mem_start = time.time()
    available_memory = await get_available_memory()
    mem_end = time.time()
    logger.info("Получено %.2f MB доступной памяти за %.3f мс", available_memory / 1024 / 1024,
                 (mem_end - mem_start) * 1000)

    collections = await get_collection_names()
    if not collections:
        logger.warning("Нет коллекций ranges_X в базе данных")
        return

    collection = db[collections[0]]
    logger.info("Оценка размера документа для %s", collection.name)
    doc_start = time.time()
    doc_size = await estimate_doc_size(collection)
    doc_end = time.time()
    logger.info("Размер документа %d байт, оценено за %.3f мс", doc_size, (doc_end - doc_start) * 1000)

    max_docs = int(available_memory // doc_size)
    current_docs = len(pending_ranges)
    logger.info("Текущих записей: %d, максимум: %d", current_docs, max_docs)

    if current_docs >= max_docs // 2:
        end_time = time.time()
        logger.info("Конец fetch_more_ranges: RAM заполнен (>50%%), пропускаем, выполнено за %.3f мс",
                    (end_time - start_time) * 1000)
        return

    load_size = min(BATCH_SIZE, (max_docs // 2) - current_docs)
    load_size_per_collection = max(1, load_size // len(collections))
    logger.info("Подгрузка: load_size=%d, load_size_per_collection=%d", load_size, load_size_per_collection)

    logger.info("Начало асинхронной загрузки из %d коллекций", len(collections))
    query_start = time.time()
    tasks = [db[col_name].find({"status": "pending"}).sort("_id", ASCENDING).limit(load_size_per_collection).to_list(
        length=load_size_per_collection)
             for col_name in collections]
    try:
        results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=LOAD_TIMEOUT)
    except asyncio.TimeoutError:
        logger.warning("Подгрузка данных превысила таймаут %d сек, продолжаем с доступными результатами", LOAD_TIMEOUT)
        results = [task.result() for task in tasks if task.done()]

    query_end = time.time()
    logger.info("Асинхронная загрузка выполнена за %.3f мс", (query_end - query_start) * 1000)

    logger.info("Обработка результатов запроса")
    process_start = time.time()
    for col_name, pending_docs in zip(collections, results):
        if pending_docs:
            pending_ranges.extend(
                [{"start": doc["start"], "end": doc["end"], "col_name": col_name} for doc in pending_docs])
            logger.info("Подгружено %d записей из %s", len(pending_docs), col_name)

    process_end = time.time()
    logger.info("Обработка завершена за %.3f мс, добавлено %d записей", (process_end - process_start) * 1000,
                 len(pending_ranges) - current_docs)

    end_time = time.time()
    logger.info("Конец fetch_more_ranges: подгружено %d участков за %.3f сек", len(pending_ranges) - current_docs,
                end_time - start_time)


async def update_stats():
    """Обновление статистики коллекций"""
    logger.info("Начало update_stats")
    start_time = time.time()
    stats_collection = db["stats"]
    collections = await get_collection_names()

    for col_name in collections:
        collection = db[col_name]
        count_start = time.time()
        total_count = await collection.count_documents({})
        pending_count = await collection.count_documents({"status": "pending"})
        count_end = time.time()

        stat = {
            "collection": col_name,
            "filled_count": total_count,
            "pending_count": pending_count,
            "completed_count": total_count - pending_count,
            "last_updated": time.time()
        }
        await stats_collection.update_one({"collection": col_name}, {"$set": stat}, upsert=True)
        logger.info("Обновлена статистика для %s: %d записей, выполнено за %.3f мс",
                     col_name, total_count, (count_end - count_start) * 1000)

    end_time = time.time()
    logger.info("Конец update_stats: обновлено %d коллекций за %.3f мс", len(collections),
                 (end_time - start_time) * 1000)


async def save_to_db():
    """Сохранение завершённых участков"""
    global completed_ranges
    logger.info("Начало save_to_db")
    while True:
        start_time = time.time()
        if completed_ranges:
            logger.info("Подготовка bulk_ops для %d записей", len(completed_ranges))
            prep_start = time.time()
            ops_by_collection = {}
            for r in completed_ranges:
                col_name = r.get("col_name", "ranges_0")  # Дефолт ranges_0
                if col_name not in ops_by_collection:
                    ops_by_collection[col_name] = []
                ops_by_collection[col_name].append(UpdateOne({"start": r["start"]}, {"$set": {"status": "completed"}}))
            prep_end = time.time()
            logger.info("Подготовка bulk_ops выполнена за %.3f мс", (prep_end - prep_start) * 1000)

            logger.info("Выполнение bulk_write для %d коллекций", len(ops_by_collection))
            write_start = time.time()
            for col_name, bulk_ops in ops_by_collection.items():
                collection = db[col_name]
                await collection.bulk_write(bulk_ops, ordered=False)
            write_end = time.time()
            logger.info("Сохранено %d завершённых участков за %.3f мс", len(completed_ranges),
                        (write_end - write_start) * 1000)

            completed_ranges = []
            await update_stats()
            logger.info("Очистка completed_ranges и вызов fetch_more_ranges")
            await fetch_more_ranges()
        else:
            logger.info("Нет данных для сохранения")

        end_time = time.time()
        logger.info("Конец итерации save_to_db, выполнено за %.3f мс, ожидание 60 сек", (end_time - start_time) * 1000)
        await asyncio.sleep(60)


class SmallTalkServiceServicer(smalltalk_pb2_grpc.SmallTalkServiceServicer):
    async def GetTask(self, request, context):
        logger.info("Начало GetTask: device_id=%d", request.device_id)
        start_time = time.time()
        if not pending_ranges:
            end_time = time.time()
            logger.info("Конец GetTask: нет задач, выполнено за %.3f мс", (end_time - start_time) * 1000)
            return smalltalk_pb2.TaskResponse(start="", end="", col_idx=0)
        task = random.choice(pending_ranges)
        col_idx = int(task["col_name"].split("_")[1])  # Извлекаем номер коллекции
        pending_ranges.remove(task)
        end_time = time.time()
        logger.info("Конец GetTask: выдана задача device_id=%d, start=%s, end=%s, col_idx=%d, выполнено за %.3f мс",
                    request.device_id, task["start"], task["end"], col_idx, (end_time - start_time) * 1000)
        return smalltalk_pb2.TaskResponse(start=task["start"], end=task["end"], col_idx=col_idx)

    async def CompleteTask(self, request, context):
        logger.info("Начало CompleteTask: device_id=%d", request.device_id)
        start_time = time.time()
        completed_ranges.append({"start": request.start, "end": request.end, "col_name": f"ranges_{request.col_idx}"})
        asyncio.create_task(fetch_more_ranges())
        end_time = time.time()
        logger.info(
            "Конец CompleteTask: завершена задача device_id=%d, start=%s, end=%s, col_idx=%d, выполнено за %.3f мс",
            request.device_id, request.start, request.end, request.col_idx, (end_time - start_time) * 1000)
        return smalltalk_pb2.CompleteResponse(status="success")

    async def Ping(self, request, context):
        logger.info("Начало Ping: message=%s", request.message)
        start_time = time.time()
        end_time = time.time()
        logger.info("Конец Ping: ответ на message=%s, выполнено за %.3f мс", request.message,
                    (end_time - start_time) * 1000)
        return smalltalk_pb2.PingResponse(message=f"Echo: {request.message}")


async def ensure_indexes():
    """Создание индекса на status для всех коллекций"""
    logger.info("Начало ensure_indexes")
    start_time = time.time()
    collections = await get_collection_names()
    for col_name in collections:
        await db[col_name].create_index([("status", ASCENDING)])
    end_time = time.time()
    logger.info("Конец ensure_indexes: индексы на поле status созданы для %d коллекций, выполнено за %.3f мс",
                len(collections), (end_time - start_time) * 1000)


async def serve():
    logger.info("Начало serve")
    start_time = time.time()

    logger.info("Вызов ensure_indexes")
    await ensure_indexes()

    logger.info("Вызов load_ranges")
    await load_ranges()

    logger.info("Создание фоновой задачи save_to_db")
    asyncio.create_task(save_to_db())

    logger.info("Запуск gRPC сервера")
    server_start = time.time()
    server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=10))
    smalltalk_pb2_grpc.add_SmallTalkServiceServicer_to_server(SmallTalkServiceServicer(), server)
    server.add_insecure_port('[::]:50051')
    await server.start()
    server_end = time.time()
    logger.info("Сервер gRPC запущен на [::]:50051 за %.3f мс", (server_end - server_start) * 1000)

    end_time = time.time()
    logger.info("Конец serve: сервер запущен, общее время %.3f сек", end_time - start_time)
    await server.wait_for_termination()


if __name__ == "__main__":
    asyncio.run(serve())