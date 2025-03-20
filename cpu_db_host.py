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

pending_ranges = []
completed_ranges = []
MAX_MEMORY_PERCENT = 0.5  # Используем до 50% доступной RAM
BATCH_SIZE = 10000  # Начальная порция данных

async def get_available_memory():
    """Получение доступной памяти в байтах"""
    logger.info("Начало get_available_memory")
    start_time = time.time()
    memory = psutil.virtual_memory()
    available = memory.available * MAX_MEMORY_PERCENT
    end_time = time.time()
    logger.info("Конец get_available_memory: доступно %.2f MB, выполнено за %.3f мс", available / 1024 / 1024, (end_time - start_time) * 1000)
    return available

async def estimate_doc_size():
    """Оценка размера одной записи в байтах"""
    logger.info("Начало estimate_doc_size")
    start_time = time.time()
    sample_doc = await ranges_collection.find_one({"status": "pending"})
    if sample_doc:
        import bson
        size = len(bson.BSON.encode(sample_doc))
        end_time = time.time()
        logger.info("Конец estimate_doc_size: размер записи %d байт, выполнено за %.3f мс", size, (end_time - start_time) * 1000)
        return size
    end_time = time.time()
    logger.info("Конец estimate_doc_size: нет данных, использую дефолт 500 байт, выполнено за %.3f мс", (end_time - start_time) * 1000)
    return 500

async def load_ranges():
    """Загрузка начальной порции данных"""
    global pending_ranges
    logger.info("Начало load_ranges")
    start_time = time.time()

    logger.info("Запрос доступной памяти")
    mem_start = time.time()
    available_memory = await get_available_memory()
    mem_end = time.time()
    logger.info("Получено %.2f MB доступной памяти за %.3f мс", available_memory / 1024 / 1024, (mem_end - mem_start) * 1000)

    logger.info("Оценка размера документа")
    doc_start = time.time()
    doc_size = await estimate_doc_size()
    doc_end = time.time()
    logger.info("Размер документа %d байт, оценено за %.3f мс", doc_size, (doc_end - doc_start) * 1000)

    max_docs = int(available_memory // doc_size)
    load_size = min(BATCH_SIZE, max_docs // 2)
    logger.info("Рассчитано: max_docs=%d, load_size=%d", max_docs, load_size)

    logger.info("Начало запроса к MongoDB")
    query_start = time.time()
    cursor = ranges_collection.find({"status": "pending"}).sort("_id", ASCENDING).limit(load_size)
    pending_docs = await cursor.to_list(length=load_size)
    query_end = time.time()
    logger.info("Запрос к MongoDB выполнен за %.3f мс, получено %d записей", (query_end - query_start) * 1000, len(pending_docs))

    logger.info("Обработка результатов запроса")
    process_start = time.time()
    pending_ranges.extend([{"start": doc["start"], "end": doc["end"]} for doc in pending_docs])
    process_end = time.time()
    logger.info("Обработка завершена за %.3f мс, добавлено %d записей", (process_end - process_start) * 1000, len(pending_docs))

    end_time = time.time()
    logger.info("Конец load_ranges: загружено %d участков за %.3f сек, RAM: %.2f MB", len(pending_docs), end_time - start_time, len(pending_ranges) * doc_size / 1024 / 1024)

async def fetch_more_ranges():
    """Подгрузка дополнительных данных"""
    global pending_ranges
    logger.info("Начало fetch_more_ranges")
    start_time = time.time()

    logger.info("Запрос доступной памяти")
    mem_start = time.time()
    available_memory = await get_available_memory()
    mem_end = time.time()
    logger.info("Получено %.2f MB доступной памяти за %.3f мс", available_memory / 1024 / 1024, (mem_end - mem_start) * 1000)

    logger.info("Оценка размера документа")
    doc_start = time.time()
    doc_size = await estimate_doc_size()
    doc_end = time.time()
    logger.info("Размер документа %d байт, оценено за %.3f мс", doc_size, (doc_end - doc_start) * 1000)

    max_docs = int(available_memory // doc_size)
    current_docs = len(pending_ranges)
    logger.info("Текущих записей: %d, максимум: %d", current_docs, max_docs)

    if current_docs >= max_docs // 2:
        end_time = time.time()
        logger.info("Конец fetch_more_ranges: RAM заполнен (>50%%), пропускаем, выполнено за %.3f мс", (end_time - start_time) * 1000)
        return

    load_size = min(BATCH_SIZE, (max_docs // 2) - current_docs)
    if load_size <= 0:
        end_time = time.time()
        logger.info("Конец fetch_more_ranges: нет места для загрузки, выполнено за %.3f мс", (end_time - start_time) * 1000)
        return

    logger.info("Начало запроса к MongoDB, load_size=%d", load_size)
    query_start = time.time()
    cursor = ranges_collection.find({"status": "pending"}).sort("_id", ASCENDING).limit(load_size)
    pending_docs = await cursor.to_list(length=load_size)
    query_end = time.time()
    logger.info("Запрос к MongoDB выполнен за %.3f мс, получено %d записей", (query_end - query_start) * 1000, len(pending_docs))

    logger.info("Обработка результатов запроса")
    process_start = time.time()
    pending_ranges.extend([{"start": doc["start"], "end": doc["end"]} for doc in pending_docs])
    process_end = time.time()
    logger.info("Обработка завершена за %.3f мс, добавлено %d записей", (process_end - process_start) * 1000, len(pending_docs))

    end_time = time.time()
    logger.info("Конец fetch_more_ranges: подгружено %d участков за %.3f сек", len(pending_docs), end_time - start_time)

async def save_to_db():
    """Сохранение завершённых участков"""
    global completed_ranges
    logger.info("Начало save_to_db")
    while True:
        start_time = time.time()
        if completed_ranges:
            logger.info("Подготовка bulk_ops для %d записей", len(completed_ranges))
            prep_start = time.time()
            bulk_ops = [UpdateOne({"start": r["start"]}, {"$set": {"status": "completed"}}) for r in completed_ranges]
            prep_end = time.time()
            logger.info("Подготовка bulk_ops выполнена за %.3f мс", (prep_end - prep_start) * 1000)

            logger.info("Выполнение bulk_write")
            write_start = time.time()
            await ranges_collection.bulk_write(bulk_ops, ordered=False)
            write_end = time.time()
            logger.info("Сохранено %d завершённых участков за %.3f мс", len(completed_ranges), (write_end - write_start) * 1000)

            completed_ranges = []
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
            return smalltalk_pb2.TaskResponse(start="", end="")
        task = random.choice(pending_ranges)
        pending_ranges.remove(task)
        end_time = time.time()
        logger.info("Конец GetTask: выдана задача device_id=%d, start=%s, end=%s, выполнено за %.3f мс",
                    request.device_id, task["start"], task["end"], (end_time - start_time) * 1000)
        return smalltalk_pb2.TaskResponse(start=task["start"], end=task["end"])

    async def CompleteTask(self, request, context):
        logger.info("Начало CompleteTask: device_id=%d", request.device_id)
        start_time = time.time()
        completed_ranges.append({"start": request.start, "end": request.end})
        asyncio.create_task(fetch_more_ranges())
        end_time = time.time()
        logger.info("Конец CompleteTask: завершена задача device_id=%d, start=%s, end=%s, выполнено за %.3f мс",
                    request.device_id, request.start, request.end, (end_time - start_time) * 1000)
        return smalltalk_pb2.CompleteResponse(status="success")

    async def Ping(self, request, context):
        logger.info("Начало Ping: message=%s", request.message)
        start_time = time.time()
        end_time = time.time()
        logger.info("Конец Ping: ответ на message=%s, выполнено за %.3f мс", request.message, (end_time - start_time) * 1000)
        return smalltalk_pb2.PingResponse(message=f"Echo: {request.message}")

async def ensure_indexes():
    """Создание индекса на status"""
    logger.info("Начало ensure_indexes")
    start_time = time.time()
    await ranges_collection.create_index([("status", ASCENDING)])
    end_time = time.time()
    logger.info("Конец ensure_indexes: индекс на поле status создан или уже существует, выполнено за %.3f мс", (end_time - start_time) * 1000)

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