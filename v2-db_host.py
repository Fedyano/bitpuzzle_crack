import asyncio
import logging
import grpc
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
from concurrent import futures
import smalltalk_pb2
import smalltalk_pb2_grpc
from datetime import datetime
import time
import sys
import json
import random
import argparse
import os
from micro_calculator import calculate_optimal_micro_count, calculate_micro_ranges, select_task_range
from settings import DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DB_HOST

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

# Загрузка данных пазлов
def load_puzzles():
    try:
        with open("puzzles.json", "r") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error("Файл puzzles.json не найден")
        sys.exit(1)
    except json.JSONDecodeError as e:
        logger.error("Ошибка декодирования puzzles.json: %s", e)
        sys.exit(1)

PUZZLES = load_puzzles()

async def initialize_puzzle(puzzle_num, test_mode=False):
    """Инициализация коллекции для указанного пазла"""
    puzzle_key = str(puzzle_num)
    if puzzle_key not in PUZZLES:
        logger.error("Пазл %s не найден в puzzles.json", puzzle_num)
        sys.exit(1)

    puzzle_data = PUZZLES[puzzle_key]
    total_keys = puzzle_data["total_keys"]
    start_hex = puzzle_data["start_hex"]
    end_hex = puzzle_data["end_hex"]

    micro_count = calculate_optimal_micro_count(total_keys)
    logger.info("Начало расчёта микроучастков для Puzzle %d", puzzle_num)
    start_time = time.time()
    micro_ranges = calculate_micro_ranges(start_hex, end_hex, micro_count)
    end_time = time.time()
    logger.info("Расчёт микроучастков завершён за %.3f сек, создано %d участков", end_time - start_time, len(micro_ranges))

    if test_mode:
        logger.info("Тестовый режим: инициализация Puzzle %d с %d микроучастками", puzzle_num, micro_count)
        return micro_ranges

    collection = db[f"puzzle_{puzzle_num}"]
    logger.info("Запрос количества документов в коллекции puzzle_%d", puzzle_num)
    start_time = time.time()
    existing_docs = await collection.count_documents({})
    end_time = time.time()
    logger.info("Запрос количества документов выполнен за %.3f сек, найдено %d документов", end_time - start_time, existing_docs)

    if existing_docs >= micro_count:
        logger.info("Коллекция puzzle_%d уже содержит %d микроучастков, пропускаем инициализацию", puzzle_num, existing_docs)
        return micro_ranges

    logger.info("Инициализация Puzzle %d", puzzle_num)
    start_time = time.time()
    await collection.drop()
    end_time = time.time()
    logger.info("Очистка коллекции выполнена за %.3f сек", end_time - start_time)

    operations = []
    for i, (micro_start_hex, micro_end_hex, side) in enumerate(micro_ranges):
        doc = {
            "START_HEX": micro_start_hex,
            "END_HEX": micro_end_hex,
            "TASK_COUNT": 0,
            "COUNT_OF_TASKS": 0,
            "COMPLETED_TASKS": 0,
            "PERCENT_COMPLETED_TASKS": 0.0,
            "PERCENT_COMPLETED_KEYS": 0.0,
            "CURRENT_STARTED_AT": None,
            "CURRENT_UPDATED_AT": None,
            "CURRENT_START_HEX": None,
            "CURRENT_END_HEX": None,
            "IS_ACTIVE": False,
            "DEVICE": None,
            "PREDICTION_ENDED_AT": None,
            "CURRENT_SPEED": None,
            "LAST_STARTED_AT": None,
            "LAST_ENDED_AT": None,
            "LAST_START_HEX": None,
            "LAST_END_HEX": None,
            "SIDE": side
        }
        operations.append(UpdateOne({"START_HEX": micro_start_hex}, {"$setOnInsert": doc}, upsert=True))

    logger.info("Начало записи %d микроучастков в коллекцию puzzle_%d", len(operations), puzzle_num)
    start_time = time.time()
    await collection.bulk_write(operations, ordered=False)
    end_time = time.time()
    logger.info("Запись микроучастков завершена за %.3f сек", end_time - start_time)
    return micro_ranges

async def test_select_task_range(micro_ranges, task_size=20_000_000_000_000, num_tests=10):
    """Тестирование выборки задач с выводом полного диапазона микроучастка"""
    logger.info("Начало тестирования select_task_range с %d попытками", num_tests)
    for i in range(num_tests):
        start_time = time.time()
        task = select_task_range(micro_ranges, task_size)
        end_time = time.time()
        if task:
            start_hex, end_hex, col_idx = task
            micro_start_hex, micro_end_hex, side = micro_ranges[col_idx]
            micro_start_int = int(micro_start_hex, 16)
            micro_end_int = int(micro_end_hex, 16)
            micro_keys_count = micro_end_int - micro_start_int + 1
            task_keys_count = int(end_hex, 16) - int(start_hex, 16) + 1
            logger.info("Тест %d: микроучасток=%s-%s, ключи=%d, задача=%s-%s, ключи=%d, col_idx=%d, side=%s, время=%.3f сек",
                        i + 1, micro_start_hex, micro_end_hex, micro_keys_count, start_hex, end_hex, task_keys_count, col_idx, side, end_time - start_time)
        else:
            logger.info("Тест %d: нет доступных задач, время=%.3f сек", i + 1, end_time - start_time)
    logger.info("Тестирование select_task_range завершено")

class SmallTalkServiceServicer(smalltalk_pb2_grpc.SmallTalkServiceServicer):
    def __init__(self, puzzle_num, test_mode=False, micro_ranges=None):
        self.puzzle_num = puzzle_num
        self.test_mode = test_mode
        self.collection = db[f"puzzle_{puzzle_num}"] if not test_mode else None
        self.micro_ranges = micro_ranges if test_mode else None

    async def GetTask(self, request, context):
        logger.debug("Запрос задачи от device_id=%d с key_count=%d", request.device_id, request.key_count)
        task_size = request.key_count

        if self.test_mode:
            if not self.micro_ranges:
                logger.debug("Нет микроучастков в тестовом режиме")
                return smalltalk_pb2.TaskResponse(start="", end="", col_idx=0)
            task = select_task_range(self.micro_ranges, task_size)
            if task:
                start_hex, end_hex, col_idx = task
                return smalltalk_pb2.TaskResponse(start=start_hex, end=end_hex, col_idx=col_idx)
            return smalltalk_pb2.TaskResponse(start="", end="", col_idx=0)

        start_time = time.time()
        inactive_docs = await self.collection.find({"IS_ACTIVE": False}).to_list(length=None)
        end_time = time.time()
        logger.info("Запрос неактивных микроучастков выполнен за %.3f сек, найдено %d документов", end_time - start_time, len(inactive_docs))

        if not inactive_docs:
            logger.debug("Нет доступных микроучастков")
            return smalltalk_pb2.TaskResponse(start="", end="", col_idx=0)

        micro_doc = random.choice(inactive_docs)
        micro_start = int(micro_doc["START_HEX"], 16)
        micro_end = int(micro_doc["END_HEX"], 16)
        completed_keys = micro_doc["COMPLETED_TASKS"] * micro_doc.get("TASK_COUNT", task_size)
        remaining_keys = micro_end - micro_start + 1 - completed_keys

        if remaining_keys <= 0:
            logger.debug("Микроучасток %s полностью обработан", micro_doc["START_HEX"])
            return smalltalk_pb2.TaskResponse(start="", end="", col_idx=0)

        if micro_doc["SIDE"] == "left":
            start_int = micro_start + completed_keys
            end_int = min(start_int + task_size - 1, micro_end)
        else:
            end_int = micro_end - completed_keys
            start_int = max(micro_start, end_int - task_size + 1)

        col_idx = int((micro_start - PUZZLES[str(self.puzzle_num)]["start_decimal"]) // (PUZZLES[str(self.puzzle_num)]["total_keys"] // calculate_optimal_micro_count(PUZZLES[str(self.puzzle_num)]["total_keys"])))
        count_of_tasks = (micro_end - micro_start + 1) // task_size + (1 if (micro_end - micro_start + 1) % task_size else 0)

        start_time = time.time()
        await self.collection.update_one(
            {"START_HEX": micro_doc["START_HEX"]},
            {
                "$set": {
                    "IS_ACTIVE": True,
                    "DEVICE": f"GPU-{request.device_id}",
                    "CURRENT_START_HEX": hex(start_int),
                    "CURRENT_END_HEX": hex(end_int),
                    "CURRENT_STARTED_AT": datetime.utcnow(),
                    "CURRENT_UPDATED_AT": datetime.utcnow(),
                    "TASK_COUNT": task_size,
                    "COUNT_OF_TASKS": count_of_tasks
                }
            }
        )
        end_time = time.time()
        logger.info("Обновление микроучастка выполнено за %.3f сек", end_time - start_time)
        logger.info("Выдана задача: start=%s, end=%s, col_idx=%d", hex(start_int), hex(end_int), col_idx)
        return smalltalk_pb2.TaskResponse(start=hex(start_int), end=hex(end_int), col_idx=col_idx)

    async def CompleteTask(self, request, context):
        logger.debug("Завершение задачи от device_id=%d", request.device_id)
        if self.test_mode:
            return smalltalk_pb2.CompleteResponse(status="success")

        start_time = time.time()
        doc = await self.collection.find_one({"CURRENT_START_HEX": request.start})
        end_time = time.time()
        logger.info("Поиск документа для завершения задачи выполнен за %.3f сек", end_time - start_time)

        if doc:
            completed_tasks = doc["COMPLETED_TASKS"] + 1
            percent_tasks = (completed_tasks / doc["COUNT_OF_TASKS"]) * 100
            percent_keys = ((completed_tasks * doc["TASK_COUNT"]) / (int(doc["END_HEX"], 16) - int(doc["START_HEX"], 16) + 1)) * 100

            start_time = time.time()
            await self.collection.update_one(
                {"START_HEX": doc["START_HEX"]},
                {
                    "$set": {
                        "COMPLETED_TASKS": completed_tasks,
                        "PERCENT_COMPLETED_TASKS": percent_tasks,
                        "PERCENT_COMPLETED_KEYS": percent_keys,
                        "IS_ACTIVE": False,
                        "LAST_STARTED_AT": doc["CURRENT_STARTED_AT"],
                        "LAST_ENDED_AT": datetime.utcnow(),
                        "LAST_START_HEX": doc["CURRENT_START_HEX"],
                        "LAST_END_HEX": doc["CURRENT_END_HEX"]
                    }
                }
            )
            end_time = time.time()
            logger.info("Обновление завершения задачи выполнено за %.3f сек", end_time - start_time)
            logger.info("Задача завершена: start=%s, col_idx=%d", request.start, request.col_idx)
        return smalltalk_pb2.CompleteResponse(status="success")

    async def UpdateStatus(self, request, context):
        logger.debug("Обновление состояния от device_id=%d", request.device_id)
        if self.test_mode:
            return smalltalk_pb2.StatusResponse(status="success")

        start_time = time.time()
        doc = await self.collection.find_one({"DEVICE": f"GPU-{request.device_id}", "IS_ACTIVE": True})
        end_time = time.time()
        logger.info("Поиск активного документа для обновления состояния выполнен за %.3f сек", end_time - start_time)

        if doc:
            start_time = time.time()
            await self.collection.update_one(
                {"START_HEX": doc["START_HEX"]},
                {"$set": {"CURRENT_UPDATED_AT": datetime.utcnow(), "CURRENT_SPEED": request.status}}
            )
            end_time = time.time()
            logger.info("Обновление состояния выполнено за %.3f сек", end_time - start_time)
        return smalltalk_pb2.StatusResponse(status="success")

    async def Ping(self, request, context):
        return smalltalk_pb2.PingResponse(message=f"Echo: {request.message}")

async def serve(puzzle_num, test_mode=False):
    micro_ranges = await initialize_puzzle(puzzle_num, test_mode)
    if micro_ranges:
        # Тестирование выборки задач
        await test_select_task_range(micro_ranges)

        # Проверка первых и последних микроучастков из базы
        if not test_mode:
            collection = db[f"puzzle_{puzzle_num}"]
            logger.info("Проверка крайних микроучастков в базе")
            start_time = time.time()
            first_doc = await collection.find_one(sort=[("START_HEX", 1)])
            last_doc = await collection.find_one(sort=[("START_HEX", -1)])
            end_time = time.time()
            logger.info("Запрос крайних документов выполнен за %.3f сек", end_time - start_time)

            if first_doc and last_doc:
                first_start = first_doc["START_HEX"]
                first_end = first_doc["END_HEX"]
                last_start = last_doc["START_HEX"]
                last_end = last_doc["END_HEX"]
                first_keys = int(first_end, 16) - int(first_start, 16) + 1
                last_keys = int(last_end, 16) - int(last_start, 16) + 1
                logger.info("Первый микроучасток: start=%s, end=%s, ключи=%d", first_start, first_end, first_keys)
                logger.info("Последний микроучасток: start=%s, end=%s, ключи=%d", last_start, last_end, last_keys)

    if test_mode:
        logger.info("Тестовый режим завершён, сервер не запускается")
        return

    server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=10))
    smalltalk_pb2_grpc.add_SmallTalkServiceServicer_to_server(SmallTalkServiceServicer(puzzle_num, test_mode, micro_ranges), server)
    server.add_insecure_port('[::]:50051')
    logger.info("Сервер gRPC запущен на [::]:50051 для Puzzle %d", puzzle_num)
    await server.start()
    await server.wait_for_termination()

def main():
    parser = argparse.ArgumentParser(description="DB Host для BTC Puzzle")
    parser.add_argument("-p", "--puzzle", type=int, required=True, help="Номер пазла для обработки")
    parser.add_argument("--test", action="store_true", help="Запуск в тестовом режиме без базы данных")
    args = parser.parse_args()

    asyncio.run(serve(args.puzzle, args.test))

if __name__ == "__main__":
    main()