import asyncio
import logging
import grpc
import json
import os
import subprocess
import sys
import time
import smalltalk_pb2
import smalltalk_pb2_grpc
from datetime import datetime

# Настройка логирования
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Параметры
CUBITCRACK_PATH = os.path.expanduser("~/BitCrack/bin/cuBitCrack")
TARGET_ADDRESS = "19vkiEajfhuZ8bs8Zu2jgmC6oqZbWqhxhG"
RESULTS_FILE = "found_keys.txt"
SERVER_ADDRESS = "localhost:50051"
KEY_COUNT = int(os.environ.get("KEY_COUNT", "50000000000"))  # 50B из окружения

# Загрузка конфигураций
try:
    with open("gpus.json", "r") as f:
        GPU_CONFIGS = json.load(f)
    logger.debug("Конфигурация GPU успешно загружена из gpus.json")
except FileNotFoundError:
    logger.error("Файл gpus.json не найден")
    sys.exit(1)
except json.JSONDecodeError as e:
    logger.error("Ошибка декодирования gpus.json: %s", e)
    sys.exit(1)

try:
    with open("puzzle.json", "r") as f:
        PUZZLE_CONFIG = json.load(f)["69"]
    logger.debug("Конфигурация Puzzle 69 успешно загружена из puzzle.json")
except FileNotFoundError:
    logger.error("Файл puzzle.json не найден")
    sys.exit(1)
except json.JSONDecodeError as e:
    logger.error("Ошибка декодирования puzzle.json: %s", e)
    sys.exit(1)


async def request_task(stub, device_id):
    """Запрос задачи у сервера"""
    logger.debug("Начало request_task для device_id=%d", device_id)
    start_time = time.time()
    request = smalltalk_pb2.TaskRequest(device_id=device_id, key_count=KEY_COUNT)
    try:
        response = await stub.GetTask(request)
        end_time = time.time()
        if response.start:
            logger.debug("Получена задача: start=%s, end=%s, col_idx=%d, выполнено за %.3f мс",
                         response.start, response.end, response.col_idx, (end_time - start_time) * 1000)
            return {"start": response.start, "end": response.end, "col_idx": response.col_idx}
        logger.debug("Нет доступных задач от сервера, выполнено за %.3f мс", (end_time - start_time) * 1000)
        return None
    except grpc.aio.AioRpcError as e:
        logger.error("Ошибка gRPC при запросе задачи: %s", e.details())
        return None


async def complete_task(stub, device_id, start, end, col_idx):
    """Сообщение о завершении задачи"""
    logger.debug("Начало complete_task для device_id=%d", device_id)
    start_time = time.time()
    request = smalltalk_pb2.CompleteRequest(device_id=device_id, start=start, end=end, col_idx=col_idx)
    try:
        await stub.CompleteTask(request)
        end_time = time.time()
        logger.debug("Задача завершена: start=%s, end=%s, col_idx=%d, выполнено за %.3f мс",
                     start, end, col_idx, (end_time - start_time) * 1000)
    except grpc.aio.AioRpcError as e:
        logger.error("Ошибка gRPC при завершении задачи: %s", e.details())


async def run_bitcrack(device_id, start, end, gpu_config):
    """Запуск BitCrack асинхронно"""
    cmd = [
        CUBITCRACK_PATH, "-d", str(device_id),
        "-b", str(gpu_config["b"]), "-t", str(gpu_config["t"]), "-p", str(gpu_config["p"]),
        "--keyspace", f"{start}:{end}",
        "-o", RESULTS_FILE,
        TARGET_ADDRESS
    ]
    logger.debug("Формирование команды BitCrack: %s", " ".join(cmd))
    start_time = time.time()

    if not os.path.exists(CUBITCRACK_PATH):
        logger.error("cuBitCrack не найден по пути %s", CUBITCRACK_PATH)
        return "", "FileNotFoundError"

    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        end_time = time.time()

        stdout_str = stdout.decode('utf-8') if stdout else ""
        stderr_str = stderr.decode('utf-8') if stderr else ""

        if process.returncode == 0:
            logger.info("BitCrack успешно выполнен на GPU %d: диапазон %s-%s, время %.3f сек",
                        device_id, start, end, end_time - start_time)
        else:
            logger.error("BitCrack завершился с ошибкой на GPU %d: код %d, stderr: %s",
                         device_id, process.returncode, stderr_str)

        if stderr_str:
            logger.debug("stderr BitCrack: %s", stderr_str)
        return stdout_str, stderr_str
    except Exception as e:
        logger.error("Ошибка при запуске BitCrack на GPU %d: %s", device_id, e)
        return "", str(e)


def detect_gpus():
    """Определение доступных GPU"""
    logger.debug("Начало detect_gpus")
    start_time = time.time()
    gpus = []
    try:
        result = subprocess.run(["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
                                capture_output=True, text=True, check=True)
        gpu_names = result.stdout.strip().split('\n')
        for i, name in enumerate(gpu_names):
            gpu_key = next((key for key in GPU_CONFIGS if key in name), "A2000-12")
            gpus.append((i, gpu_key))
        end_time = time.time()
        logger.info("Обнаружено %d GPU: %s, выполнено за %.3f мс", len(gpus), gpus, (end_time - start_time) * 1000)
    except subprocess.CalledProcessError as e:
        logger.error("Ошибка nvidia-smi: %s", e)
        sys.exit(1)
    except FileNotFoundError:
        logger.error("nvidia-smi не найден, проверьте установку NVIDIA драйверов")
        sys.exit(1)
    except Exception as e:
        logger.error("Неизвестная ошибка при обнаружении GPU: %s", e)
        sys.exit(1)
    return gpus


async def gpu_worker(device_id, gpu_type, stub):
    """Рабочая функция для GPU"""
    gpu_config = GPU_CONFIGS.get(gpu_type, GPU_CONFIGS["A2000-12"])
    logger.info("Старт gpu_worker для GPU %d (%s) с конфигурацией: %s", device_id, gpu_type, gpu_config)

    while True:
        task = await request_task(stub, device_id)
        if not task:
            logger.info("Нет доступных задач для GPU %d, завершение работы", device_id)
            break

        start = task["start"]
        end = task["end"]
        col_idx = task["col_idx"]
        logger.debug("Получена задача для GPU %d: start=%s, end=%s, col_idx=%d", device_id, start, end, col_idx)

        stdout, stderr = await run_bitcrack(device_id, start, end, gpu_config)

        await complete_task(stub, device_id, start, end, col_idx)

        if "Found key for address" in stdout:
            try:
                with open(RESULTS_FILE, "r") as f:
                    last_line = f.readlines()[-1].strip()
                    parts = last_line.split()
                    if len(parts) >= 2 and parts[0] == TARGET_ADDRESS:
                        private_key = parts[1]
                        success_message = f"""
                        +{'-' * 60}+
                        |{' ' * 20}!!! КЛЮЧ НАЙДЕН !!!{' ' * 20}|
                        +{'-' * 60}+
                        | Устройство: GPU {device_id} ({gpu_type})          |
                        | Диапазон: {start} - {end}                         |
                        | Адрес: {TARGET_ADDRESS}                           |
                        | Приватный ключ: {private_key}                     |
                        | Время: {datetime.now()}                           |
                        | Сохранено в: {RESULTS_FILE}                       |
                        +{'-' * 60}+
                        """
                        logger.info(success_message)
                        print(success_message)
                        sys.exit(0)
            except FileNotFoundError:
                logger.error("Файл результатов %s не найден", RESULTS_FILE)
            except Exception as e:
                logger.error("Ошибка при чтении файла результатов: %s", e)


async def main():
    """Главная функция"""
    logger.info("Запуск gpu_host")
    start_time = time.time()

    gpus = detect_gpus()
    if not gpus:
        logger.error("Не обнаружено доступных GPU, завершение работы")
        sys.exit(1)

    logger.debug("Создание gRPC канала к %s", SERVER_ADDRESS)
    try:
        async with grpc.aio.insecure_channel(SERVER_ADDRESS) as channel:
            stub = smalltalk_pb2_grpc.SmallTalkServiceStub(channel)
            logger.debug("Канал gRPC успешно создан")

            # Проверка соединения
            logger.debug("Проверка соединения с сервером")
            ping_start = time.time()
            await stub.Ping(smalltalk_pb2.PingRequest(message="Ping from gpu_host"))
            ping_end = time.time()
            logger.info("Соединение с сервером подтверждено за %.3f мс", (ping_end - ping_start) * 1000)

            tasks = [gpu_worker(gpu_id, gpu_type, stub) for gpu_id, gpu_type in gpus]
            await asyncio.gather(*tasks)
    except grpc.aio.AioRpcError as e:
        logger.error("Ошибка соединения с сервером gRPC: %s", e.details())
        sys.exit(1)

    end_time = time.time()
    logger.info("Завершение gpu_host, общее время работы %.3f сек", end_time - start_time)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Программа прервана пользователем")
        sys.exit(0)
    except Exception as e:
        logger.error("Неизвестная ошибка в main: %s", e)
        sys.exit(1)