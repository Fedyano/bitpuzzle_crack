import asyncio
import logging
import grpc
import json
import os
import subprocess
import sys
import bitcrack_pb2
import bitcrack_pb2_grpc
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
with open("gpus.json", "r") as f:
    GPU_CONFIGS = json.load(f)

with open("puzzle.json", "r") as f:
    PUZZLE_CONFIG = json.load(f)["69"]

async def request_task(stub, device_id):
    request = bitcrack_pb2.TaskRequest(device_id=device_id, key_count=KEY_COUNT)
    response = await stub.GetTask(request)
    return {"start": response.start, "end": response.end} if response.start else None

async def complete_task(stub, device_id, start, end):
    request = bitcrack_pb2.CompleteRequest(device_id=device_id, start=start, end=end)
    await stub.CompleteTask(request)

def run_bitcrack(device_id, start, end, gpu_config):
    cmd = [
        CUBITCRACK_PATH, "-d", str(device_id),
        "-b", str(gpu_config["b"]), "-t", str(gpu_config["t"]), "-p", str(gpu_config["p"]),
        "--keyspace", f"{start}:{end}",
        "-o", RESULTS_FILE,
        TARGET_ADDRESS
    ]
    logger.info("Запуск BitCrack на GPU %d: диапазон %s-%s", device_id, start, end)
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stdout, stderr = process.communicate()
    logger.info("Завершён BitCrack на GPU %d: диапазон %s-%s", device_id, start, end)
    return stdout, stderr

def detect_gpus():
    gpus = []
    try:
        result = subprocess.run(["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"], capture_output=True, text=True)
        gpu_names = result.stdout.strip().split('\n')
        for i, name in enumerate(gpu_names):
            gpu_key = next((key for key in GPU_CONFIGS if key in name), "A2000-12")
            gpus.append((i, gpu_key))
        logger.info("Обнаружено GPU: %s", gpus)
    except Exception as e:
        logger.error("Ошибка при обнаружении GPU: %s", e)
        sys.exit(1)
    return gpus

async def gpu_worker(device_id, gpu_type, stub):
    gpu_config = GPU_CONFIGS.get(gpu_type, GPU_CONFIGS["A2000-12"])
    while True:
        task = await request_task(stub, device_id)
        if not task:
            logger.info("Нет доступных задач для GPU %d", device_id)
            break

        start = task["start"]
        end = task["end"]
        stdout, stderr = run_bitcrack(device_id, start, end, gpu_config)

        await complete_task(stub, device_id, start, end)

        if "Found key for address" in stdout:
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

async def main():
    gpus = detect_gpus()
    async with grpc.aio.insecure_channel(SERVER_ADDRESS) as channel:
        stub = bitcrack_pb2_grpc.BitCrackServiceStub(channel)
        tasks = [gpu_worker(gpu_id, gpu_type, stub) for gpu_id, gpu_type in gpus]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())