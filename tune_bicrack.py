import subprocess
import logging
import json
import os
import sys
import time

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

CUBITCRACK_PATH = os.path.expanduser("~/BitCrack/bin/cuBitCrack")
TARGET_ADDRESS = "19vkiEajfhuZ8bs8Zu2jgmC6oqZbWqhxhG"

with open("gpus.json", "r") as f:
    GPU_CONFIGS = json.load(f)


def run_bitcrack_test(device_id, b, t, p, duration=60):
    cmd = [
        CUBITCRACK_PATH, "-d", str(device_id),
        "-b", str(b), "-t", str(t), "-p", str(p),
        "--keyspace", "0x1:0x100000000",
        TARGET_ADDRESS
    ]
    logger.debug("Запуск теста: %s", " ".join(cmd))
    start_time = time.time()
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    time.sleep(duration)  # Даём тесту поработать
    process.terminate()
    stdout, stderr = process.communicate()

    keys_per_sec = None
    for line in stderr.splitlines():
        if "Keys per second" in line:
            keys_per_sec = float(line.split(":")[1].strip().split()[0]) * 1e6  # Переводим Mkeys/s в keys/s
            break

    logger.debug("Результат теста: b=%d, t=%d, p=%d, скорость=%.2f Mkeys/s", b, t, p,
                 keys_per_sec / 1e6 if keys_per_sec else 0)
    return keys_per_sec


def tune_parameters(device_id, gpu_type, max_vram_mb=12000):
    logger.info("Тестирование параметров для GPU %d (%s)", device_id, gpu_type)
    results = []

    # Возможные значения параметров
    b_values = [128, 256, 512]
    t_values = [128, 256, 512, 1024]
    p_values = [1024, 2048, 4096, 8192, 16384]

    for b in b_values:
        for t in t_values:
            for p in p_values:
                # Проверка использования VRAM (примерно: b * t * p * 40 байт)
                vram_usage_mb = (b * t * p * 40) / (1024 * 1024)
                if vram_usage_mb > max_vram_mb:
                    logger.debug("Пропуск b=%d, t=%d, p=%d: VRAM %.2f MB превышает %d MB", b, t, p, vram_usage_mb,
                                 max_vram_mb)
                    continue

                speed = run_bitcrack_test(device_id, b, t, p)
                if speed:
                    results.append({"b": b, "t": t, "p": p, "speed": speed, "vram_mb": vram_usage_mb})
                    logger.info("Тест: b=%d, t=%d, p=%d, скорость=%.2f Mkeys/s, VRAM=%.2f MB",
                                b, t, p, speed / 1e6, vram_usage_mb)

    # Сортировка по скорости
    results.sort(key=lambda x: x["speed"], reverse=True)
    best_config = results[0] if results else None
    logger.info("Лучшая конфигурация для %s: %s", gpu_type, best_config)
    return best_config


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


def main():
    gpus = detect_gpus()
    for device_id, gpu_type in gpus:
        best_config = tune_parameters(device_id, gpu_type)
        if best_config:
            GPU_CONFIGS[gpu_type] = {"b": best_config["b"], "t": best_config["t"], "p": best_config["p"],
                                     "speed": best_config["speed"]}

    with open("gpus.json", "w") as f:
        json.dump(GPU_CONFIGS, f, indent=2)
    logger.info("Конфигурация GPU обновлена в gpus.json")


if __name__ == "__main__":
    main()
