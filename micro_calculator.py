import logging
import random
from math import ceil

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def calculate_optimal_micro_count(total_keys, max_micro_count=10000, min_micro_size=80_000_000_000_000):
    """
    Расчёт оптимального количества микроучастков.
    :param total_keys: общее число ключей в диапазоне (int)
    :param max_micro_count: максимальное число микроучастков для базы
    :param min_micro_size: минимальный размер микроучастка (1B ключей)
    :return: количество микроучастков
    """
    target_size = max(min_micro_size, total_keys // max_micro_count)
    micro_count = ceil(total_keys / target_size)
    logger.info("Оптимальное количество микроучастков: %d, целевой размер: %d", micro_count, target_size)
    return micro_count

def calculate_micro_ranges(start_hex, end_hex, micro_count):
    """
    Расчёт границ микроучастков в hex-формате.
    :param start_hex: начальное значение диапазона (hex-строка, например "0x100000000000000000")
    :param end_hex: конечное значение диапазона (hex-строка)
    :param micro_count: количество микроучастков
    :return: список кортежей (start_hex, end_hex, side)
    """
    start_int = int(start_hex, 16)
    end_int = int(end_hex, 16)
    total_keys = end_int - start_int + 1
    base_size = total_keys // micro_count
    remainder = total_keys % micro_count

    ranges = []
    current_start = start_int
    for i in range(micro_count):
        micro_size = base_size + (1 if i < remainder else 0)
        micro_end = current_start + micro_size - 1
        side = "left" if i % 2 == 0 else "right"
        ranges.append((hex(current_start), hex(micro_end), side))
        current_start = micro_end + 1

    logger.info("Рассчитано %d микроучастков, примерный размер: %d ключей", len(ranges), base_size)
    return ranges

def select_task_range(micro_ranges, task_size):
    """
    Выборка микроучастка для задачи с учётом направления.
    :param micro_ranges: список микроучастков [(start_hex, end_hex, side), ...]
    :param task_size: размер задачи в ключах (int)
    :return: (start_hex, end_hex, col_idx) или None, если нет доступных задач
    """
    side = random.choice(["left", "right"])
    logger.info("Выбрано направление: %s", side)

    inactive_ranges = [r for r in micro_ranges if r[2] == side]
    if not inactive_ranges:
        logger.info("Нет доступных микроучастков с направлением %s, пробуем другое", side)
        side = "right" if side == "left" else "left"
        inactive_ranges = [r for r in micro_ranges if r[2] == side]
        if not inactive_ranges:
            logger.debug("Нет доступных микроучастков")
            return None

    micro_start_hex, micro_end_hex, micro_side = random.choice(inactive_ranges)
    micro_start = int(micro_start_hex, 16)
    micro_end = int(micro_end_hex, 16)
    remaining_keys = micro_end - micro_start + 1

    if remaining_keys <= 0:
        logger.info("Микроучасток %s-%s полностью обработан", micro_start_hex, micro_end_hex)
        return None

    if micro_side == "left":
        start_int = micro_start
        end_int = min(start_int + task_size - 1, micro_end)
    else:
        end_int = micro_end
        start_int = max(micro_start, end_int - task_size + 1)

    col_idx = micro_ranges.index((micro_start_hex, micro_end_hex, micro_side))
    logger.info("Выбрана задача: start=%s, end=%s, col_idx=%d, side=%s", hex(start_int), hex(end_int), col_idx, micro_side)
    return (hex(start_int), hex(end_int), col_idx)

# Тестовые функции
def test_calculations():
    # Мокап-данные для Puzzle 69
    total_keys = 295147905179352825856
    start_hex = "0x100000000000000000"
    end_hex = "0x1fffffffffffffffff"
    task_size = 80_000_000_000_000  # 20T ключей

    # Тест 1: Расчёт оптимального количества микроучастков
    micro_count = calculate_optimal_micro_count(total_keys)
    print(f"Оптимальное количество микроучастков: {micro_count}")

    # Тест 2: Расчёт границ микроучастков
    ranges = calculate_micro_ranges(start_hex, end_hex, micro_count)
    print(f"Пример первых 5 микроучастков: {ranges[:5]}")
    print(f"Последний микроучасток: {ranges[-1]}")
    total_calculated_keys = sum(int(end, 16) - int(start, 16) + 1 for start, end, _ in ranges)
    print(f"Рассчитанное общее число ключей: {total_calculated_keys}, ожидаемое: {total_keys}")

    # Тест 3: Выборка задачи
    task = select_task_range(ranges, task_size)
    if task:
        start, end, col_idx = task
        print(f"Выбрана задача: start={start}, end={end}, col_idx={col_idx}")

if __name__ == "__main__":
    test_calculations()