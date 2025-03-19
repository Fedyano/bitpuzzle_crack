
# BitCrack Puzzle Solver

Проект для распределённого поиска приватных ключей Bitcoin в заданных диапазонах (например, Puzzle 69) с использованием GPU. Основан на BitCrack, оптимизирован для масштабирования и совместной работы.

## Архитектура

Проект состоит из трёх основных компонентов:

1. **`cpu_db_planter`**:
   - Засевает базу данных MongoDB участками по 50 миллиардов ключей (50B).
   - Каждый участок — это диапазон для обработки GPU-хостами.

2. **`cpu_db_host`**:
   - Сервер gRPC, хранящий выборку участков в оперативной памяти (RAM).
   - Отдаёт задачи GPU-хостам и принимает результаты через JSON-gRPC.
   - Периодически синхронизирует завершённые участки с MongoDB.

3. **`gpu_host`**:
   - Клиент gRPC, выполняющий вычисления с помощью BitCrack.
   - Автоматически определяет доступные GPU и использует настройки из `gpus.json`.
   - Запрашивает задачи у `cpu_db_host`, указывая желаемое количество ключей (по умолчанию 50B).

## Установка

1. **Зависимости**:
   ```bash
   pip install motor aiohttp grpcio grpcio-tools
   ```

2. **Сборка BitCrack**:
   - Склонируйте репозиторий BitCrack и соберите его:
     ```bash
     git clone https://github.com/brichard19/BitCrack
     cd BitCrack
     make BUILD_CUDA=1
     ```
   - Убедитесь, что `cuBitCrack` находится в `~/BitCrack/bin/`.

3. **MongoDB**:
   - Установите MongoDB в обычном режиме (репликация не требуется):
     ```bash
     sudo apt install mongodb
     sudo systemctl start mongodb
     ```

4. **Генерация gRPC кода**:
   ```bash
   python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. bitcrack.proto
   ```

## Конфигурация

### `settings.json`
Настройки базы данных и окружения хранятся в `settings.json`. Пример:

```json
{
  "production": {
    "DB_USER": "user",
    "DB_PASSWORD": "pass",
    "DB_HOST": "localhost",
    "DB_PORT": "27017",
    "DB_NAME": "btc_puzzle_69",
    "KEY_COUNT": "50000000000"
  },
  "development": {
    "DB_USER": "",
    "DB_PASSWORD": "",
    "DB_HOST": "localhost",
    "DB_PORT": "27017",
    "DB_NAME": "btc_puzzle_69_dev",
    "KEY_COUNT": "50000000000"
  },
  "default": {
    "DB_USER": "",
    "DB_PASSWORD": "",
    "DB_HOST": "localhost",
    "DB_PORT": "27017",
    "DB_NAME": "btc_puzzle_69",
    "KEY_COUNT": "50000000000"
  }
}
```

- Переменные окружения (`APP_ENV`, `APP_CD`) переопределяют выбор конфигурации.

### `gpus.json`
Настройки GPU для BitCrack:

```json
{
  "5080": {
    "t": 128,
    "b": 256,
    "p": 2048,
    "speed": 4520000000
  },
  "4090": {
    "t": 128,
    "b": 256,
    "p": 2048,
    "speed": 5960000000
  },
  "A2000-12": {
    "t": 128,
    "b": 256,
    "p": 2048,
    "speed": 648000000
  }
}
```

- `t`: Количество потоков на блок.
- `b`: Количество блоков.
- `p`: Количество точек на поток.
- `speed`: Скорость генерации ключей (keys/s).

Эти параметры влияют на вычислительную нагрузку:
- **Потоки (`-t`)**: До 1024 на блок, определяет параллелизм внутри блока.
- **Блоки (`-b`)**: Количество независимых групп потоков.
- **Точки (`-p`)**: Число ключей, генерируемых одним потоком; больше точек = больше VRAM.

### `puzzle.json`
Данные о пазлах:

```json
{
  "69": {
    "id": 69,
    "target_address": "19vkiEajfhuZ8bs8Zu2jgmC6oqZbWqhxhG",
    "start": 68,
    "end": 69,
    "progress": 0
  }
}
```

## Запуск

1. **Засев базы**:
   ```bash
   python3 cpu_db_planter.py
   ```

2. **Сервер gRPC**:
   ```bash
   python3 cpu_db_host.py
   ```

3. **Клиент GPU**:
   ```bash
   export APP_ENV=production
   pm2 start gpu_host.py --name btcpuzzle && pm2 logs 0
   ```

## Параметры BitCrack
BitCrack использует параметры для управления вычислениями:
- `-t <threads>`: Потоки на блок (например, 128, 256, 512).
- `-b <blocks>`: Блоки (например, 128, 256).
- `-p <points>`: Точки на поток (например, 2048, 4096), определяет использование VRAM.
- Пример: `-b 128 -t 256 -p 2048` генерирует \( 128 \times 256 \times 2048 = 67,108,864 \) точек (~2.5 GB VRAM).

Для оптимизации:
- Увеличивайте `-p` для карт с большим VRAM (RTX 4090: 24 GB).
- Балансируйте `-t` и `-b` для максимального использования CUDA-ядер.

## Масштабирование
- **Добавление GPU-хостов**: Запустите `gpu_host.py` на новых машинах с `SERVER_ADDRESS`.
- **Совместная работа**: Используйте `bitcrack.proto` для подключения сторонних клиентов.

## Требования
- Python 3.8+
- MongoDB 4.0+
- NVIDIA GPU с CUDA


{
  "production": {
    "DB_USER": "user",
    "DB_PASSWORD": "pass",
    "DB_HOST": "localhost",
    "DB_PORT": "27017",
    "DB_NAME": "btc_puzzle_69",
    "KEY_COUNT": "50000000000"
  },
  "development": {
    "DB_USER": "",
    "DB_PASSWORD": "",
    "DB_HOST": "localhost",
    "DB_PORT": "27017",
    "DB_NAME": "btc_puzzle_69_dev",
    "KEY_COUNT": "50000000000"
  },
  "default": {
    "DB_USER": "",
    "DB_PASSWORD": "",
    "DB_HOST": "localhost",
    "DB_PORT": "27017",
    "DB_NAME": "btc_puzzle_69",
    "KEY_COUNT": "50000000000"
  }
}
