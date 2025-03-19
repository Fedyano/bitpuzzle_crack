import os
import json
import logging
import sys

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Определение окружения и пути к конфигурации
env = os.getenv('APP_ENV', 'default').lower()
config_dir = os.getenv('APP_CD', 'config').lower()

# Маппинг файлов конфигурации
config_file_map = {
    'production': f'{config_dir}/production.json',
    'development': f'{config_dir}/development.json',
    'default': f'{config_dir}/default.json',
    'local': f'{config_dir}/local.json'
}

# Проверка наличия файлов
available_files = [path for path in config_file_map.values() if os.path.exists(path)]
logger.info(f"Available configuration files: {available_files}")

# Выбор файла конфигурации
config_file = config_file_map.get(env, f'{config_dir}/default.json')
if not os.path.exists(config_file):
    logger.warning(f"Configuration file for '{env}' not found. Using default configuration.")
    config_file = config_file_map['default']

# Чтение конфигурации из JSON
with open(config_file, 'r') as f:
    config_data = json.load(f)

# Получение данных для текущего окружения
# config = config_data.get(env, config_data)

def get_config_param(env_var, key, default=None):
    """Получает параметр из окружения или JSON-конфигурации"""
    return os.getenv(env_var) or config_data.get(key, default)

# Параметры базы данных (обычный режим MongoDB)
DB_USER = get_config_param('DB_USER', 'DB_USER', '')
DB_PASSWORD = get_config_param('DB_PASSWORD', 'DB_PASSWORD', '')
DB_HOST = get_config_param('DB_HOST', 'DB_HOST', 'localhost')
DB_PORT = get_config_param('DB_PORT', 'DB_PORT', '27017')
DB_NAME = get_config_param('DB_NAME', 'DB_NAME', 'btcpuzzle')
KEY_COUNT = int(get_config_param('KEY_COUNT', 'KEY_COUNT', '50000000000'))