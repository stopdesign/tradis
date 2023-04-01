import argparse
import logging
import sys
from functools import lru_cache
from pathlib import Path

import yaml
from pydantic import BaseModel, BaseSettings

log = logging.getLogger("tradis.settings")


BASE_DIR = Path(__file__).parent
DEFAULT_CONFIG = BASE_DIR.parent / "config" / "default.yaml"


# Конфиг из командной строки
parser = argparse.ArgumentParser()
parser.add_argument("--config", default=DEFAULT_CONFIG)
parser.add_argument("--history", action=argparse.BooleanOptionalAction)
args = parser.parse_args()


class RedisConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 6379
    db: int = 0
    password: str | None = None
    decode_responses = True


class GatewayConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 0


# Прочитать конфиг из файла
try:
    config = yaml.full_load(open(args.config))
except FileNotFoundError:
    log.error(f"Config file not found: '{args.config}'")
    sys.exit(1)


# Рассовать конфиг по моделям
class AppConfig(BaseSettings):
    redis: RedisConfig = RedisConfig(**config.get("redis", {}))
    gateway: GatewayConfig = GatewayConfig(**config.get("gateway", {}))
    instruments: list[dict] = config.get("instruments", [])
    history: bool = bool(args.history)


# Типа Singleton, чтобы не повторять загрузку конфига
@lru_cache(maxsize=0)
def get_config() -> AppConfig:
    return AppConfig()


app_config = get_config()
