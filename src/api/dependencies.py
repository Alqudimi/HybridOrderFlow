from __future__ import annotations

import logging
from collections.abc import Generator
from pymongo import MongoClient
from pymongo.database import Database

from config.settings import Settings

logger = logging.getLogger(__name__)

_mongo_client: MongoClient | None = None


def get_settings() -> Settings:
    settings = Settings.from_env()
    settings.validate()
    return settings


def get_mongo_client() -> MongoClient:
    global _mongo_client
    if _mongo_client is None:
        settings = get_settings()
        _mongo_client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=5000)
    return _mongo_client


def get_db() -> Generator[Database, None, None]:
    client = get_mongo_client()
    settings = get_settings()
    db = client[settings.mongo_database]
    yield db


def close_mongo_client() -> None:
    global _mongo_client
    if _mongo_client is not None:
        _mongo_client.close()
        _mongo_client = None
