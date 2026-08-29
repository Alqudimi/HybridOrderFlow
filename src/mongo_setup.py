"""Explicit MongoDB setup entry point kept separate from pipeline logic."""

from __future__ import annotations

from config.settings import Settings
from src.repositories import MongoOrdersRepository


def create_repository(settings: Settings) -> MongoOrdersRepository:
    repository = MongoOrdersRepository(settings.mongo_uri, settings.mongo_database)
    repository.ensure_schema()
    return repository
