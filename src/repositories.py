from __future__ import annotations

import copy
import hashlib
import json
import logging
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any, Protocol

logger = logging.getLogger(__name__)

VALIDATED_JSON_SCHEMA: dict[str, Any] = {
    "bsonType": "object",
    "required": ["order_id", "customer_id", "quality_status"],
    "properties": {
        "order_id": {"bsonType": "string", "minLength": 1},
        "customer_id": {"bsonType": "string", "minLength": 1},
        "quality_status": {"enum": ["valid", "corrected"]},
        "order_date": {"bsonType": "string"},
        "status": {"bsonType": "string"},
        "customer_name": {"bsonType": "string"},
        "customer_phone": {"bsonType": "string"},
        "customer_email": {"bsonType": "string"},
        "city": {"bsonType": "string"},
        "district": {"bsonType": "string"},
        "delivery_type": {"bsonType": "string"},
        "delivery_cost": {"bsonType": ["double", "int", "long", "decimal"]},
        "payment_method": {"bsonType": "string"},
        "payment_status": {"bsonType": "string"},
        "payment_amount": {"bsonType": ["double", "int", "long", "decimal"]},
        "currency": {"bsonType": "string"},
        "total_amount": {"bsonType": ["double", "int", "long", "decimal"]},
        "items_json": {"bsonType": "array"},
        "corrections": {"bsonType": "array"},
        "raw_record": {"bsonType": "object"},
        "source": {"bsonType": "object"},
    },
}


@dataclass
class UpsertStats:
    inserted_count: int = 0
    updated_count: int = 0
    unchanged_count: int = 0


class OrdersRepository(Protocol):
    def ensure_schema(self) -> None: ...

    def insert_raw_batch(
        self, documents: list[dict[str, Any]], request_key: str | None = None
    ) -> bool: ...

    def iter_raw(
        self, run_id: str, after_source_row: int | None = None
    ) -> Iterator[dict[str, Any]]: ...

    def upsert_validated(
        self,
        documents: Iterable[dict[str, Any]],
        request_key: str | None = None,
        version_field: str = "version",
    ) -> UpsertStats: ...

    def insert_quarantine(
        self, documents: list[dict[str, Any]], request_key: str | None = None
    ) -> bool: ...

    def get_validated(self, order_id: str) -> dict[str, Any] | None: ...


class InMemoryOrdersRepository:
    """Deterministic repository used by tests and --dry-run demonstrations."""

    def __init__(self) -> None:
        self.orders_raw: list[dict[str, Any]] = []
        self.orders_validated: dict[str, dict[str, Any]] = {}
        self.orders_quarantine: list[dict[str, Any]] = []
        self._idempotency_keys: dict[str, str] = {}
        self.schema_ready = False

    def ensure_schema(self) -> None:
        self.schema_ready = True

    def insert_raw_batch(
        self, documents: list[dict[str, Any]], request_key: str | None = None
    ) -> bool:
        if not self._claim_request(request_key, documents):
            return False
        self.orders_raw.extend(copy.deepcopy(documents))
        return bool(documents)

    def iter_raw(
        self, run_id: str, after_source_row: int | None = None
    ) -> Iterator[dict[str, Any]]:
        for document in self.orders_raw:
            if (
                document["run_id"] == run_id
                and (
                    after_source_row is None
                    or document["source_row_number"] > after_source_row
                )
            ):
                yield copy.deepcopy(document)

    def upsert_validated(
        self,
        documents: Iterable[dict[str, Any]],
        request_key: str | None = None,
        version_field: str = "version",
    ) -> UpsertStats:
        materialized = list(documents)
        if not self._claim_request(request_key, materialized):
            return UpsertStats(unchanged_count=len(materialized))
        stats = UpsertStats()
        for document in materialized:
            key = str(document["order_id"])
            existing = self.orders_validated.get(key)
            candidate = copy.deepcopy(document)
            if existing is None:
                stats.inserted_count += 1
            elif _incoming_version_is_not_newer(
                existing, candidate, version_field
            ):
                stats.unchanged_count += 1
                continue
            elif existing == candidate:
                stats.unchanged_count += 1
            else:
                stats.updated_count += 1
            self.orders_validated[key] = candidate
        return stats

    def insert_quarantine(
        self, documents: list[dict[str, Any]], request_key: str | None = None
    ) -> bool:
        if not self._claim_request(request_key, documents):
            return False
        self.orders_quarantine.extend(copy.deepcopy(documents))
        return bool(documents)

    def count_business_records(self) -> int:
        return len(self.orders_validated)

    def get_validated(self, order_id: str) -> dict[str, Any] | None:
        document = self.orders_validated.get(order_id)
        return copy.deepcopy(document) if document is not None else None

    def _claim_request(
        self, request_key: str | None, documents: Iterable[dict[str, Any]]
    ) -> bool:
        if request_key is None:
            return True
        payload_hash = _payload_hash(documents)
        previous_hash = self._idempotency_keys.get(request_key)
        if previous_hash is not None and previous_hash != payload_hash:
            raise ValueError(
                f"Idempotency key {request_key!r} was reused with a different payload"
            )
        self._idempotency_keys[request_key] = payload_hash
        return previous_hash is None


class MongoOrdersRepository:
    """MongoDB adapter; business writes use replace/upsert, never check-then-insert."""

    def __init__(self, uri: str, database: str) -> None:
        try:
            from pymongo import MongoClient
        except ImportError as error:
            raise RuntimeError(
                "pymongo is required for MongoDB mode; install requirements.txt"
            ) from error
        self._client = MongoClient(uri, serverSelectionTimeoutMS=5_000)
        self._db = self._client[database]

    def ensure_schema(self) -> None:
        from pymongo import ASCENDING

        existing = set(self._db.list_collection_names())
        for name in ("orders_raw", "orders_validated", "orders_quarantine"):
            if name not in existing:
                self._db.create_collection(name)
        # Raw intentionally has neither a validator nor a unique index.
        self._db.orders_raw.create_index(
            [("run_id", ASCENDING), ("source_row_number", ASCENDING)]
        )
        self._db.orders_validated.create_index(
            [("order_id", ASCENDING)], unique=True, name="unique_order_id"
        )
        self._db.orders_quarantine.create_index(
            [("run_id", ASCENDING), ("source_row_number", ASCENDING)]
        )
        self._db.pipeline_idempotency_keys.create_index(
            [("created_at", ASCENDING)]
        )
        self._db.command(
            "collMod",
            "orders_validated",
            validator={"$jsonSchema": VALIDATED_JSON_SCHEMA},
            validationLevel="strict",
            validationAction="error",
        )

    def insert_raw_batch(
        self, documents: list[dict[str, Any]], request_key: str | None = None
    ) -> bool:
        if not documents:
            return False
        if not self._claim_request(request_key, documents):
            return False
        prepared = (
            [
                {
                    **copy.deepcopy(document),
                    "_id": _stable_document_id(request_key, index, document),
                }
                for index, document in enumerate(documents)
            ]
            if request_key is not None
            else documents
        )
        try:
            self._db.orders_raw.insert_many(prepared, ordered=False)
        except Exception as error:
            if not _only_duplicate_key_errors(error):
                raise
            logger.info("Raw batch replay contained only existing documents")
        self._complete_request(request_key)
        return True

    def iter_raw(
        self, run_id: str, after_source_row: int | None = None
    ) -> Iterator[dict[str, Any]]:
        query: dict[str, Any] = {"run_id": run_id}
        if after_source_row is not None:
            query["source_row_number"] = {"$gt": after_source_row}
        yield from self._db.orders_raw.find(query).sort("source_row_number", 1)

    def upsert_validated(
        self,
        documents: Iterable[dict[str, Any]],
        request_key: str | None = None,
        version_field: str = "version",
    ) -> UpsertStats:
        from pymongo import UpdateOne

        materialized = list(documents)
        if not materialized:
            return UpsertStats()
        if not self._claim_request(request_key, materialized):
            return UpsertStats(unchanged_count=len(materialized))
        stats = UpsertStats()
        operations: list[Any] = []
        # Reads here are only for transparent metrics. The conditional update
        # below remains the decision and write operation.
        for document in materialized:
            key = document["order_id"]
            previous = self._db.orders_validated.find_one({"order_id": key})
            if previous is None:
                stats.inserted_count += 1
            elif _incoming_version_is_not_newer(
                previous, document, version_field
            ):
                stats.unchanged_count += 1
            elif _without_mongo_id(previous) == _without_mongo_id(document):
                stats.unchanged_count += 1
            else:
                stats.updated_count += 1
            incoming_version = _as_int(document.get(version_field))
            if incoming_version is None:
                version_condition: dict[str, Any] = {
                    "$eq": [{"$ifNull": [f"${version_field}", None]}, None]
                }
            else:
                version_condition = {
                    "$lte": [
                        {"$ifNull": [f"${version_field}", -1]},
                        incoming_version,
                    ]
                }
            # The conditional replacement is evaluated by MongoDB against the
            # current document, so an older or unversioned concurrent writer
            # cannot overwrite a newer version.
            operations.append(
                UpdateOne(
                    {"order_id": key},
                    [
                        {
                            "$replaceWith": {
                                "$cond": [
                                    version_condition,
                                    {"$literal": document},
                                    "$$ROOT",
                                ]
                            }
                        }
                    ],
                    upsert=True,
                )
            )
        self._db.orders_validated.bulk_write(operations, ordered=False)
        self._complete_request(request_key)
        return stats

    def insert_quarantine(
        self, documents: list[dict[str, Any]], request_key: str | None = None
    ) -> bool:
        if not documents:
            return False
        if not self._claim_request(request_key, documents):
            return False
        prepared = (
            [
                {
                    **copy.deepcopy(document),
                    "_id": _stable_document_id(request_key, index, document),
                }
                for index, document in enumerate(documents)
            ]
            if request_key is not None
            else documents
        )
        try:
            self._db.orders_quarantine.insert_many(prepared, ordered=False)
        except Exception as error:
            if not _only_duplicate_key_errors(error):
                raise
            logger.info("Quarantine batch replay contained existing documents")
        self._complete_request(request_key)
        return True

    def get_validated(self, order_id: str) -> dict[str, Any] | None:
        return self._db.orders_validated.find_one({"order_id": order_id})

    def aggregate_quality_errors(
        self, run_id: str | None = None
    ) -> list[dict[str, Any]]:
        from src.mongo_aggregation import quality_error_counts_pipeline

        return list(
            self._db.orders_quarantine.aggregate(
                quality_error_counts_pipeline(run_id)
            )
        )

    def aggregate_validated_statuses(self) -> list[dict[str, Any]]:
        from src.mongo_aggregation import validated_status_counts_pipeline

        return list(
            self._db.orders_validated.aggregate(
                validated_status_counts_pipeline()
            )
        )

    def aggregate_latest_orders(
        self, version_field: str = "version"
    ) -> list[dict[str, Any]]:
        from src.mongo_aggregation import latest_version_per_order_pipeline

        return list(
            self._db.orders_validated.aggregate(
                latest_version_per_order_pipeline(version_field)
            )
        )

    def close(self) -> None:
        self._client.close()

    def _claim_request(
        self, request_key: str | None, documents: Iterable[dict[str, Any]]
    ) -> bool:
        if request_key is None:
            return True
        from pymongo.errors import DuplicateKeyError

        payload_hash = _payload_hash(documents)
        collection = self._db.pipeline_idempotency_keys
        try:
            collection.insert_one(
                {
                    "_id": request_key,
                    "payload_hash": payload_hash,
                    "status": "processing",
                }
            )
            return True
        except DuplicateKeyError:
            existing = collection.find_one({"_id": request_key})
            if existing and existing.get("payload_hash") != payload_hash:
                raise ValueError(
                    f"Idempotency key {request_key!r} was reused with a different payload"
                )
            # A request left in processing is safe to retry because all
            # underlying writes are themselves upserts/deterministic inserts.
            return bool(existing and existing.get("status") != "completed")

    def _complete_request(self, request_key: str | None) -> None:
        if request_key is not None:
            from datetime import datetime, timezone

            self._db.pipeline_idempotency_keys.update_one(
                {"_id": request_key},
                {
                    "$set": {
                        "status": "completed",
                        "completed_at": datetime.now(timezone.utc),
                    }
                },
            )

def _without_mongo_id(document: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in document.items() if key != "_id"}


def _payload_hash(documents: Iterable[dict[str, Any]]) -> str:
    canonical_documents = [
        _canonicalize_for_idempotency(document) for document in documents
    ]
    encoded = json.dumps(
        canonical_documents,
        sort_keys=True,
        ensure_ascii=False,
        default=str,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _canonicalize_for_idempotency(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _canonicalize_for_idempotency(item)
            for key, item in value.items()
            if key not in {"ingested_at"}
        }
    if isinstance(value, list):
        return [_canonicalize_for_idempotency(item) for item in value]
    return value


def _stable_document_id(
    request_key: str | None, index: int, document: dict[str, Any]
) -> str:
    identity = (
        f"{request_key or 'unkeyed'}|{index}|"
        f"{document.get('run_id', '')}|{document.get('source_row_number', '')}"
    )
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def _as_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None and str(value).strip() else None
    except (TypeError, ValueError):
        return None


def _incoming_version_is_not_newer(
    existing: dict[str, Any], incoming: dict[str, Any], version_field: str
) -> bool:
    incoming_version = _as_int(incoming.get(version_field))
    existing_version = _as_int(existing.get(version_field))
    if incoming_version is None and existing_version is not None:
        return True
    return (
        incoming_version is not None
        and existing_version is not None
        and incoming_version <= existing_version
    )


def _only_duplicate_key_errors(error: Exception) -> bool:
    details = getattr(error, "details", None)
    write_errors = details.get("writeErrors", []) if isinstance(details, dict) else []
    return bool(write_errors) and all(
        entry.get("code") == 11000 for entry in write_errors
    )
