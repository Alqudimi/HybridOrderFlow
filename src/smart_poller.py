"""Smart polling and durable source-processing state.

The poller is intentionally separate from the ELT transform. It decides
whether a source is new before an expensive load starts.
"""

from __future__ import annotations

import csv
import hashlib
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FileSnapshot:
    source_key: str
    size_bytes: int
    modified_ns: int
    content_hash: str
    max_version: int | None


@dataclass(frozen=True)
class PollDecision:
    should_process: bool
    reason: str
    snapshot: FileSnapshot
    claim_id: str | None = None
    checkpoint: dict[str, Any] | None = None


class PollingStateStore(Protocol):
    def get_source_state(self, source_key: str) -> dict[str, Any] | None: ...

    def claim(
        self, snapshot: FileSnapshot, lease_seconds: int
    ) -> str | None: ...

    def get_checkpoint(self, snapshot: FileSnapshot) -> dict[str, Any] | None: ...

    def mark_progress(
        self, snapshot: FileSnapshot, checkpoint: dict[str, Any]
    ) -> None: ...

    def mark_success(self, snapshot: FileSnapshot) -> None: ...

    def close(self) -> None: ...


class InMemoryPollingStateStore:
    def __init__(self) -> None:
        self.states: dict[str, dict[str, Any]] = {}
        self.claims: dict[str, dict[str, Any]] = {}

    def get_source_state(self, source_key: str) -> dict[str, Any] | None:
        return self.states.get(source_key)

    def claim(self, snapshot: FileSnapshot, lease_seconds: int) -> str | None:
        claim_id = _claim_id(snapshot)
        now = datetime.now(timezone.utc)
        existing = self.claims.get(claim_id)
        if existing is not None:
            if existing["status"] == "succeeded":
                return None
            claimed_at = existing["claimed_at"]
            if claimed_at > now - timedelta(seconds=lease_seconds):
                return None
            existing["claimed_at"] = now
            return claim_id
        self.claims[claim_id] = {
            "status": "processing",
            "claimed_at": now,
            "checkpoint": None,
        }
        return claim_id

    def get_checkpoint(self, snapshot: FileSnapshot) -> dict[str, Any] | None:
        claim = self.claims.get(_claim_id(snapshot))
        checkpoint = claim.get("checkpoint") if claim else None
        return dict(checkpoint) if checkpoint else None

    def mark_progress(
        self, snapshot: FileSnapshot, checkpoint: dict[str, Any]
    ) -> None:
        claim = self.claims.get(_claim_id(snapshot))
        if claim is not None:
            claim["checkpoint"] = dict(checkpoint)
            claim["claimed_at"] = datetime.now(timezone.utc)

    def mark_success(self, snapshot: FileSnapshot) -> None:
        self.states[snapshot.source_key] = {
            "last_content_hash": snapshot.content_hash,
            "last_size_bytes": snapshot.size_bytes,
            "last_modified_ns": snapshot.modified_ns,
            "last_version": snapshot.max_version,
            "status": "succeeded",
        }
        claim = self.claims.get(_claim_id(snapshot))
        if claim is not None:
            claim["status"] = "succeeded"

    def close(self) -> None:
        return None


class MongoPollingStateStore:
    """Durable Mongo state with a unique claim per source content/version."""

    def __init__(self, uri: str, database: str) -> None:
        try:
            from pymongo import MongoClient
        except ImportError as error:
            raise RuntimeError("pymongo is required for smart polling") from error
        self._client = MongoClient(uri, serverSelectionTimeoutMS=5_000)
        self._db = self._client[database]
        self._state = self._db.pipeline_source_state
        self._claims = self._db.pipeline_processing_claims
        self._state.create_index("source_key", unique=True)
        self._claims.create_index("source_key")
        self._claims.create_index("status")

    def get_source_state(self, source_key: str) -> dict[str, Any] | None:
        return self._state.find_one({"source_key": source_key})

    def claim(self, snapshot: FileSnapshot, lease_seconds: int) -> str | None:
        from pymongo.errors import DuplicateKeyError

        claim_id = _claim_id(snapshot)
        now = datetime.now(timezone.utc)
        try:
            self._claims.insert_one(
                {
                    "_id": claim_id,
                    "source_key": snapshot.source_key,
                    "content_hash": snapshot.content_hash,
                    "max_version": snapshot.max_version,
                    "status": "processing",
                    "claimed_at": now,
                }
            )
        except DuplicateKeyError:
            existing = self._claims.find_one({"_id": claim_id})
            if not existing or existing.get("status") == "succeeded":
                return None
            cutoff = now - timedelta(seconds=lease_seconds)
            reclaimed = self._claims.update_one(
                {
                    "_id": claim_id,
                    "status": "processing",
                    "claimed_at": {"$lte": cutoff},
                },
                {"$set": {"claimed_at": now}},
            )
            return claim_id if reclaimed.modified_count == 1 else None
        return claim_id

    def get_checkpoint(self, snapshot: FileSnapshot) -> dict[str, Any] | None:
        claim = self._claims.find_one(
            {"_id": _claim_id(snapshot)}, {"checkpoint": 1}
        )
        checkpoint = claim.get("checkpoint") if claim else None
        return dict(checkpoint) if checkpoint else None

    def mark_progress(
        self, snapshot: FileSnapshot, checkpoint: dict[str, Any]
    ) -> None:
        self._claims.update_one(
            {"_id": _claim_id(snapshot), "status": "processing"},
            {
                "$set": {
                    "checkpoint": checkpoint,
                    "claimed_at": datetime.now(timezone.utc),
                }
            },
        )

    def mark_success(self, snapshot: FileSnapshot) -> None:
        claim_id = _claim_id(snapshot)
        self._claims.update_one(
            {"_id": claim_id},
            {
                "$set": {
                    "status": "succeeded",
                    "completed_at": datetime.now(timezone.utc),
                }
            },
        )
        self._state.update_one(
            {"source_key": snapshot.source_key},
            {
                "$set": {
                    "last_content_hash": snapshot.content_hash,
                    "last_size_bytes": snapshot.size_bytes,
                    "last_modified_ns": snapshot.modified_ns,
                    "last_version": snapshot.max_version,
                    "status": "succeeded",
                    "updated_at": datetime.now(timezone.utc),
                }
            },
            upsert=True,
        )

    def close(self) -> None:
        self._client.close()


class SmartPoller:
    def __init__(
        self,
        state_store: PollingStateStore,
        version_field: str = "version",
        lease_seconds: int = 300,
    ) -> None:
        self.state_store = state_store
        self.version_field = version_field
        self.lease_seconds = lease_seconds

    def poll(self, path: str | Path) -> PollDecision:
        source = Path(path)
        if not source.is_file():
            raise FileNotFoundError(f"Polling source does not exist: {source}")
        source_key = str(source.resolve())
        stat = source.stat()
        previous = self.state_store.get_source_state(source_key)
        if previous and previous.get("status") == "succeeded" and (
            previous.get("last_size_bytes") == stat.st_size
            and previous.get("last_modified_ns") == stat.st_mtime_ns
        ):
            snapshot = _snapshot(
                source, stat, previous.get("last_version"), self.version_field
            )
            if snapshot.content_hash == previous.get("last_content_hash"):
                return PollDecision(
                    False,
                    "source size and modification time are unchanged",
                    snapshot,
                )

        snapshot = _snapshot(source, stat, None, self.version_field)
        if (
            previous
            and snapshot.max_version is not None
            and previous.get("last_version") is not None
            and snapshot.max_version <= previous["last_version"]
        ):
            return PollDecision(
                False,
                (
                    f"source max {self.version_field}={snapshot.max_version} is "
                    f"not newer than watermark={previous['last_version']}"
                ),
                snapshot,
            )
        claim_id = self.state_store.claim(snapshot, self.lease_seconds)
        if claim_id is None:
            return PollDecision(
                False,
                "this source fingerprint/version is already claimed or processed",
                snapshot,
            )
        checkpoint = self.state_store.get_checkpoint(snapshot)
        if checkpoint:
            reason = (
                "resuming source from checkpoint after an interrupted run"
            )
        else:
            reason = "new source fingerprint/version detected"
        return PollDecision(
            True, reason, snapshot, claim_id, checkpoint
        )

    def mark_progress(
        self,
        decision: PollDecision,
        source_row_number: int,
        counters: dict[str, Any],
    ) -> None:
        if decision.should_process:
            self.state_store.mark_progress(
                decision.snapshot,
                {
                    "last_source_row_number": source_row_number,
                    **counters,
                },
            )

    def mark_success(self, decision: PollDecision) -> None:
        if decision.should_process:
            self.state_store.mark_success(decision.snapshot)


def _snapshot(
    source: Path,
    stat: os.stat_result,
    fallback_version: int | None,
    version_field: str,
) -> FileSnapshot:
    digest = hashlib.sha256()
    max_version: int | None = fallback_version
    with source.open("rb") as binary:
        for chunk in iter(lambda: binary.read(1024 * 1024), b""):
            digest.update(chunk)
    try:
        with source.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            values = [
                _as_int(row.get(version_field))
                for row in reader
                if row.get(version_field) not in (None, "")
            ]
            if values:
                max_version = max(values)
    except (UnicodeDecodeError, csv.Error):
        logger.warning("Unable to scan version watermark for %s", source)
    return FileSnapshot(
        source_key=str(source.resolve()),
        size_bytes=stat.st_size,
        modified_ns=stat.st_mtime_ns,
        content_hash=digest.hexdigest(),
        max_version=max_version,
    )


def _claim_id(snapshot: FileSnapshot) -> str:
    value = (
        f"{snapshot.source_key}|{snapshot.content_hash}|"
        f"{snapshot.max_version if snapshot.max_version is not None else ''}"
    )
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _as_int(value: Any) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None
