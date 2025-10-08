"""Persistence helpers for the inventory ingestion pipeline."""
from __future__ import annotations

import json
import sqlite3
from collections import OrderedDict
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterable, Iterator, Optional, Sequence

SCHEMA = """
CREATE TABLE IF NOT EXISTS sync_state (
    endpoint TEXT PRIMARY KEY,
    last_synced_at TEXT
);

CREATE TABLE IF NOT EXISTS products (
    id TEXT PRIMARY KEY,
    data TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    fetched_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS purchases (
    id TEXT PRIMARY KEY,
    data TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    fetched_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sales (
    id TEXT PRIMARY KEY,
    data TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    fetched_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouses (
    id TEXT PRIMARY KEY,
    data TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    fetched_at TEXT NOT NULL
);
"""


class InventoryRepository:
    """Simple SQLite-backed repository for inventory data."""

    def __init__(self, db_path: Path | str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connection() as conn:
            conn.executescript(SCHEMA)

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def upsert_records(
        self,
        endpoint: str,
        records: Iterable[dict],
        record_id_field: str = "id",
    ) -> int:
        table = endpoint
        now = datetime.utcnow().isoformat()
        rows = 0
        with self._connection() as conn:
            for record in records:
                record_id = str(record.get(record_id_field))
                if record_id is None:
                    continue
                updated_at = record.get("updated_at") or record.get("fecha_modificacion")
                conn.execute(
                    f"""
                    INSERT INTO {table} (id, data, updated_at, fetched_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        data=excluded.data,
                        updated_at=excluded.updated_at,
                        fetched_at=excluded.fetched_at
                    """,
                    (
                        record_id,
                        json.dumps(record, ensure_ascii=False),
                        updated_at or now,
                        now,
                    ),
                )
                rows += 1
        return rows

    def get_resource_overview(self) -> OrderedDict[str, dict[str, Optional[str] | int]]:
        """Return aggregated information per resource table.

        The overview includes the number of stored records, the latest update timestamp
        reported by Contifico, when the record was fetched locally, and the last
        synchronisation timestamp stored in ``sync_state``.
        """

        resources = ["products", "purchases", "sales", "warehouses"]
        overview: OrderedDict[str, dict[str, Optional[str] | int]] = OrderedDict()

        with self._connection() as conn:
            sync_state = {
                row["endpoint"]: row["last_synced_at"]
                for row in conn.execute("SELECT endpoint, last_synced_at FROM sync_state")
            }

            for resource in resources:
                row = conn.execute(
                    f"""
                    SELECT
                        COUNT(*) AS count,
                        MAX(updated_at) AS last_updated,
                        MAX(fetched_at) AS last_fetched
                    FROM {resource}
                    """
                ).fetchone()

                overview[resource] = {
                    "count": int(row["count"]) if row and row["count"] is not None else 0,
                    "last_updated": row["last_updated"] if row else None,
                    "last_fetched": row["last_fetched"] if row else None,
                    "last_synced": sync_state.get(resource),
                }

        return overview

    def get_last_synced_at(self, endpoint: str) -> Optional[datetime]:
        with self._connection() as conn:
            cur = conn.execute(
                "SELECT last_synced_at FROM sync_state WHERE endpoint = ?", (endpoint,)
            )
            row = cur.fetchone()
            if row and row["last_synced_at"]:
                return datetime.fromisoformat(row["last_synced_at"])
        return None

    def update_last_synced_at(self, endpoint: str, value: datetime) -> None:
        with self._connection() as conn:
            conn.execute(
                """
                INSERT INTO sync_state (endpoint, last_synced_at)
                VALUES (?, ?)
                ON CONFLICT(endpoint) DO UPDATE SET last_synced_at=excluded.last_synced_at
                """,
                (endpoint, value.isoformat()),
            )


def chunked(iterable: Iterable[dict], size: int) -> Iterator[Sequence[dict]]:
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch
