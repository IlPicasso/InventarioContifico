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

CREATE TABLE IF NOT EXISTS categories (
    id TEXT PRIMARY KEY,
    data TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    fetched_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS brands (
    id TEXT PRIMARY KEY,
    data TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    fetched_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS variants (
    id TEXT PRIMARY KEY,
    data TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    fetched_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
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

CREATE TABLE IF NOT EXISTS inventory_movements (
    id TEXT PRIMARY KEY,
    data TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    fetched_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS remission_guides (
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

CREATE TABLE IF NOT EXISTS documents (
    id TEXT PRIMARY KEY,
    data TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    fetched_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS registry_transactions (
    id TEXT PRIMARY KEY,
    data TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    fetched_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS persons (
    id TEXT PRIMARY KEY,
    data TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    fetched_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cost_centers (
    id TEXT PRIMARY KEY,
    data TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    fetched_at TEXT NOT NULL
);

"""


class InventoryRepository:
    """Simple SQLite-backed repository for inventory data."""

    RESOURCES = (
        "categories",
        "brands",
        "variants",
        "products",
        "warehouses",
        "inventory_movements",
        "remission_guides",
        "purchases",
        "sales",
        "documents",
        "registry_transactions",
        "persons",
        "cost_centers",
    )

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
        timestamp_fields: Sequence[str] | None = None,
    ) -> int:
        table = endpoint
        now = datetime.utcnow().isoformat()
        timestamp_fields = (
            tuple(timestamp_fields)
            if timestamp_fields
            else (
                "updated_at",
                "fecha_modificacion",
                "fecha",
                "fecha_emision",
                "created_at",
            )
        )
        rows = 0
        with self._connection() as conn:
            for record in records:
                record_id = str(record.get(record_id_field))
                if record_id is None:
                    continue
                updated_at = None
                for field in timestamp_fields:
                    value = record.get(field)
                    if value:
                        updated_at = value
                        break
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

        resources = list(self.RESOURCES)
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

    def _validate_resource(self, resource: str) -> str:
        if resource not in self.RESOURCES:
            raise ValueError(f"Recurso desconocido: {resource}")
        return resource

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

    def search_records(
        self,
        resource: str,
        query: str | None = None,
        *,
        limit: int = 20,
    ) -> list[dict[str, Optional[str] | dict]]:
        """Return locally stored records for ``resource`` matching ``query``.

        The search checks both the identifier and the JSON payload. When no query
        is provided the latest records are returned so operators can confirm that
        synchronisation succeeded.
        """

        resource = self._validate_resource(resource)
        limit = max(1, min(int(limit), 100))
        cleaned_query = query.strip() if query else None
        like_pattern = f"%{cleaned_query}%" if cleaned_query else None

        with self._connection() as conn:
            if cleaned_query:
                rows = conn.execute(
                    f"""
                    SELECT id, data, updated_at, fetched_at
                    FROM {resource}
                    WHERE id = ? OR data LIKE ?
                    ORDER BY fetched_at DESC
                    LIMIT ?
                    """,
                    (cleaned_query, like_pattern, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    f"""
                    SELECT id, data, updated_at, fetched_at
                    FROM {resource}
                    ORDER BY fetched_at DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()

        results: list[dict[str, Optional[str] | dict]] = []
        for row in rows or []:
            payload = json.loads(row["data"]) if row["data"] else None
            results.append(
                {
                    "id": row["id"],
                    "data": payload,
                    "updated_at": row["updated_at"],
                    "fetched_at": row["fetched_at"],
                }
            )
        return results

    def get_record(self, resource: str, record_id: str) -> dict | None:
        """Return a single stored record for ``resource`` by its identifier."""

        resource = self._validate_resource(resource)
        record_id = record_id.strip()
        if not record_id:
            return None

        with self._connection() as conn:
            row = conn.execute(
                f"""
                SELECT id, data, updated_at, fetched_at
                FROM {resource}
                WHERE id = ?
                LIMIT 1
                """,
                (record_id,),
            ).fetchone()

        if not row:
            return None
        payload = json.loads(row["data"]) if row["data"] else None
        return {
            "id": row["id"],
            "data": payload,
            "updated_at": row["updated_at"],
            "fetched_at": row["fetched_at"],
        }


def chunked(iterable: Iterable[dict], size: int) -> Iterator[Sequence[dict]]:
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch
