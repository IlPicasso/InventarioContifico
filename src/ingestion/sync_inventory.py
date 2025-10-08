"""Synchronise Contifico data into the local persistence layer."""
from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timezone
from typing import Callable, Dict, Iterable

from dotenv import load_dotenv

from ..contifico_client import ContificoClient
from ..persistence import InventoryRepository, chunked

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ENDPOINTS: Dict[str, Callable[[ContificoClient, datetime | None], Iterable[dict]]]
ENDPOINTS = {
    "products": lambda client, since: client.iter_products(updated_since=since),
    "purchases": lambda client, since: client.iter_purchases(updated_since=since),
    "sales": lambda client, since: client.iter_sales(updated_since=since),
    "warehouses": lambda client, since: client.iter_warehouses(updated_since=since),
}


def synchronise_inventory(
    repo: InventoryRepository,
    client: ContificoClient,
    *,
    since: datetime | None = None,
    batch_size: int = 100,
) -> dict[str, int]:
    """Run a full sync cycle for every configured resource."""

    totals: dict[str, int] = {}
    for endpoint, fetcher in ENDPOINTS.items():
        logger.info("Syncing %s", endpoint)
        last_synced = since or repo.get_last_synced_at(endpoint)
        total = 0

        records = fetcher(client, last_synced)
        for batch in chunked(records, batch_size):
            total += repo.upsert_records(endpoint, batch)

        repo.update_last_synced_at(endpoint, datetime.now(timezone.utc))
        totals[endpoint] = total
        logger.info("%s sync complete: %s records", endpoint, total)

    return totals


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--since",
        help="ISO8601 timestamp to force as starting point",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of records to persist per transaction",
    )
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()

    api_key = os.getenv("CONTIFICO_API_KEY")
    api_token = os.getenv("CONTIFICO_API_TOKEN")
    if not api_key:
        raise RuntimeError("CONTIFICO_API_KEY is not defined")
    if not api_token:
        raise RuntimeError("CONTIFICO_API_TOKEN is not defined")

    base_url = os.getenv(
        "CONTIFICO_API_BASE_URL", "https://api.contifico.com/sistema/api/v1"
    )
    db_path = os.getenv("INVENTORY_DB_PATH", "data/inventory.db")

    client = ContificoClient(api_key=api_key, api_token=api_token, base_url=base_url)
    repo = InventoryRepository(db_path)

    forced_since = datetime.fromisoformat(args.since) if args.since else None

    synchronise_inventory(repo, client, since=forced_since, batch_size=args.batch_size)


if __name__ == "__main__":
    main()
