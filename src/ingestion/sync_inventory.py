"""Synchronise Contifico data into the local persistence layer."""
from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timezone
from typing import Callable, Dict, Iterable, Sequence

from dotenv import load_dotenv

from ..contifico_client import ContificoClient
from ..persistence import InventoryRepository, chunked
from ..logging_config import configure_logging

logger = logging.getLogger(__name__)

ResourceFetcher = Callable[[ContificoClient, datetime | None, int | None], Iterable[dict]]

ENDPOINTS: Dict[str, ResourceFetcher] = {
    "categories": lambda client, since, page_size: client.iter_categories(
        updated_since=since, page_size=page_size
    ),
    "brands": lambda client, since, page_size: client.iter_brands(
        updated_since=since, page_size=page_size
    ),
    "variants": lambda client, since, page_size: client.iter_variants(
        updated_since=since, page_size=page_size
    ),
    "products": lambda client, since, page_size: client.iter_products(
        updated_since=since, page_size=page_size
    ),
    "warehouses": lambda client, since, page_size: client.iter_warehouses(
        updated_since=since, page_size=page_size
    ),
    "remission_guides": lambda client, since, page_size: client.iter_remission_guides(
        updated_since=since, page_size=page_size
    ),
    "purchases": lambda client, since, page_size: client.iter_purchases(
        updated_since=since, page_size=page_size
    ),
    "sales": lambda client, since, page_size: client.iter_sales(
        updated_since=since, page_size=page_size
    ),
    "documents": lambda client, since, page_size: client.iter_documents_catalog(
        updated_since=since, page_size=page_size
    ),
    "registry_transactions": lambda client, since, page_size: client.iter_registry_transactions(
        updated_since=since, page_size=page_size
    ),
    "persons": lambda client, since, page_size: client.iter_persons(
        updated_since=since, page_size=page_size
    ),
    "cost_centers": lambda client, since, page_size: client.iter_cost_centers(
        updated_since=since, page_size=page_size
    ),
}


def synchronise_inventory(
    repo: InventoryRepository,
    client: ContificoClient,
    *,
    since: datetime | None = None,
    batch_size: int = 100,
    resources: Sequence[str] | None = None,
    full_refresh: bool = False,
    page_size: int | None = None,
) -> dict[str, int]:
    """Run a full sync cycle for every configured resource."""

    selected = list(resources) if resources else list(ENDPOINTS.keys())
    unknown = sorted(set(selected) - ENDPOINTS.keys())
    if unknown:
        raise ValueError(f"Recursos desconocidos solicitados: {', '.join(unknown)}")

    totals: dict[str, int] = {}
    for endpoint in selected:
        fetcher = ENDPOINTS[endpoint]
        logger.info("Syncing %s", endpoint)
        last_synced = None if full_refresh else (since or repo.get_last_synced_at(endpoint))
        total = 0

        records = fetcher(client, last_synced, page_size)
        for batch in chunked(records, batch_size):
            saved = repo.upsert_records(endpoint, batch)
            batch_size_actual = len(batch)
            total += saved
            skipped = batch_size_actual - saved
            if logger.isEnabledFor(logging.DEBUG):
                sample_ids = []
                for item in batch[:5]:
                    sample_id = None
                    for key in ("id", "codigo", "code", "uuid", "external_id"):
                        value = item.get(key)
                        if value:
                            sample_id = str(value)
                            break
                    sample_ids.append(sample_id)
                logger.debug(
                    "Persistido lote de %s (%s/%s registros). Identificadores de muestra: %s",
                    endpoint,
                    saved,
                    batch_size_actual,
                    sample_ids,
                )
            if skipped:
                logger.warning(
                    "%s registros omitidos en el lote de %s por falta de identificador.",
                    skipped,
                    endpoint,
                )

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
    parser.add_argument(
        "--resources",
        nargs="+",
        choices=sorted(ENDPOINTS.keys()),
        help="Limit the sync to a subset of resources",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Ignora el historial y vuelve a descargar todos los registros del recurso",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        help="Override the Contífico page size for API pagination",
    )
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    configure_logging(os.getenv("LOG_LEVEL", "INFO"), os.getenv("LOG_FILE"))
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

    page_size_env = os.getenv("CONTIFICO_PAGE_SIZE")
    default_page_size = int(page_size_env) if page_size_env else None

    client = ContificoClient(
        api_key=api_key,
        api_token=api_token,
        base_url=base_url,
        default_page_size=default_page_size,
    )
    repo = InventoryRepository(db_path)

    forced_since = datetime.fromisoformat(args.since) if args.since else None

    synchronise_inventory(
        repo,
        client,
        since=forced_since,
        batch_size=args.batch_size,
        resources=args.resources,
        full_refresh=args.full_refresh,
        page_size=args.page_size or default_page_size,
    )


if __name__ == "__main__":
    main()
