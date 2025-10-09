"""FastAPI application that powers the Contifico inventory dashboard."""
from __future__ import annotations

import logging
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import ValidationError
from pydantic_settings import BaseSettings

from ..contifico_client import ContificoClient
from ..ingestion.sync_inventory import synchronise_inventory
from ..persistence import InventoryRepository
from ..logging_config import configure_logging

load_dotenv()

BASE_PATH = Path(__file__).parent
TEMPLATES = Jinja2Templates(directory=str(BASE_PATH / "templates"))
STATIC_DIR = BASE_PATH / "static"

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    """Application configuration sourced from environment variables."""

    contifico_api_key: str
    contifico_api_token: str
    contifico_api_base_url: str = "https://api.contifico.com/sistema/api/v1"
    inventory_db_path: str = "data/inventory.db"
    sync_batch_size: int = 100
    contifico_page_size: int = 200
    log_level: str = "INFO"
    log_file: str | None = None

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

app = FastAPI(
    title="Inventario Contifico",
    description="Panel web para monitorear el inventario sincronizado desde Contifico.",
    version="0.1.0",
)

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

RESOURCE_LABEL_OVERRIDES = {
    "categories": "Categorías",
    "brands": "Marcas",
    "variants": "Variantes",
    "products": "Productos",
    "warehouses": "Bodegas",
    "inventory_movements": "Movimientos de Inventario",
    "remission_guides": "Guías de Remisión",
    "purchases": "Compras",
    "sales": "Ventas",
    "documents": "Documentos",
    "registry_transactions": "Transacciones de Registro",
    "persons": "Personas",
    "cost_centers": "Centros de Costo",
    "chart_of_accounts": "Plan de Cuentas",
    "journal_entries": "Asientos Contables",
    "bank_accounts": "Cuentas Bancarias",
    "bank_movements": "Movimientos Bancarios",
}

RESOURCE_LABELS = {
    slug: RESOURCE_LABEL_OVERRIDES.get(slug, slug.replace("_", " ").title())
    for slug in InventoryRepository.RESOURCES
}

UPCOMING_FEATURES = (
    {
        "title": "Indicadores de rotación de inventario",
        "description": "Análisis de rotación y cobertura utilizando los movimientos sincronizados.",
    },
    {
        "title": "KPIs de ventas",
        "description": "Tableros comparativos por período, sucursal y categoría de producto.",
    },
    {
        "title": "Alertas de stock",
        "description": "Notificaciones para niveles críticos por bodega en base a mínimos configurables.",
    },
)


@lru_cache()
def get_settings() -> Settings:
    """Return cached application settings."""

    try:
        return Settings()
    except ValidationError as exc:  # pragma: no cover - defensive guard for runtime
        missing = {err["loc"][0] for err in exc.errors() if err["type"] == "value_error.missing"}
        raise RuntimeError(
            "Faltan variables de entorno requeridas: " + ", ".join(sorted(missing))
        ) from exc


@lru_cache(maxsize=1)
def get_repository() -> InventoryRepository:
    """Initialise (and cache) the repository according to the configured DB path."""

    settings = get_settings()
    return InventoryRepository(settings.inventory_db_path)


def build_client(settings: Settings) -> ContificoClient:
    """Instantiate a Contifico client using the configured credentials."""

    return ContificoClient(
        api_key=settings.contifico_api_key,
        api_token=settings.contifico_api_token,
        base_url=settings.contifico_api_base_url,
        default_page_size=settings.contifico_page_size,
    )


@app.get("/", response_class=HTMLResponse)
def dashboard(
    request: Request, repo: InventoryRepository = Depends(get_repository)
) -> HTMLResponse:
    """Render the main dashboard with aggregated inventory information."""

    overview = repo.get_resource_overview()
    resources = [
        {
            "slug": slug,
            "label": RESOURCE_LABELS.get(slug, slug.replace("_", " ").title()),
            "count": data.get("count", 0),
            "last_updated": data.get("last_updated"),
            "last_fetched": data.get("last_fetched"),
            "last_synced": data.get("last_synced"),
        }
        for slug, data in overview.items()
    ]
    has_data = any(resource["count"] for resource in resources)
    resource_options = [
        {
            "slug": slug,
            "label": RESOURCE_LABELS.get(slug, slug.replace("_", " ").title()),
        }
        for slug in overview.keys()
    ]

    return TEMPLATES.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "resources": resources,
            "has_data": has_data,
            "upcoming": UPCOMING_FEATURES,
            "current_year": datetime.utcnow().year,
            "resource_options": resource_options,
        },
    )


@app.get("/api/overview")
def api_overview(repo: InventoryRepository = Depends(get_repository)) -> dict[str, list[dict[str, Any]]]:
    """Expose a machine-friendly snapshot of the stored resources."""

    overview = repo.get_resource_overview()
    payload: list[dict[str, Any]] = [
        {
            "resource": slug,
            "label": RESOURCE_LABELS.get(slug, slug.replace("_", " ").title()),
            **data,
        }
        for slug, data in overview.items()
    ]
    return {"resources": payload}


@app.get("/api/resource/{resource_slug}")
def api_search_resource(
    resource_slug: str,
    q: str | None = None,
    limit: int = Query(default=20, ge=1, le=100),
    repo: InventoryRepository = Depends(get_repository),
) -> dict[str, Any]:
    """Search downloaded records for a given resource."""

    try:
        results = repo.search_records(resource_slug, query=q, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return {
        "resource": resource_slug,
        "label": RESOURCE_LABELS.get(
            resource_slug, resource_slug.replace("_", " ").title()
        ),
        "query": q,
        "results": results,
    }


@app.get("/api/resource/{resource_slug}/item/{record_id}")
def api_get_resource_item(
    resource_slug: str,
    record_id: str,
    repo: InventoryRepository = Depends(get_repository),
) -> dict[str, Any]:
    """Return a specific record stored locally for validation."""

    try:
        record = repo.get_record(resource_slug, record_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    if not record:
        raise HTTPException(status_code=404, detail="Registro no encontrado")

    return {
        "resource": resource_slug,
        "label": RESOURCE_LABELS.get(
            resource_slug, resource_slug.replace("_", " ").title()
        ),
        "record": record,
    }


@app.post("/api/sync", status_code=202)
def api_trigger_sync(
    background: BackgroundTasks,
    since: str | None = None,
    resources: list[str] | None = Query(default=None),
    full_refresh: bool = False,
) -> dict[str, Any]:
    """Kick off a background sync cycle using the configured credentials."""

    settings = get_settings()
    try:
        since_dt = datetime.fromisoformat(since) if since else None
    except ValueError as exc:  # pragma: no cover - FastAPI handles validation
        raise HTTPException(status_code=400, detail="Formato de fecha inválido") from exc

    selected_resources = [r for r in resources or [] if r]
    invalid = sorted(set(selected_resources) - set(RESOURCE_LABELS.keys()))
    if invalid:
        raise HTTPException(
            status_code=400,
            detail=f"Recursos inválidos solicitados: {', '.join(invalid)}",
        )

    def _run_sync() -> None:
        repo = InventoryRepository(settings.inventory_db_path)
        client = build_client(settings)
        try:
            totals = synchronise_inventory(
                repo,
                client,
                since=since_dt,
                batch_size=settings.sync_batch_size,
                resources=selected_resources or None,
                full_refresh=full_refresh,
                page_size=settings.contifico_page_size,
            )
            logger.info("Sincronización completada: %s", totals)
        except Exception:  # pragma: no cover - runtime safeguard
            logger.exception("Falló la sincronización de inventario")

    background.add_task(_run_sync)
    return {
        "detail": "Sincronización en curso",
        "since": since,
        "batch_size": settings.sync_batch_size,
        "resources": selected_resources or "all",
        "full_refresh": full_refresh,
        "page_size": settings.contifico_page_size,
    }
@app.on_event("startup")
def configure_app_logging() -> None:
    """Inicializa el logging global según las variables de entorno."""

    settings = get_settings()
    configure_logging(settings.log_level, settings.log_file)
    logger.debug("Logging configurado para la aplicación web")

