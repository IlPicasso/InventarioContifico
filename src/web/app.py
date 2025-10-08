"""FastAPI application that powers the Contifico inventory dashboard."""
from __future__ import annotations

import os
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from ..persistence import InventoryRepository

load_dotenv()

BASE_PATH = Path(__file__).parent
TEMPLATES = Jinja2Templates(directory=str(BASE_PATH / "templates"))
STATIC_DIR = BASE_PATH / "static"

app = FastAPI(
    title="Inventario Contifico",
    description="Panel web para monitorear el inventario sincronizado desde Contifico.",
    version="0.1.0",
)

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

RESOURCE_LABELS = {
    "products": "Productos",
    "purchases": "Compras",
    "sales": "Ventas",
    "warehouses": "Bodegas",
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


@lru_cache(maxsize=1)
def get_repository() -> InventoryRepository:
    """Initialise (and cache) the repository according to the configured DB path."""

    db_path = os.getenv("INVENTORY_DB_PATH", "data/inventory.db")
    return InventoryRepository(db_path)


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

    return TEMPLATES.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "resources": resources,
            "has_data": has_data,
            "upcoming": UPCOMING_FEATURES,
            "current_year": datetime.utcnow().year,
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
