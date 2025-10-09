"""FastAPI application that powers the Contifico inventory dashboard."""
from __future__ import annotations

import logging
from datetime import datetime
from io import BytesIO
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from dotenv import load_dotenv
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import ValidationError
from pydantic_settings import BaseSettings

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from ..analytics import generate_inventory_report
from ..contifico_client import ContificoClient
from ..ingestion.sync_inventory import synchronise_inventory
from ..persistence import InventoryRepository
from ..logging_config import configure_logging

load_dotenv()

BASE_PATH = Path(__file__).parent
TEMPLATES = Jinja2Templates(directory=str(BASE_PATH / "templates"))
STATIC_DIR = BASE_PATH / "static"

logger = logging.getLogger(__name__)

DEFAULT_VELOCITY_PERIOD_DAYS = 30
DEFAULT_TURNOVER_PERIOD_DAYS = 90
DEFAULT_LOW_STOCK_THRESHOLD_DAYS = 14.0
DEFAULT_EXCESS_STOCK_THRESHOLD_DAYS = 60.0
DEFAULT_TOP_N = 5
ALERT_LABELS = {
    "low_stock": "Bajo stock",
    "reorder_recommended": "Requiere reposición",
    "excess_stock": "Exceso de inventario",
    "no_sales": "Sin ventas registradas",
    "no_purchases": "Sin compras registradas",
    "stagnant_stock": "Inventario estancado",
}


def _format_metric(value: Any, *, decimals: int = 2) -> str:
    """Format metric values for presentation in templates."""

    if value is None:
        return "N/D"
    if isinstance(value, (int, float)):
        if isinstance(value, float):
            formatted = f"{value:,.{decimals}f}"
        else:
            formatted = f"{value:,}"
        return formatted.replace(",", "\u202f")
    return str(value)


TEMPLATES.env.filters.setdefault("format_metric", _format_metric)


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
    "remission_guides": "Guías de Remisión",
    "purchases": "Compras",
    "sales": "Ventas",
    "documents": "Documentos",
    "registry_transactions": "Transacciones de Registro",
    "persons": "Personas",
    "cost_centers": "Centros de Costo",
}

RESOURCE_LABELS = {
    slug: RESOURCE_LABEL_OVERRIDES.get(slug, slug.replace("_", " ").title())
    for slug in InventoryRepository.RESOURCES
}

UPCOMING_FEATURES = (
    {
        "title": "Indicadores de rotación de inventario",
        "description": "Análisis de rotación y cobertura basados en catálogos y documentos registrados.",
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


def _analytics_params(
    *,
    velocity_period_days: int | None,
    turnover_period_days: int | None,
    low_stock_threshold_days: float,
    excess_stock_threshold_days: float,
    top_n: int,
    limit: int,
) -> dict[str, Any]:
    return {
        "velocity_period_days": velocity_period_days,
        "turnover_period_days": turnover_period_days,
        "low_stock_threshold_days": low_stock_threshold_days,
        "excess_stock_threshold_days": excess_stock_threshold_days,
        "top_n": top_n,
        "limit": limit,
    }


def _build_pdf_table(data: list[list[str]], *, header: bool = True) -> Table:
    table = Table(data, hAlign="LEFT")
    style = [
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#d7deea")),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
    ]
    if header and data:
        style.extend(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0b7285")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ]
        )
    table.setStyle(TableStyle(style))
    return table


def _build_inventory_pdf(report: dict[str, Any], params: dict[str, Any]) -> BytesIO:
    buffer = BytesIO()
    document = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        leftMargin=40,
        rightMargin=40,
        topMargin=60,
        bottomMargin=36,
    )
    styles = getSampleStyleSheet()
    story: list[Any] = []

    summary = report.get("summary", {})
    story.append(Paragraph("Reporte integral de inventario", styles["Title"]))
    generated_at = summary.get("generated_at")
    if generated_at:
        story.append(Paragraph(f"Generado: {generated_at}", styles["BodyText"]))
    story.append(
        Paragraph(
            "Parámetros del análisis: "
            f"velocidad = {params['velocity_period_days'] or 'sin límite'} días · "
            f"rotación = {params['turnover_period_days'] or 'sin límite'} días · "
            f"umbral bajo = {params['low_stock_threshold_days']} días · "
            f"umbral exceso = {params['excess_stock_threshold_days']} días",
            styles["BodyText"],
        )
    )
    story.append(Spacer(1, 12))

    story.append(Paragraph("Resumen ejecutivo", styles["Heading2"]))
    summary_rows = [
        ["Métrica", "Valor"],
        ["Productos analizados", _format_metric(summary.get("total_products"), decimals=0)],
        ["Unidades compradas", _format_metric(summary.get("total_purchased_units"), decimals=0)],
        ["Unidades vendidas", _format_metric(summary.get("total_sold_units"), decimals=0)],
        ["Unidades en stock", _format_metric(summary.get("total_stock_units"), decimals=0)],
        [
            "Lead time promedio (días)",
            _format_metric(summary.get("average_lead_time_days")),
        ],
        [
            "Velocidad de ventas (unid/día)",
            _format_metric(summary.get("overall_sales_velocity_per_day")),
        ],
        [
            "Cobertura promedio (días)",
            _format_metric(summary.get("overall_stock_coverage_days")),
        ],
        [
            "Rotación de inventario",
            _format_metric(summary.get("overall_inventory_turnover")),
        ],
    ]
    story.append(_build_pdf_table(summary_rows))
    story.append(Spacer(1, 18))

    rankings = report.get("rankings", {})
    ranking_definitions = [
        (
            "top_selling_products",
            "Productos más vendidos",
            ["Producto", "Unidades", "Velocidad/día"],
            lambda item: [
                item.get("product_label", item.get("product_id", "-")),
                _format_metric(item.get("total_sold_units"), decimals=0),
                _format_metric(item.get("sales_velocity_per_day")),
            ],
        ),
        (
            "top_stock_levels",
            "Inventario disponible",
            ["Producto", "Unidades", "Cobertura (días)"],
            lambda item: [
                item.get("product_label", item.get("product_id", "-")),
                _format_metric(item.get("current_stock_units"), decimals=0),
                _format_metric(item.get("stock_coverage_days")),
            ],
        ),
        (
            "fastest_turnover",
            "Mayor rotación",
            ["Producto", "Rotación"],
            lambda item: [
                item.get("product_label", item.get("product_id", "-")),
                _format_metric(item.get("inventory_turnover")),
            ],
        ),
        (
            "longest_lead_times",
            "Mayores lead times",
            ["Producto", "Lead time (días)"],
            lambda item: [
                item.get("product_label", item.get("product_id", "-")),
                _format_metric(item.get("average_lead_time_days")),
            ],
        ),
    ]

    for key, title, headers, formatter in ranking_definitions:
        items = rankings.get(key) or []
        if not items:
            continue
        story.append(Paragraph(title, styles["Heading2"]))
        rows = [headers]
        for entry in items:
            rows.append(formatter(entry))
        story.append(_build_pdf_table(rows))
        story.append(Spacer(1, 12))

    product_rows = [[
        "Producto",
        "Velocidad (unid/día)",
        "Cobertura (días)",
        "Rotación",
        "Punto de reorden",
        "Stock actual",
    ]]
    for product in report.get("products", [])[: params.get("top_n", DEFAULT_TOP_N)]:
        product_rows.append(
            [
                product.get("product_label", product.get("product_id", "-")),
                _format_metric(product.get("sales_velocity_per_day")),
                _format_metric(product.get("stock_coverage_days")),
                _format_metric(product.get("inventory_turnover")),
                _format_metric(product.get("reorder_point")),
                _format_metric(product.get("current_stock_units"), decimals=0),
            ]
        )
    if len(product_rows) > 1:
        story.append(Paragraph("Detalle por producto", styles["Heading2"]))
        story.append(_build_pdf_table(product_rows))
        story.append(Spacer(1, 12))

    alerts = report.get("alerts", {})
    if any(alerts.get(key) for key in ALERT_LABELS):
        story.append(Paragraph("Alertas destacadas", styles["Heading2"]))
        body = styles["BodyText"].clone("Alerts")
        body.fontSize = 9
        for key, label in ALERT_LABELS.items():
            items = alerts.get(key) or []
            if not items:
                continue
            story.append(Paragraph(label, styles["Heading3"]))
            for entry in items[: params.get("top_n", DEFAULT_TOP_N)]:
                details: list[str] = []
                if "stock_coverage_days" in entry:
                    details.append(
                        f"Cobertura { _format_metric(entry.get('stock_coverage_days')) } días"
                    )
                if "current_stock_units" in entry:
                    details.append(
                        f"Stock { _format_metric(entry.get('current_stock_units'), decimals=0) }"
                    )
                if "reorder_point" in entry and entry.get("reorder_point") is not None:
                    details.append(
                        f"Reorden { _format_metric(entry.get('reorder_point')) }"
                    )
                story.append(
                    Paragraph(
                        f"&bull; Producto {entry.get('product_label', entry.get('product_id', '-'))}: "
                        + (", ".join(details) or "sin datos adicionales"),
                        body,
                    )
                )
            story.append(Spacer(1, 6))

    document.build(story)
    buffer.seek(0)
    return buffer


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


@app.get("/analytics", response_class=HTMLResponse)
def analytics_dashboard(
    request: Request,
    velocity_period_days: int | None = Query(
        default=DEFAULT_VELOCITY_PERIOD_DAYS, ge=1, le=365
    ),
    turnover_period_days: int | None = Query(
        default=DEFAULT_TURNOVER_PERIOD_DAYS, ge=1, le=730
    ),
    low_stock_threshold_days: float = Query(
        default=DEFAULT_LOW_STOCK_THRESHOLD_DAYS, gt=0, le=365
    ),
    excess_stock_threshold_days: float = Query(
        default=DEFAULT_EXCESS_STOCK_THRESHOLD_DAYS, gt=0, le=730
    ),
    top_n: int = Query(default=DEFAULT_TOP_N, ge=1, le=25),
    limit: int = Query(default=1000, ge=10, le=5000),
    repo: InventoryRepository = Depends(get_repository),
) -> HTMLResponse:
    params = _analytics_params(
        velocity_period_days=velocity_period_days,
        turnover_period_days=turnover_period_days,
        low_stock_threshold_days=low_stock_threshold_days,
        excess_stock_threshold_days=excess_stock_threshold_days,
        top_n=top_n,
        limit=limit,
    )
    report = generate_inventory_report(
        repo,
        velocity_period_days=velocity_period_days,
        turnover_period_days=turnover_period_days,
        low_stock_threshold_days=low_stock_threshold_days,
        excess_stock_threshold_days=excess_stock_threshold_days,
        top_n=top_n,
        limit=limit,
    )

    summary = report.get("summary", {})
    summary_cards = [
        {
            "label": "Productos analizados",
            "value": _format_metric(summary.get("total_products"), decimals=0),
        },
        {
            "label": "Unidades vendidas",
            "value": _format_metric(summary.get("total_sold_units"), decimals=0),
        },
        {
            "label": "Unidades en stock",
            "value": _format_metric(summary.get("total_stock_units"), decimals=0),
        },
        {
            "label": "Velocidad promedio (unid/día)",
            "value": _format_metric(summary.get("overall_sales_velocity_per_day")),
        },
        {
            "label": "Cobertura promedio (días)",
            "value": _format_metric(summary.get("overall_stock_coverage_days")),
        },
        {
            "label": "Rotación de inventario",
            "value": _format_metric(summary.get("overall_inventory_turnover")),
        },
    ]

    alerts = report.get("alerts", {})
    alert_counters = {
        key: len(alerts.get(key) or [])
        for key in ALERT_LABELS
    }

    query = {k: v for k, v in params.items() if v is not None}
    query_string = urlencode(query)
    pdf_url = request.url_for("analytics_report_pdf")
    if query_string:
        pdf_url = f"{pdf_url}?{query_string}"

    return TEMPLATES.TemplateResponse(
        "analytics.html",
        {
            "request": request,
            "report": report,
            "summary_cards": summary_cards,
            "alert_labels": ALERT_LABELS,
            "alert_counters": alert_counters,
            "params": params,
            "pdf_url": pdf_url,
            "current_year": datetime.utcnow().year,
        },
    )


@app.get("/analytics/report.pdf")
def analytics_report_pdf(
    velocity_period_days: int | None = Query(
        default=DEFAULT_VELOCITY_PERIOD_DAYS, ge=1, le=365
    ),
    turnover_period_days: int | None = Query(
        default=DEFAULT_TURNOVER_PERIOD_DAYS, ge=1, le=730
    ),
    low_stock_threshold_days: float = Query(
        default=DEFAULT_LOW_STOCK_THRESHOLD_DAYS, gt=0, le=365
    ),
    excess_stock_threshold_days: float = Query(
        default=DEFAULT_EXCESS_STOCK_THRESHOLD_DAYS, gt=0, le=730
    ),
    top_n: int = Query(default=DEFAULT_TOP_N, ge=1, le=25),
    limit: int = Query(default=1000, ge=10, le=5000),
    repo: InventoryRepository = Depends(get_repository),
) -> StreamingResponse:
    params = _analytics_params(
        velocity_period_days=velocity_period_days,
        turnover_period_days=turnover_period_days,
        low_stock_threshold_days=low_stock_threshold_days,
        excess_stock_threshold_days=excess_stock_threshold_days,
        top_n=top_n,
        limit=limit,
    )
    report = generate_inventory_report(
        repo,
        velocity_period_days=velocity_period_days,
        turnover_period_days=turnover_period_days,
        low_stock_threshold_days=low_stock_threshold_days,
        excess_stock_threshold_days=excess_stock_threshold_days,
        top_n=top_n,
        limit=limit,
    )

    pdf_buffer = _build_inventory_pdf(report, params)
    filename = f"reporte-inventario-{datetime.utcnow():%Y%m%d}.pdf"

    return StreamingResponse(
        pdf_buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
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


@app.get("/api/resources/sample")
def api_sample_all_resources(
    limit: int = Query(default=1, ge=1, le=5),
    repo: InventoryRepository = Depends(get_repository),
) -> dict[str, Any]:
    """Return a small sample of stored records for every supported resource."""

    resources_payload: list[dict[str, Any]] = []
    for slug in InventoryRepository.RESOURCES:
        try:
            results = repo.search_records(slug, limit=limit)
        except ValueError as exc:  # pragma: no cover - defensive
            raise HTTPException(status_code=404, detail=str(exc)) from exc

        resources_payload.append(
            {
                "resource": slug,
                "label": RESOURCE_LABELS.get(slug, slug.replace("_", " ").title()),
                "count": len(results),
                "results": results,
            }
        )

    return {
        "limit": limit,
        "resources": resources_payload,
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

