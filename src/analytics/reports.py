"""Generación de reportes y KPIs consolidados para inventario."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from statistics import mean
from typing import Any, Sequence

from ..persistence import InventoryRepository
from .lead_time import average_lead_time
from .loaders import load_purchases, load_sales, load_stock_levels
from .models import Purchase, Sale, StockLevel
from .reorder_points import calculate_reorder_point
from .sales_velocity import (
    calculate_inventory_turnover,
    calculate_sales_velocity,
    calculate_stock_coverage,
)
from .sku import format_variant_label, split_sku_and_size

ProductReport = dict[str, Any]
InventoryReport = dict[str, Any]


@dataclass(frozen=True)
class ProductCatalogEntry:
    code: str
    internal_id: str | None = None
    name: str | None = None
    category_id: str | None = None
    category_name: str | None = None


class ProductCatalog:
    def __init__(self, entries: Sequence[ProductCatalogEntry]):
        self._by_code: dict[str, ProductCatalogEntry] = {}
        self._by_internal_id: dict[str, ProductCatalogEntry] = {}
        for entry in entries:
            if entry.code:
                self._by_code[entry.code] = entry
            if entry.internal_id:
                self._by_internal_id[entry.internal_id] = entry

    def resolve(
        self,
        *,
        code: str | None = None,
        source_id: str | None = None,
    ) -> tuple[str, ProductCatalogEntry | None]:
        candidate_code = (code or "").strip()
        candidate_id = (source_id or "").strip()

        if candidate_code:
            entry = self._by_code.get(candidate_code)
            if entry:
                return entry.code or candidate_code, entry

        if candidate_id:
            entry = self._by_internal_id.get(candidate_id)
            if entry:
                return entry.code or candidate_code or candidate_id, entry

        if candidate_code:
            return candidate_code, None
        if candidate_id:
            return candidate_id, None
        return "", None


def _clean_text(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _load_product_catalog(
    repo: InventoryRepository, *, limit: int = 1000
) -> ProductCatalog:
    categories: dict[str, str] = {}
    for record in repo.search_records("categories", limit=limit):
        data = record.get("data") or {}
        category_id = _clean_text(data.get("id") or record.get("id"))
        if not category_id:
            continue
        category_name = _clean_text(data.get("nombre") or data.get("name"))
        if category_name:
            categories[category_id] = category_name

    entries: list[ProductCatalogEntry] = []
    for record in repo.search_records("products", limit=limit):
        data = record.get("data") or {}
        internal_id = _clean_text(data.get("id") or record.get("id"))
        code = _clean_text(data.get("codigo") or data.get("code"))
        if not code and not internal_id:
            continue
        if not code:
            code = internal_id
        name = _clean_text(data.get("nombre") or data.get("name"))
        category_id = _clean_text(data.get("categoria_id") or data.get("category_id"))
        category_name = _clean_text(
            data.get("categoria_nombre")
            or data.get("category_name")
            or categories.get(category_id or "")
        )
        entries.append(
            ProductCatalogEntry(
                code=code or "",
                internal_id=internal_id,
                name=name,
                category_id=category_id,
                category_name=category_name,
            )
        )

    return ProductCatalog(entries)


def _mean_inventory(stock_levels: Sequence[StockLevel]) -> float:
    if not stock_levels:
        return 0.0
    return mean(level.quantity for level in stock_levels)


def _serialize_models(models: Sequence[Purchase | Sale | StockLevel]) -> list[dict[str, Any]]:
    return [model.model_dump() for model in models]


def _build_product_report(
    *,
    product_sku: str,
    metadata: ProductCatalogEntry | None,
    internal_ids: Sequence[str],
    purchases: Sequence[Purchase],
    sales: Sequence[Sale],
    stock_levels: Sequence[StockLevel],
    velocity_period_days: int | None,
    turnover_period_days: int | None,
    safety_stock: float,
) -> ProductReport:
    lead = average_lead_time(purchases)
    velocity = calculate_sales_velocity(sales, period_days=velocity_period_days)
    coverage = calculate_stock_coverage(stock_levels, velocity)
    average_inventory = _mean_inventory(stock_levels)
    turnover = calculate_inventory_turnover(
        sales,
        average_inventory,
        period_days=turnover_period_days,
    )

    reorder_point = None
    if lead and velocity is not None:
        reorder_point = calculate_reorder_point(
            daily_demand=velocity,
            lead_time_days=lead.total_seconds() / 86_400,
            safety_stock=max(safety_stock, 0.0),
        )

    sku = (product_sku or (metadata.code if metadata else "") or "").strip()
    if not sku:
        sku = next((identifier for identifier in internal_ids if identifier), "")

    product_code, variant_size = split_sku_and_size(sku)
    label = format_variant_label(sku)

    product_name = metadata.name if metadata else None
    category_id = metadata.category_id if metadata else None
    category_name = metadata.category_name if metadata else None

    internal_identifiers = {identifier for identifier in internal_ids if identifier}
    if metadata and metadata.internal_id:
        internal_identifiers.add(metadata.internal_id)
    if not internal_identifiers and sku:
        internal_identifiers.add(sku)

    purchases_display = [purchase.model_copy(update={"product_id": sku}) for purchase in purchases]
    sales_display = [sale.model_copy(update={"product_id": sku}) for sale in sales]
    stock_display = [level.model_copy(update={"product_id": sku}) for level in stock_levels]

    return {
        "product_id": sku,
        "product_code": product_code,
        "variant_size": variant_size,
        "product_label": label,
        "product_name": product_name,
        "category_id": category_id,
        "category_name": category_name,
        "product_internal_ids": sorted(internal_identifiers),
        "average_lead_time_days": (
            lead.total_seconds() / 86_400 if lead else None
        ),
        "sales_velocity_per_day": velocity,
        "stock_coverage_days": coverage,
        "inventory_turnover": turnover,
        "reorder_point": reorder_point,
        "total_purchased_units": sum(p.quantity for p in purchases),
        "total_sold_units": sum(s.quantity for s in sales),
        "current_stock_units": sum(level.quantity for level in stock_levels),
        "average_inventory_units": average_inventory,
        "purchases": purchases_display,
        "sales": sales_display,
        "stock_levels": stock_display,
    }


def _resolve_safety_stock(
    safety_stock: float | dict[str, float] | None,
    product_code: str,
    identifiers: Sequence[str],
) -> float:
    if isinstance(safety_stock, dict):
        lookup_order = [product_code, *identifiers]
        for key in lookup_order:
            if not key:
                continue
            if key in safety_stock:
                return max(float(safety_stock[key]), 0.0)
        return 0.0

    value = safety_stock or 0.0
    return max(float(value), 0.0)


def _resolve_product_key(
    catalog: ProductCatalog,
    purchases: Sequence[Purchase],
    sales: Sequence[Sale],
    stock_levels: Sequence[StockLevel],
    fallback: str,
) -> tuple[str, ProductCatalogEntry | None]:
    for collection in (purchases, sales, stock_levels):
        for item in collection:
            code, entry = catalog.resolve(
                code=item.product_id, source_id=item.source_product_id
            )
            if code:
                return code, entry

    code, entry = catalog.resolve(code=fallback, source_id=None)
    if code:
        return code, entry
    return fallback, entry


def _serialise_product_report(report: ProductReport) -> ProductReport:
    serialised = dict(report)
    serialised["purchases"] = _serialize_models(report["purchases"])
    serialised["sales"] = _serialize_models(report["sales"])
    serialised["stock_levels"] = _serialize_models(report["stock_levels"])
    return serialised


def generate_product_kpis(
    repo: InventoryRepository,
    product_id: str,
    *,
    velocity_period_days: int | None = None,
    turnover_period_days: int | None = None,
    safety_stock: float = 0.0,
    limit: int = 1000,
) -> ProductReport:
    """Genera un resumen de KPIs para un producto específico."""

    catalog = _load_product_catalog(repo, limit=limit)
    purchases = load_purchases(repo, product_id=product_id, limit=limit)
    sales = load_sales(repo, product_id=product_id, limit=limit)
    stock_levels = load_stock_levels(repo, product_id=product_id, limit=limit)

    product_sku, metadata = _resolve_product_key(
        catalog, purchases, sales, stock_levels, product_id
    )
    product_sku = product_sku or product_id

    internal_ids: set[str] = set()
    for collection in (purchases, sales, stock_levels):
        for item in collection:
            if item.source_product_id:
                internal_ids.add(item.source_product_id)
    if product_id:
        internal_ids.add(product_id)
    identifiers = sorted(internal_ids)
    resolved_safety = _resolve_safety_stock(safety_stock, product_sku, identifiers)

    return _build_product_report(
        product_sku=product_sku,
        metadata=metadata,
        internal_ids=identifiers,
        purchases=purchases,
        sales=sales,
        stock_levels=stock_levels,
        velocity_period_days=velocity_period_days,
        turnover_period_days=turnover_period_days,
        safety_stock=resolved_safety,
    )


def generate_inventory_report(
    repo: InventoryRepository,
    *,
    velocity_period_days: int | None = None,
    turnover_period_days: int | None = None,
    safety_stock: float | dict[str, float] | None = 0.0,
    low_stock_threshold_days: float = 7.0,
    excess_stock_threshold_days: float = 60.0,
    top_n: int = 5,
    limit: int = 1000,
) -> InventoryReport:
    """Genera un reporte integral con múltiples vistas del inventario."""

    catalog = _load_product_catalog(repo, limit=limit)
    purchases = load_purchases(repo, limit=limit)
    sales = load_sales(repo, limit=limit)
    stock_levels = load_stock_levels(repo, limit=limit)

    per_product_raw: list[ProductReport] = []
    purchases_by_product: dict[str, list[Purchase]] = defaultdict(list)
    sales_by_product: dict[str, list[Sale]] = defaultdict(list)
    stock_by_product: dict[str, list[StockLevel]] = defaultdict(list)
    metadata_by_product: dict[str, ProductCatalogEntry | None] = {}
    internal_ids_by_product: dict[str, set[str]] = defaultdict(set)

    def _register_product(
        key: str,
        entry: ProductCatalogEntry | None,
        source_id: str | None,
    ) -> None:
        if not key:
            return
        if entry is not None or key not in metadata_by_product:
            metadata_by_product[key] = entry
        if source_id:
            internal_ids_by_product[key].add(source_id)
        if entry and entry.internal_id:
            internal_ids_by_product[key].add(entry.internal_id)

    def _resolve_key(code: str, source_id: str | None) -> tuple[str, ProductCatalogEntry | None]:
        resolved_code, entry = catalog.resolve(code=code, source_id=source_id)
        key = resolved_code or code or (source_id or "")
        return key, entry

    for purchase in purchases:
        key, entry = _resolve_key(purchase.product_id, purchase.source_product_id)
        if not key:
            continue
        purchases_by_product[key].append(purchase)
        _register_product(key, entry, purchase.source_product_id)

    for sale in sales:
        key, entry = _resolve_key(sale.product_id, sale.source_product_id)
        if not key:
            continue
        sales_by_product[key].append(sale)
        _register_product(key, entry, sale.source_product_id)

    for level in stock_levels:
        key, entry = _resolve_key(level.product_id, level.source_product_id)
        if not key:
            continue
        stock_by_product[key].append(level)
        _register_product(key, entry, level.source_product_id)

    product_codes = sorted(
        {
            *purchases_by_product.keys(),
            *sales_by_product.keys(),
            *stock_by_product.keys(),
        }
    )

    for product_code in product_codes:
        metadata = metadata_by_product.get(product_code)
        if metadata is None:
            _, metadata = catalog.resolve(code=product_code, source_id=None)
        identifiers = sorted(internal_ids_by_product.get(product_code, set()))
        safety_value = _resolve_safety_stock(safety_stock, product_code, identifiers)
        per_product_raw.append(
            _build_product_report(
                product_sku=product_code,
                metadata=metadata,
                internal_ids=identifiers,
                purchases=purchases_by_product.get(product_code, ()),
                sales=sales_by_product.get(product_code, ()),
                stock_levels=stock_by_product.get(product_code, ()),
                velocity_period_days=velocity_period_days,
                turnover_period_days=turnover_period_days,
                safety_stock=safety_value,
            )
        )

    overall_lead_time = average_lead_time(purchases)
    overall_velocity = calculate_sales_velocity(sales, period_days=velocity_period_days)
    overall_stock_coverage = calculate_stock_coverage(stock_levels, overall_velocity)
    overall_turnover = calculate_inventory_turnover(
        sales,
        _mean_inventory(stock_levels),
        period_days=turnover_period_days,
    )

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_products": len(per_product_raw),
        "total_purchased_units": sum(r["total_purchased_units"] for r in per_product_raw),
        "total_sold_units": sum(r["total_sold_units"] for r in per_product_raw),
        "total_stock_units": sum(r["current_stock_units"] for r in per_product_raw),
        "average_lead_time_days": (
            overall_lead_time.total_seconds() / 86_400 if overall_lead_time else None
        ),
        "overall_sales_velocity_per_day": overall_velocity,
        "overall_stock_coverage_days": overall_stock_coverage,
        "overall_inventory_turnover": overall_turnover,
    }

    rankings: dict[str, list[dict[str, Any]]] = {}
    def _product_fields(report: ProductReport) -> dict[str, Any]:
        return {
            "product_id": report["product_id"],
            "product_code": report.get("product_code"),
            "variant_size": report.get("variant_size"),
            "product_label": report.get("product_label", report["product_id"]),
            "product_name": report.get("product_name"),
            "category_id": report.get("category_id"),
            "category_name": report.get("category_name"),
            "product_internal_ids": report.get("product_internal_ids"),
        }

    rankings["top_selling_products"] = [
        {
            **_product_fields(report),
            "total_sold_units": report["total_sold_units"],
            "sales_velocity_per_day": report["sales_velocity_per_day"],
        }
        for report in sorted(
            per_product_raw,
            key=lambda item: item["total_sold_units"],
            reverse=True,
        )[:top_n]
    ]
    rankings["top_stock_levels"] = [
        {
            **_product_fields(report),
            "current_stock_units": report["current_stock_units"],
            "stock_coverage_days": report["stock_coverage_days"],
        }
        for report in sorted(
            per_product_raw,
            key=lambda item: item["current_stock_units"],
            reverse=True,
        )[:top_n]
    ]
    rankings["longest_lead_times"] = [
        {
            **_product_fields(report),
            "average_lead_time_days": report["average_lead_time_days"],
        }
        for report in sorted(
            (r for r in per_product_raw if r["average_lead_time_days"] is not None),
            key=lambda item: item["average_lead_time_days"],
            reverse=True,
        )[:top_n]
    ]
    rankings["fastest_turnover"] = [
        {
            **_product_fields(report),
            "inventory_turnover": report["inventory_turnover"],
        }
        for report in sorted(
            (r for r in per_product_raw if r["inventory_turnover"] is not None),
            key=lambda item: item["inventory_turnover"],
            reverse=True,
        )[:top_n]
    ]

    alerts: dict[str, list[dict[str, Any]]] = {}
    alerts["low_stock"] = [
        {
            **_product_fields(report),
            "stock_coverage_days": report["stock_coverage_days"],
            "current_stock_units": report["current_stock_units"],
        }
        for report in sorted(
            (
                r
                for r in per_product_raw
                if r["stock_coverage_days"] is not None
                and r["stock_coverage_days"] <= low_stock_threshold_days
            ),
            key=lambda item: item["stock_coverage_days"] or 0.0,
        )
    ]
    alerts["reorder_recommended"] = [
        {
            **_product_fields(report),
            "reorder_point": report["reorder_point"],
            "current_stock_units": report["current_stock_units"],
        }
        for report in per_product_raw
        if report["reorder_point"] is not None
        and report["current_stock_units"] < report["reorder_point"]
    ]
    alerts["no_sales"] = [
        {
            **_product_fields(report),
            "current_stock_units": report["current_stock_units"],
        }
        for report in per_product_raw
        if report["total_sold_units"] == 0 and report["current_stock_units"] > 0
    ]
    alerts["no_purchases"] = [
        {
            **_product_fields(report),
            "current_stock_units": report["current_stock_units"],
        }
        for report in per_product_raw
        if report["total_purchased_units"] == 0 and report["current_stock_units"] > 0
    ]
    alerts["excess_stock"] = [
        {
            **_product_fields(report),
            "stock_coverage_days": report["stock_coverage_days"],
            "current_stock_units": report["current_stock_units"],
        }
        for report in sorted(
            (
                r
                for r in per_product_raw
                if r["stock_coverage_days"] is not None
                and r["stock_coverage_days"] >= excess_stock_threshold_days
            ),
            key=lambda item: item["stock_coverage_days"] or 0.0,
            reverse=True,
        )
    ]
    alerts["stagnant_stock"] = [
        {
            **_product_fields(report),
            "current_stock_units": report["current_stock_units"],
        }
        for report in per_product_raw
        if (
            (report["sales_velocity_per_day"] is None or report["sales_velocity_per_day"] == 0)
            and report["current_stock_units"] > 0
        )
    ]

    metadata = {
        "velocity_period_days": velocity_period_days,
        "turnover_period_days": turnover_period_days,
        "low_stock_threshold_days": low_stock_threshold_days,
        "excess_stock_threshold_days": excess_stock_threshold_days,
        "top_n": top_n,
        "limit": limit,
    }

    return {
        "summary": summary,
        "products": [_serialise_product_report(report) for report in per_product_raw],
        "rankings": rankings,
        "alerts": alerts,
        "metadata": metadata,
    }


__all__ = ["generate_product_kpis", "generate_inventory_report"]
