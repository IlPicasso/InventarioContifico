"""Generación de reportes y KPIs consolidados para inventario."""
from __future__ import annotations

from collections import defaultdict
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

ProductReport = dict[str, Any]
InventoryReport = dict[str, Any]


def _mean_inventory(stock_levels: Sequence[StockLevel]) -> float:
    if not stock_levels:
        return 0.0
    return mean(level.quantity for level in stock_levels)


def _serialize_models(models: Sequence[Purchase | Sale | StockLevel]) -> list[dict[str, Any]]:
    return [model.model_dump() for model in models]


def _build_product_report(
    *,
    product_id: str,
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

    return {
        "product_id": product_id,
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
        "purchases": list(purchases),
        "sales": list(sales),
        "stock_levels": list(stock_levels),
    }


def _resolve_safety_stock(
    safety_stock: float | dict[str, float] | None, product_id: str
) -> float:
    if isinstance(safety_stock, dict):
        value = safety_stock.get(product_id, 0.0)
    else:
        value = safety_stock or 0.0
    return max(float(value), 0.0)


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

    purchases = load_purchases(repo, product_id=product_id, limit=limit)
    sales = load_sales(repo, product_id=product_id, limit=limit)
    stock_levels = load_stock_levels(repo, product_id=product_id, limit=limit)

    return _build_product_report(
        product_id=product_id,
        purchases=purchases,
        sales=sales,
        stock_levels=stock_levels,
        velocity_period_days=velocity_period_days,
        turnover_period_days=turnover_period_days,
        safety_stock=safety_stock,
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

    purchases = load_purchases(repo, limit=limit)
    sales = load_sales(repo, limit=limit)
    stock_levels = load_stock_levels(repo, limit=limit)

    product_ids = sorted(
        {
            *[purchase.product_id for purchase in purchases],
            *[sale.product_id for sale in sales],
            *[level.product_id for level in stock_levels],
        }
    )

    per_product_raw: list[ProductReport] = []
    purchases_by_product: dict[str, list[Purchase]] = defaultdict(list)
    sales_by_product: dict[str, list[Sale]] = defaultdict(list)
    stock_by_product: dict[str, list[StockLevel]] = defaultdict(list)

    for purchase in purchases:
        purchases_by_product[purchase.product_id].append(purchase)
    for sale in sales:
        sales_by_product[sale.product_id].append(sale)
    for level in stock_levels:
        stock_by_product[level.product_id].append(level)

    for product_id in product_ids:
        per_product_raw.append(
            _build_product_report(
                product_id=product_id,
                purchases=purchases_by_product.get(product_id, ()),
                sales=sales_by_product.get(product_id, ()),
                stock_levels=stock_by_product.get(product_id, ()),
                velocity_period_days=velocity_period_days,
                turnover_period_days=turnover_period_days,
                safety_stock=_resolve_safety_stock(safety_stock, product_id),
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
    rankings["top_selling_products"] = [
        {
            "product_id": report["product_id"],
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
            "product_id": report["product_id"],
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
            "product_id": report["product_id"],
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
            "product_id": report["product_id"],
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
            "product_id": report["product_id"],
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
            "product_id": report["product_id"],
            "reorder_point": report["reorder_point"],
            "current_stock_units": report["current_stock_units"],
        }
        for report in per_product_raw
        if report["reorder_point"] is not None
        and report["current_stock_units"] < report["reorder_point"]
    ]
    alerts["no_sales"] = [
        {
            "product_id": report["product_id"],
            "current_stock_units": report["current_stock_units"],
        }
        for report in per_product_raw
        if report["total_sold_units"] == 0 and report["current_stock_units"] > 0
    ]
    alerts["no_purchases"] = [
        {
            "product_id": report["product_id"],
            "current_stock_units": report["current_stock_units"],
        }
        for report in per_product_raw
        if report["total_purchased_units"] == 0 and report["current_stock_units"] > 0
    ]
    alerts["excess_stock"] = [
        {
            "product_id": report["product_id"],
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
            "product_id": report["product_id"],
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
