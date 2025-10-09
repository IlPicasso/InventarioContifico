"""Herramientas para calcular KPIs de inventario basados en los datos sincronizados."""
from __future__ import annotations

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

__all__ = [
    "average_lead_time",
    "calculate_inventory_turnover",
    "calculate_reorder_point",
    "calculate_sales_velocity",
    "calculate_stock_coverage",
    "generate_product_kpis",
    "load_purchases",
    "load_sales",
    "load_stock_levels",
    "Purchase",
    "Sale",
    "StockLevel",
]


def _mean_inventory(stock_levels: Sequence[StockLevel]) -> float:
    if not stock_levels:
        return 0.0
    return mean(level.quantity for level in stock_levels)


def generate_product_kpis(
    repo: InventoryRepository,
    product_id: str,
    *,
    velocity_period_days: int | None = None,
    turnover_period_days: int | None = None,
    safety_stock: float = 0.0,
) -> dict[str, Any]:
    """Genera un resumen de KPIs para un producto específico."""

    purchases = load_purchases(repo, product_id=product_id)
    sales = load_sales(repo, product_id=product_id)
    stock_levels = load_stock_levels(repo, product_id=product_id)

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
        "purchases": purchases,
        "sales": sales,
        "stock_levels": stock_levels,
    }
