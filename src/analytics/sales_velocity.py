"""Cálculo de métricas de velocidad de ventas y rotación de inventario."""
from __future__ import annotations

from typing import Iterable

from .models import Sale, StockLevel


def _period_in_days(sales: Iterable[Sale], period_days: int | None = None) -> int:
    if period_days:
        return max(1, int(period_days))
    sales_list = list(sales)
    if not sales_list:
        return 0
    start = min(s.sold_at for s in sales_list)
    end = max(s.sold_at for s in sales_list)
    delta = (end - start).days + 1
    return max(delta, 1)


def calculate_sales_velocity(
    sales: Iterable[Sale], *, period_days: int | None = None
) -> float | None:
    """Calcula las unidades vendidas por día."""

    sales_list = list(sales)
    if not sales_list:
        return None
    total_quantity = sum(sale.quantity for sale in sales_list)
    days = _period_in_days(sales_list, period_days=period_days)
    if days <= 0:
        return None
    return total_quantity / days


def calculate_stock_coverage(
    stock_levels: Iterable[StockLevel], velocity: float | None
) -> float | None:
    """Calcula la cobertura de inventario en días considerando la velocidad de venta."""

    if velocity is None or velocity <= 0:
        return None
    stock_list = list(stock_levels)
    if not stock_list:
        return None
    total_stock = sum(level.quantity for level in stock_list)
    return total_stock / velocity


def calculate_inventory_turnover(
    sales: Iterable[Sale],
    average_inventory: float,
    *,
    period_days: int | None = None,
) -> float | None:
    """Calcula la rotación de inventario anualizada."""

    if average_inventory <= 0:
        return None
    sales_list = list(sales)
    if not sales_list:
        return None
    total_sold = sum(sale.quantity for sale in sales_list)
    days = _period_in_days(sales_list, period_days=period_days)
    if days <= 0:
        return None
    annualisation_factor = 365 / days
    return (total_sold / average_inventory) * annualisation_factor


__all__ = [
    "calculate_sales_velocity",
    "calculate_stock_coverage",
    "calculate_inventory_turnover",
]
