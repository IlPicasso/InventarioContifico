"""Herramientas para calcular KPIs y reportes de inventario."""
from __future__ import annotations

from .lead_time import average_lead_time
from .loaders import load_purchases, load_sales, load_stock_levels
from .models import Purchase, Sale, StockLevel
from .reorder_points import calculate_reorder_point
from .reports import (
    generate_inventory_report,
    generate_product_kpis,
)
from .sales_velocity import (
    calculate_inventory_turnover,
    calculate_sales_velocity,
    calculate_stock_coverage,
)
from .sku import format_variant_label, split_sku_and_size

__all__ = [
    "average_lead_time",
    "calculate_inventory_turnover",
    "calculate_reorder_point",
    "calculate_sales_velocity",
    "calculate_stock_coverage",
    "generate_inventory_report",
    "generate_product_kpis",
    "load_purchases",
    "load_sales",
    "load_stock_levels",
    "Purchase",
    "Sale",
    "StockLevel",
    "split_sku_and_size",
    "format_variant_label",
]
