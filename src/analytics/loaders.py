"""Conversión de datos crudos de la base SQLite a modelos analíticos."""
from __future__ import annotations

from datetime import datetime
from typing import Iterable, Iterator, Sequence

from ..persistence import InventoryRepository
from .models import Purchase, Sale, StockLevel

_DATETIME_FIELDS = (
    "fecha_emision",
    "fecha",
    "created_at",
    "fecha_creacion",
    "fecha_documento",
    "fecha_registro",
)

_RECEIPT_FIELDS = (
    "fecha_recepcion",
    "fecha_entrega",
    "fecha_modificacion",
    "updated_at",
)

_SALE_DATETIME_FIELDS = (
    "fecha_emision",
    "fecha",
    "created_at",
    "fecha_venta",
)

_STOCK_DATETIME_FIELDS = (
    "fecha_actualizacion",
    "fecha",
    "updated_at",
    "fetched_at",
)


def _parse_datetime(value: str | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _parse_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _extract_first_datetime(data: dict, fields: Sequence[str]) -> datetime | None:
    for field in fields:
        parsed = _parse_datetime(data.get(field))
        if parsed:
            return parsed
    return None


def _iter_purchase_lines(record: dict) -> Iterator[Purchase]:
    data = record.get("data") or {}
    ordered_at = _extract_first_datetime(data, _DATETIME_FIELDS)
    if not ordered_at:
        return iter(())
    lines = data.get("detalles") or data.get("items") or []
    receptions = data.get("recepciones") or []
    warehouse_id = data.get("bodega_id") or data.get("warehouse_id")
    supplier_id = data.get("proveedor_id") or data.get("supplier_id")

    for line in lines:
        product_id = (
            line.get("producto_id")
            or line.get("product_id")
            or line.get("variant_id")
            or data.get("producto_id")
            or data.get("product_id")
        )
        if not product_id:
            continue
        receipt = _extract_first_datetime(line, _RECEIPT_FIELDS)
        if not receipt:
            receipt = _extract_first_datetime(data, _RECEIPT_FIELDS)
        if not receipt and receptions:
            for reception in receptions:
                reception_date = _parse_datetime(
                    reception.get("fecha")
                    or reception.get("fecha_recepcion")
                    or reception.get("created_at")
                )
                for detail in reception.get("detalles", []):
                    detail_product = (
                        detail.get("producto_id")
                        or detail.get("product_id")
                        or detail.get("variant_id")
                    )
                    if detail_product and str(detail_product) == str(product_id):
                        receipt = reception_date
                        break
                if receipt:
                    break
        quantity = _parse_float(
            line.get("cantidad")
            or line.get("quantity")
            or line.get("cant")
            or data.get("cantidad")
            or data.get("quantity")
            or 0,
        )
        yield Purchase(
            purchase_id=str(record.get("id")),
            product_id=str(product_id),
            ordered_at=ordered_at,
            received_at=receipt,
            quantity=max(quantity, 0.0),
            warehouse_id=str(warehouse_id) if warehouse_id else None,
            supplier_id=str(supplier_id) if supplier_id else None,
        )


def load_purchases(
    repo: InventoryRepository,
    *,
    product_id: str | None = None,
    limit: int = 1000,
) -> list[Purchase]:
    """Carga las líneas de compras almacenadas localmente."""

    records = repo.search_records("purchases", limit=limit)
    purchases: list[Purchase] = []
    for record in records:
        for purchase in _iter_purchase_lines(record):
            if product_id and (
                purchase.product_id != product_id
                and purchase.product_code != product_id
            ):
                continue
            purchases.append(purchase)
    return purchases


def _iter_sale_lines(record: dict) -> Iterator[Sale]:
    data = record.get("data") or {}
    sold_at = _extract_first_datetime(data, _SALE_DATETIME_FIELDS)
    if not sold_at:
        return iter(())
    lines = data.get("detalles") or data.get("items") or []
    warehouse_id = data.get("bodega_id") or data.get("warehouse_id")
    customer_id = data.get("cliente_id") or data.get("customer_id")
    for line in lines:
        product_id = (
            line.get("producto_id")
            or line.get("product_id")
            or line.get("variant_id")
            or data.get("producto_id")
            or data.get("product_id")
        )
        if not product_id:
            continue
        quantity = _parse_float(
            line.get("cantidad")
            or line.get("quantity")
            or line.get("cant")
            or data.get("cantidad")
            or data.get("quantity")
            or 0,
        )
        yield Sale(
            sale_id=str(record.get("id")),
            product_id=str(product_id),
            sold_at=sold_at,
            quantity=max(quantity, 0.0),
            warehouse_id=str(warehouse_id) if warehouse_id else None,
            customer_id=str(customer_id) if customer_id else None,
        )


def load_sales(
    repo: InventoryRepository,
    *,
    product_id: str | None = None,
    limit: int = 1000,
) -> list[Sale]:
    """Carga las líneas de ventas almacenadas localmente."""

    records = repo.search_records("sales", limit=limit)
    sales: list[Sale] = []
    for record in records:
        for sale in _iter_sale_lines(record):
            if product_id and (
                sale.product_id != product_id and sale.product_code != product_id
            ):
                continue
            sales.append(sale)
    return sales


def _iter_stock_levels(record: dict) -> Iterator[StockLevel]:
    data = record.get("data") or {}
    product_id = (
        data.get("producto_id")
        or data.get("product_id")
        or data.get("id")
        or record.get("id")
    )
    if not product_id:
        return iter(())
    warehouse_id = data.get("bodega_id") or data.get("warehouse_id")
    as_of = _extract_first_datetime(data, _STOCK_DATETIME_FIELDS)
    if not as_of:
        as_of = _parse_datetime(record.get("fetched_at"))
    quantity = _parse_float(
        data.get("existencia")
        or data.get("stock")
        or data.get("cantidad")
        or data.get("quantity")
        or 0,
    )
    yield StockLevel(
        product_id=str(product_id),
        quantity=max(quantity, 0.0),
        as_of=as_of or datetime.utcnow(),
        warehouse_id=str(warehouse_id) if warehouse_id else None,
    )


def load_stock_levels(
    repo: InventoryRepository,
    *,
    product_id: str | None = None,
    limit: int = 1000,
) -> list[StockLevel]:
    """Carga las existencias almacenadas localmente."""

    records = repo.search_records("variants", limit=limit)
    stock_levels: list[StockLevel] = []
    for record in records:
        for stock in _iter_stock_levels(record):
            if product_id and (
                stock.product_id != product_id and stock.product_code != product_id
            ):
                continue
            stock_levels.append(stock)
    return stock_levels


__all__ = ["load_purchases", "load_sales", "load_stock_levels"]
