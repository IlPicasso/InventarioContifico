"""Modelos de datos utilizados por los módulos de analítica."""
from __future__ import annotations

from datetime import datetime, timedelta
from pydantic import BaseModel, ConfigDict, Field, field_validator

from .sku import format_variant_label, split_sku_and_size


class Purchase(BaseModel):
    """Representa una línea de compra asociada a un producto."""

    model_config = ConfigDict(extra="ignore")

    purchase_id: str = Field(..., description="Identificador único del documento de compra")
    product_id: str = Field(..., description="Identificador del producto o variante comprada")
    source_product_id: str | None = Field(
        None,
        description=(
            "Identificador original del producto en Contifico cuando difiere del código"
        ),
    )
    ordered_at: datetime = Field(..., description="Fecha en la que se emitió la orden de compra")
    received_at: datetime | None = Field(
        None, description="Fecha en la que el producto fue recibido"
    )
    quantity: float = Field(..., ge=0, description="Cantidad recibida o esperada")
    warehouse_id: str | None = Field(
        None, description="Identificador de la bodega relacionada con la compra"
    )
    supplier_id: str | None = Field(
        None, description="Identificador del proveedor asociado"
    )

    @field_validator("purchase_id", "product_id", mode="before")
    @classmethod
    def _strip(cls, value: str) -> str:  # noqa: D401
        """Normaliza los campos de texto eliminando espacios en blanco."""

        return value.strip()

    @field_validator("source_product_id", mode="before")
    @classmethod
    def _strip_optional(cls, value: str | None) -> str | None:
        if value is None:
            return value
        return value.strip() or None

    @property
    def lead_time(self) -> timedelta | None:
        """Devuelve el tiempo transcurrido entre la compra y la recepción."""

        if not self.received_at:
            return None
        return self.received_at - self.ordered_at

    @property
    def product_code(self) -> str:
        """Código base del producto sin sufijo de talla."""

        code, _ = split_sku_and_size(self.product_id)
        return code

    @property
    def variant_size(self) -> str | None:
        """Talla del producto cuando está disponible."""

        _, size = split_sku_and_size(self.product_id)
        return size

    @property
    def product_label(self) -> str:
        """Etiqueta amigable para reportes, combinando código y talla."""

        return format_variant_label(self.product_id)


class Sale(BaseModel):
    """Representa una línea de venta de producto."""

    model_config = ConfigDict(extra="ignore")

    sale_id: str = Field(..., description="Identificador del documento de venta")
    product_id: str = Field(..., description="Producto vendido")
    source_product_id: str | None = Field(
        None,
        description="Identificador original del producto en Contifico para la línea de venta",
    )
    sold_at: datetime = Field(..., description="Fecha de la transacción")
    quantity: float = Field(..., ge=0, description="Cantidad vendida en unidades")
    warehouse_id: str | None = Field(None, description="Bodega de despacho")
    customer_id: str | None = Field(None, description="Cliente asociado a la venta")

    @field_validator("sale_id", "product_id", mode="before")
    @classmethod
    def _strip(cls, value: str) -> str:
        return value.strip()

    @field_validator("source_product_id", mode="before")
    @classmethod
    def _strip_optional(cls, value: str | None) -> str | None:
        if value is None:
            return value
        return value.strip() or None

    @property
    def product_code(self) -> str:
        code, _ = split_sku_and_size(self.product_id)
        return code

    @property
    def variant_size(self) -> str | None:
        _, size = split_sku_and_size(self.product_id)
        return size

    @property
    def product_label(self) -> str:
        return format_variant_label(self.product_id)


class StockLevel(BaseModel):
    """Representa una existencia disponible para un producto."""

    model_config = ConfigDict(extra="ignore")

    product_id: str = Field(..., description="Producto asociado al inventario")
    source_product_id: str | None = Field(
        None,
        description="Identificador original del producto usado en el origen del inventario",
    )
    quantity: float = Field(..., ge=0, description="Cantidad disponible")
    as_of: datetime = Field(..., description="Fecha de corte de la medición")
    warehouse_id: str | None = Field(None, description="Bodega a la que pertenece la existencia")

    @field_validator("product_id", mode="before")
    @classmethod
    def _strip(cls, value: str) -> str:
        return value.strip()

    @field_validator("source_product_id", mode="before")
    @classmethod
    def _strip_optional(cls, value: str | None) -> str | None:
        if value is None:
            return value
        return value.strip() or None

    @property
    def product_code(self) -> str:
        code, _ = split_sku_and_size(self.product_id)
        return code

    @property
    def variant_size(self) -> str | None:
        _, size = split_sku_and_size(self.product_id)
        return size

    @property
    def product_label(self) -> str:
        return format_variant_label(self.product_id)


__all__ = ["Purchase", "Sale", "StockLevel"]
