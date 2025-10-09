"""Utilities to normalise product SKUs used for textile variants."""
from __future__ import annotations


def split_sku_and_size(sku: str | None) -> tuple[str, str | None]:
    """Return the base product code and size from a SKU.

    Contifico variant codes for apparel frequently follow the pattern
    ``CODIGOMADRE/TALLA``. The mother code identifies the product style while the
    suffix after the slash denotes the size ("talla").

    The helper is tolerant to malformed data: empty chunks are ignored and the
    original SKU is returned when no size suffix is present.
    """

    if sku is None:
        return "", None

    text = str(sku).strip()
    if not text:
        return "", None

    if "/" not in text:
        return text, None

    parent, size = text.rsplit("/", 1)
    parent = parent.strip()
    size = size.strip()

    base = parent or text
    normalised_size = size or None
    return base, normalised_size


def format_variant_label(sku: str | None, *, size_label: str = "Talla") -> str:
    """Build a human readable label for a variant SKU."""

    base, size = split_sku_and_size(sku)
    if size:
        return f"{base} ({size_label} {size})"
    return base


__all__ = ["split_sku_and_size", "format_variant_label"]
