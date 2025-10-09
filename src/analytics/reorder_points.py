"""Funciones para calcular puntos de reorden basados en la demanda."""
from __future__ import annotations


def calculate_reorder_point(
    *, daily_demand: float, lead_time_days: float, safety_stock: float = 0.0
) -> float:
    """Calcula el punto de reorden esperado para un producto."""

    if daily_demand < 0:
        raise ValueError("La demanda diaria no puede ser negativa")
    if lead_time_days < 0:
        raise ValueError("El tiempo de entrega no puede ser negativo")
    if safety_stock < 0:
        raise ValueError("El stock de seguridad no puede ser negativo")

    return (daily_demand * lead_time_days) + safety_stock


__all__ = ["calculate_reorder_point"]
