"""Cálculo de métricas relacionadas al tiempo de abastecimiento."""
from __future__ import annotations

from datetime import timedelta
from statistics import mean
from typing import Iterable

from .models import Purchase


def _valid_lead_times(purchases: Iterable[Purchase]) -> list[timedelta]:
    values: list[timedelta] = []
    for purchase in purchases:
        lead = purchase.lead_time
        if lead is not None and lead.total_seconds() >= 0:
            values.append(lead)
    return values


def average_lead_time(purchases: Iterable[Purchase]) -> timedelta | None:
    """Calcula el tiempo promedio transcurrido entre compra y recepción."""

    lead_times = _valid_lead_times(purchases)
    if not lead_times:
        return None
    average_seconds = mean(lead.total_seconds() for lead in lead_times)
    return timedelta(seconds=average_seconds)


__all__ = ["average_lead_time"]
