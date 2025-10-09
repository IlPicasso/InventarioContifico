from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analytics import (
    average_lead_time,
    calculate_inventory_turnover,
    calculate_reorder_point,
    calculate_sales_velocity,
    calculate_stock_coverage,
    generate_product_kpis,
    load_purchases,
    load_sales,
    load_stock_levels,
)
from src.persistence import InventoryRepository


@pytest.fixture()
def repo(tmp_path: Path) -> InventoryRepository:
    return InventoryRepository(tmp_path / "inventory.db")


def _iso(dt: datetime) -> str:
    return dt.replace(tzinfo=timezone.utc).isoformat()


@pytest.fixture()
def sample_data(repo: InventoryRepository) -> None:
    repo.upsert_records(
        "purchases",
        [
            {
                "id": "PO-1",
                "fecha_emision": _iso(datetime(2024, 1, 1, 9)),
                "fecha_recepcion": _iso(datetime(2024, 1, 5, 15)),
                "detalles": [
                    {"producto_id": "SKU-1", "cantidad": 10},
                ],
            },
            {
                "id": "PO-2",
                "fecha_emision": _iso(datetime(2024, 1, 10, 12)),
                "recepciones": [
                    {
                        "fecha": _iso(datetime(2024, 1, 14, 10)),
                        "detalles": [
                            {"producto_id": "SKU-1", "cantidad": 8},
                        ],
                    }
                ],
                "detalles": [
                    {"producto_id": "SKU-1", "cantidad": 8},
                ],
            },
        ],
    )

    repo.upsert_records(
        "sales",
        [
            {
                "id": "SA-1",
                "fecha_emision": _iso(datetime(2024, 1, 5, 10)),
                "detalles": [
                    {"producto_id": "SKU-1", "cantidad": 2},
                ],
            },
            {
                "id": "SA-2",
                "fecha_emision": _iso(datetime(2024, 1, 6, 12)),
                "detalles": [
                    {"producto_id": "SKU-1", "cantidad": 3},
                ],
            },
            {
                "id": "SA-3",
                "fecha_emision": _iso(datetime(2024, 1, 15, 16)),
                "detalles": [
                    {"producto_id": "SKU-1", "cantidad": 5},
                ],
            },
        ],
    )

    repo.upsert_records(
        "variants",
        [
            {
                "id": "VAR-1",
                "producto_id": "SKU-1",
                "existencia": 20,
                "fecha_actualizacion": _iso(datetime(2024, 1, 15, 20)),
            },
            {
                "id": "VAR-2",
                "producto_id": "SKU-1",
                "existencia": 12,
                "fecha_actualizacion": _iso(datetime(2024, 1, 16, 8)),
            },
        ],
    )


def test_loaders_build_domain_models(repo: InventoryRepository, sample_data: None) -> None:
    purchases = load_purchases(repo, product_id="SKU-1")
    sales = load_sales(repo, product_id="SKU-1")
    stock_levels = load_stock_levels(repo, product_id="SKU-1")

    assert len(purchases) == 2
    assert purchases[0].product_id == "SKU-1"
    assert purchases[0].lead_time is not None

    assert len(sales) == 3
    assert {sale.quantity for sale in sales} == {2, 3, 5}

    assert len(stock_levels) == 2
    assert all(level.product_id == "SKU-1" for level in stock_levels)


def test_metric_calculations(repo: InventoryRepository, sample_data: None) -> None:
    purchases = load_purchases(repo, product_id="SKU-1")
    sales = load_sales(repo, product_id="SKU-1")
    stock_levels = load_stock_levels(repo, product_id="SKU-1")

    lead_time = average_lead_time(purchases)
    assert lead_time is not None
    assert pytest.approx(lead_time.total_seconds() / 3600, rel=1e-3) == 98

    velocity = calculate_sales_velocity(sales)
    assert velocity is not None
    assert pytest.approx(velocity, rel=1e-3) == pytest.approx(10 / 11, rel=1e-3)

    coverage = calculate_stock_coverage(stock_levels, velocity)
    assert coverage is not None
    assert pytest.approx(coverage, rel=1e-3) == pytest.approx(32 / velocity, rel=1e-3)

    turnover = calculate_inventory_turnover(
        sales,
        average_inventory=sum(level.quantity for level in stock_levels) / len(stock_levels),
        period_days=30,
    )
    assert turnover is not None
    assert pytest.approx(turnover, rel=1e-3) == pytest.approx((10 / 16) * (365 / 30), rel=1e-3)

    reorder_point = calculate_reorder_point(
        daily_demand=velocity,
        lead_time_days=lead_time.total_seconds() / 86_400,
        safety_stock=5,
    )
    assert pytest.approx(reorder_point, rel=1e-3) == pytest.approx((velocity * (98 / 24)) + 5, rel=1e-3)


def test_generate_product_kpis(repo: InventoryRepository, sample_data: None) -> None:
    report = generate_product_kpis(
        repo,
        "SKU-1",
        turnover_period_days=30,
        safety_stock=5,
    )

    assert report["product_id"] == "SKU-1"
    assert pytest.approx(report["average_lead_time_days"], rel=1e-3) == pytest.approx(98 / 24, rel=1e-3)
    assert pytest.approx(report["sales_velocity_per_day"], rel=1e-3) == pytest.approx(10 / 11, rel=1e-3)
    assert pytest.approx(report["stock_coverage_days"], rel=1e-3) == pytest.approx(32 / (10 / 11), rel=1e-3)
    assert pytest.approx(report["inventory_turnover"], rel=1e-3) == pytest.approx((10 / 16) * (365 / 30), rel=1e-3)
    assert pytest.approx(report["reorder_point"], rel=1e-3) == pytest.approx((10 / 11) * (98 / 24) + 5, rel=1e-3)

    # Los objetos originales quedan disponibles para depurar o construir reportes.
    assert len(report["purchases"]) == 2
    assert len(report["sales"]) == 3
    assert len(report["stock_levels"]) == 2
