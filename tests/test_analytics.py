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
    generate_inventory_report,
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
                    {"producto_id": "SKU-1/54", "cantidad": 10},
                ],
            },
            {
                "id": "PO-2",
                "fecha_emision": _iso(datetime(2024, 1, 10, 12)),
                "recepciones": [
                    {
                        "fecha": _iso(datetime(2024, 1, 14, 10)),
                        "detalles": [
                            {"producto_id": "SKU-1/54", "cantidad": 8},
                        ],
                    }
                ],
                "detalles": [
                    {"producto_id": "SKU-1/54", "cantidad": 8},
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
                    {"producto_id": "SKU-1/54", "cantidad": 2},
                ],
            },
            {
                "id": "SA-2",
                "fecha_emision": _iso(datetime(2024, 1, 6, 12)),
                "detalles": [
                    {"producto_id": "SKU-1/54", "cantidad": 3},
                ],
            },
            {
                "id": "SA-3",
                "fecha_emision": _iso(datetime(2024, 1, 15, 16)),
                "detalles": [
                    {"producto_id": "SKU-1/54", "cantidad": 5},
                ],
            },
        ],
    )

    repo.upsert_records(
        "variants",
        [
            {
                "id": "VAR-1",
                "producto_id": "SKU-1/54",
                "existencia": 20,
                "fecha_actualizacion": _iso(datetime(2024, 1, 15, 20)),
            },
            {
                "id": "VAR-2",
                "producto_id": "SKU-1/54",
                "existencia": 12,
                "fecha_actualizacion": _iso(datetime(2024, 1, 16, 8)),
            },
        ],
    )


def test_loaders_build_domain_models(repo: InventoryRepository, sample_data: None) -> None:
    purchases = load_purchases(repo, product_id="SKU-1/54")
    sales = load_sales(repo, product_id="SKU-1/54")
    stock_levels = load_stock_levels(repo, product_id="SKU-1/54")

    assert len(purchases) == 2
    assert purchases[0].product_id == "SKU-1/54"
    assert purchases[0].product_code == "SKU-1"
    assert purchases[0].variant_size == "54"
    assert purchases[0].lead_time is not None

    assert len(sales) == 3
    assert {sale.quantity for sale in sales} == {2, 3, 5}
    assert sales[0].product_label == "SKU-1 (Talla 54)"

    assert len(stock_levels) == 2
    assert all(level.product_id == "SKU-1/54" for level in stock_levels)

    base_filtered = load_purchases(repo, product_id="SKU-1")
    assert len(base_filtered) == 2
    assert base_filtered[0].variant_size == "54"


def test_loaders_fallback_to_documents(repo: InventoryRepository) -> None:
    repo.upsert_records(
        "documents",
        [
            {
                "id": "DOC-PO-1",
                "tipo": "LQC",
                "tipo_registro": "PRO",
                "fecha_emision": _iso(datetime(2024, 2, 1, 9)),
                "fecha_recepcion": _iso(datetime(2024, 2, 3, 9)),
                "detalles": [
                    {"producto_id": "SKU-2/42", "cantidad": 4},
                ],
            },
            {
                "id": "DOC-SA-1",
                "tipo": "FAC",
                "tipo_registro": "CLI",
                "fecha_emision": _iso(datetime(2024, 2, 5, 11)),
                "detalles": [
                    {"producto_id": "SKU-2/42", "cantidad": 3},
                ],
            },
        ],
    )

    purchases = load_purchases(repo, product_id="SKU-2/42")
    sales = load_sales(repo, product_id="SKU-2/42")

    assert len(purchases) == 1
    assert purchases[0].purchase_id == "DOC-PO-1"
    assert purchases[0].quantity == 4
    assert len(sales) == 1
    assert sales[0].sale_id == "DOC-SA-1"
    assert sales[0].quantity == 3


def test_metric_calculations(repo: InventoryRepository, sample_data: None) -> None:
    purchases = load_purchases(repo, product_id="SKU-1/54")
    sales = load_sales(repo, product_id="SKU-1/54")
    stock_levels = load_stock_levels(repo, product_id="SKU-1/54")

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
        "SKU-1/54",
        turnover_period_days=30,
        safety_stock=5,
    )

    assert report["product_id"] == "SKU-1/54"
    assert report["product_code"] == "SKU-1"
    assert report["variant_size"] == "54"
    assert report["product_label"] == "SKU-1 (Talla 54)"
    assert pytest.approx(report["average_lead_time_days"], rel=1e-3) == pytest.approx(98 / 24, rel=1e-3)
    assert pytest.approx(report["sales_velocity_per_day"], rel=1e-3) == pytest.approx(10 / 11, rel=1e-3)
    assert pytest.approx(report["stock_coverage_days"], rel=1e-3) == pytest.approx(32 / (10 / 11), rel=1e-3)
    assert pytest.approx(report["inventory_turnover"], rel=1e-3) == pytest.approx((10 / 16) * (365 / 30), rel=1e-3)
    assert pytest.approx(report["reorder_point"], rel=1e-3) == pytest.approx((10 / 11) * (98 / 24) + 5, rel=1e-3)
    assert report["total_purchased_units"] == 18
    assert report["total_sold_units"] == 10
    assert report["current_stock_units"] == 32

    # Los objetos originales quedan disponibles para depurar o construir reportes.
    assert len(report["purchases"]) == 2
    assert len(report["sales"]) == 3
    assert len(report["stock_levels"]) == 2


def test_generate_inventory_report(repo: InventoryRepository, sample_data: None) -> None:
    report = generate_inventory_report(
        repo,
        turnover_period_days=30,
        safety_stock={"SKU-1/54": 5},
        low_stock_threshold_days=30,
        excess_stock_threshold_days=30,
    )

    summary = report["summary"]
    assert summary["total_products"] == 1
    assert pytest.approx(summary["total_stock_units"], rel=1e-3) == 32
    assert pytest.approx(summary["overall_sales_velocity_per_day"], rel=1e-3) == pytest.approx(10 / 11, rel=1e-3)
    assert pytest.approx(summary["overall_stock_coverage_days"], rel=1e-3) == pytest.approx(32 / (10 / 11), rel=1e-3)

    products = report["products"]
    assert len(products) == 1
    product_entry = products[0]
    assert product_entry["product_id"] == "SKU-1/54"
    assert product_entry["product_code"] == "SKU-1"
    assert product_entry["variant_size"] == "54"
    assert product_entry["product_label"] == "SKU-1 (Talla 54)"
    assert product_entry["purchases"][0]["product_id"] == "SKU-1/54"
    assert product_entry["sales"][0]["product_id"] == "SKU-1/54"

    rankings = report["rankings"]
    assert rankings["top_selling_products"][0]["product_id"] == "SKU-1/54"
    assert rankings["top_selling_products"][0]["product_label"] == "SKU-1 (Talla 54)"
    assert rankings["top_stock_levels"][0]["product_id"] == "SKU-1/54"

    alerts = report["alerts"]
    assert alerts["low_stock"] == []
    assert alerts["reorder_recommended"] == []
    assert alerts["excess_stock"][0]["product_id"] == "SKU-1/54"
    assert alerts["excess_stock"][0]["product_label"] == "SKU-1 (Talla 54)"
    assert alerts["stagnant_stock"] == []

    metadata = report["metadata"]
    assert metadata["low_stock_threshold_days"] == 30


def test_stock_levels_fallback_to_products(repo: InventoryRepository) -> None:
    repo.upsert_records(
        "products",
        [
            {
                "id": "SIM-1",
                "tipo_producto": "SIM",
                "cantidad_stock": "12.5",
                "fecha_actualizacion": _iso(datetime(2024, 2, 1, 10)),
            },
            {
                "id": "SIM-2",
                "tipo_producto": "SIM",
                "cantidad_stock": "0",
                "fecha_modificacion": _iso(datetime(2024, 2, 1, 9)),
            },
        ],
    )

    levels = load_stock_levels(repo)
    quantities_by_id = {level.product_id: level.quantity for level in levels}

    assert quantities_by_id["SIM-1"] == pytest.approx(12.5)
    assert quantities_by_id["SIM-2"] == pytest.approx(0.0)

    filtered = load_stock_levels(repo, product_id="SIM-1")
    assert len(filtered) == 1
    assert filtered[0].product_id == "SIM-1"
