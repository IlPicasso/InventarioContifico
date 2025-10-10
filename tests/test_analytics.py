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
        "categories",
        [
            {"id": "CAT-001", "nombre": "Sastrería"},
            {"id": "CAT-002", "nombre": "General"},
        ],
    )

    repo.upsert_records(
        "products",
        [
            {
                "id": "PROD-1",
                "codigo": "SKU-1/54",
                "nombre": "JACKET XOXO",
                "categoria_id": "CAT-001",
                "categoria_nombre": "Sastrería",
            }
        ],
    )

    repo.upsert_records(
        "purchases",
        [
            {
                "id": "PO-1",
                "fecha_emision": _iso(datetime(2024, 1, 1, 9)),
                "fecha_recepcion": _iso(datetime(2024, 1, 5, 15)),
                "detalles": [
                    {
                        "producto_id": "PROD-1",
                        "producto_codigo": "SKU-1/54",
                        "cantidad": 10,
                    },
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
                    {
                        "producto_id": "PROD-1",
                        "producto_codigo": "SKU-1/54",
                        "cantidad": 8,
                    },
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
                    {
                        "producto_id": "PROD-1",
                        "producto_codigo": "SKU-1/54",
                        "cantidad": 2,
                    },
                ],
            },
            {
                "id": "SA-2",
                "fecha_emision": _iso(datetime(2024, 1, 6, 12)),
                "detalles": [
                    {
                        "producto_id": "PROD-1",
                        "producto_codigo": "SKU-1/54",
                        "cantidad": 3,
                    },
                ],
            },
            {
                "id": "SA-3",
                "fecha_emision": _iso(datetime(2024, 1, 15, 16)),
                "detalles": [
                    {
                        "producto_id": "PROD-1",
                        "producto_codigo": "SKU-1/54",
                        "cantidad": 5,
                    },
                ],
            },
        ],
    )

    repo.upsert_records(
        "variants",
        [
            {
                "id": "VAR-1",
                "producto_id": "PROD-1",
                "codigo": "SKU-1/54",
                "existencia": 20,
                "fecha_actualizacion": _iso(datetime(2024, 1, 15, 20)),
            },
            {
                "id": "VAR-2",
                "producto_id": "PROD-1",
                "codigo": "SKU-1/54",
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
    assert purchases[0].source_product_id == "PROD-1"
    assert purchases[0].product_code == "SKU-1"
    assert purchases[0].variant_size == "54"
    assert purchases[0].lead_time is not None

    assert len(sales) == 3
    assert {sale.quantity for sale in sales} == {2, 3, 5}
    assert sales[0].product_label == "SKU-1 (Talla 54)"
    assert sales[0].source_product_id == "PROD-1"

    assert len(stock_levels) == 2
    assert all(level.product_id == "SKU-1/54" for level in stock_levels)
    assert all(level.source_product_id == "PROD-1" for level in stock_levels)

    base_filtered = load_purchases(repo, product_id="SKU-1")
    assert len(base_filtered) == 2
    assert base_filtered[0].variant_size == "54"

    internal_filtered = load_sales(repo, product_id="PROD-1")
    assert len(internal_filtered) == 3


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


def test_load_sales_supports_records_with_fecha_registro(
    repo: InventoryRepository,
) -> None:
    sale_timestamp = datetime(2024, 3, 5, 11)
    expected_timestamp = sale_timestamp.replace(tzinfo=timezone.utc)
    repo.upsert_records(
        "sales",
        [
            {
                "id": "SA-REG",
                "fecha_registro": _iso(sale_timestamp),
                "detalles": [
                    {"producto_id": "SKU-REG/38", "cantidad": 4},
                ],
            }
        ],
    )

    sales = load_sales(repo)

    assert len(sales) == 1
    assert sales[0].sold_at == expected_timestamp

    report = generate_inventory_report(repo)

    assert report["summary"]["total_sold_units"] == pytest.approx(4.0)
    assert report["summary"]["overall_sales_velocity_per_day"] == pytest.approx(4.0)


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
    assert report["product_name"] == "JACKET XOXO"
    assert report["category_id"] == "CAT-001"
    assert report["category_name"] == "Sastrería"
    assert report["product_internal_ids"] == ["PROD-1"]
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
    assert report["purchases"][0].source_product_id == "PROD-1"
    assert report["stock_levels"][0].source_product_id == "PROD-1"


def test_generate_inventory_report(repo: InventoryRepository, sample_data: None) -> None:
    report = generate_inventory_report(
        repo,
        turnover_period_days=30,
        safety_stock={"PROD-1": 5},
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
    assert product_entry["product_name"] == "JACKET XOXO"
    assert product_entry["category_id"] == "CAT-001"
    assert product_entry["category_name"] == "Sastrería"
    assert product_entry["product_internal_ids"] == ["PROD-1"]
    assert pytest.approx(product_entry["reorder_point"], rel=1e-3) == pytest.approx((10 / 11) * (98 / 24) + 5, rel=1e-3)
    assert product_entry["purchases"][0]["product_id"] == "SKU-1/54"
    assert product_entry["sales"][0]["product_id"] == "SKU-1/54"

    rankings = report["rankings"]
    assert rankings["top_selling_products"][0]["product_id"] == "SKU-1/54"
    assert rankings["top_selling_products"][0]["product_label"] == "SKU-1 (Talla 54)"
    assert rankings["top_selling_products"][0]["category_name"] == "Sastrería"
    assert rankings["top_stock_levels"][0]["product_id"] == "SKU-1/54"
    assert rankings["top_stock_levels"][0]["product_internal_ids"] == ["PROD-1"]

    alerts = report["alerts"]
    assert alerts["low_stock"] == []
    assert alerts["reorder_recommended"] == []
    assert alerts["excess_stock"][0]["product_id"] == "SKU-1/54"
    assert alerts["excess_stock"][0]["product_label"] == "SKU-1 (Talla 54)"
    assert alerts["excess_stock"][0]["product_name"] == "JACKET XOXO"
    assert alerts["stagnant_stock"] == []

    metadata = report["metadata"]
    assert metadata["low_stock_threshold_days"] == 30


def test_inventory_report_supports_latin_dates(repo: InventoryRepository) -> None:
    repo.upsert_records(
        "categories",
        [
            {"id": "CAT-LAT", "nombre": "Promociones"},
        ],
    )
    repo.upsert_records(
        "products",
        [
            {
                "id": "PR-LAT-1",
                "codigo": "SKU-EC-42",
                "nombre": "CAMISA PROMO",
                "categoria_id": "CAT-LAT",
            }
        ],
    )
    repo.upsert_records(
        "purchases",
        [
            {
                "id": "PO-LAT-1",
                "fecha_emision": "05/06/2025",
                "fecha_recepcion": "07/06/2025",
                "detalles": [
                    {
                        "producto_id": "PR-LAT-1",
                        "producto_codigo": "SKU-EC-42",
                        "cantidad": "12",
                    }
                ],
            }
        ],
    )
    repo.upsert_records(
        "sales",
        [
            {
                "id": "SA-LAT-1",
                "fecha_emision": "08/06/2025",
                "detalles": [
                    {
                        "producto_id": "PR-LAT-1",
                        "producto_codigo": "SKU-EC-42",
                        "cantidad": "5",
                    }
                ],
            }
        ],
    )
    repo.upsert_records(
        "variants",
        [
            {
                "id": "VAR-LAT-1",
                "producto_id": "PR-LAT-1",
                "codigo": "SKU-EC-42",
                "existencia": "20",
                "fecha_actualizacion": "09/06/2025",
            }
        ],
    )

    report = generate_inventory_report(repo)

    summary = report["summary"]
    assert summary["total_products"] == 1
    assert summary["total_purchased_units"] == 12.0
    assert summary["total_sold_units"] == 5.0
    assert summary["total_stock_units"] == 20.0

    product = report["products"][0]
    assert product["product_id"] == "SKU-EC-42"
    assert product["total_purchased_units"] == 12.0
    assert product["total_sold_units"] == 5.0
    assert product["current_stock_units"] == 20.0


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
