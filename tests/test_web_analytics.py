from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.persistence import InventoryRepository
from src.web.app import app, get_repository


def _iso(dt: datetime) -> str:
    return dt.replace(tzinfo=timezone.utc).isoformat()


@pytest.fixture(autouse=True)
def configure_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CONTIFICO_API_KEY", "test-key")
    monkeypatch.setenv("CONTIFICO_API_TOKEN", "test-token")
    monkeypatch.setenv("CONTIFICO_API_BASE_URL", "https://api.test.local")


@pytest.fixture()
def populated_repo(tmp_path: Path) -> InventoryRepository:
    repo = InventoryRepository(tmp_path / "inventory.db")
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
    return repo


@pytest.fixture()
def client(populated_repo: InventoryRepository) -> TestClient:
    get_repository.cache_clear()
    app.dependency_overrides[get_repository] = lambda: populated_repo
    test_client = TestClient(app)
    yield test_client
    app.dependency_overrides.clear()
    get_repository.cache_clear()


def test_analytics_dashboard_renders_metrics(client: TestClient) -> None:
    response = client.get("/analytics")

    assert response.status_code == 200
    assert "KPIs accionables" in response.text
    assert "SKU-1" in response.text
    assert "Descargar reporte en PDF" in response.text


def test_pdf_report_is_generated(client: TestClient) -> None:
    response = client.get("/analytics/report.pdf")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/pdf")
    assert len(response.content) > 2000
