"""Client helpers for interacting with the Contifico API."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Iterable, Iterator, Optional

import requests

logger = logging.getLogger(__name__)


class ContificoClientError(RuntimeError):
    """Base error for Contifico client failures."""


class ContificoConfigurationError(ContificoClientError):
    """Raised when the client configuration is invalid."""


class ContificoTransportError(ContificoClientError):
    """Raised when the HTTP transport layer fails."""


class ContificoAPIError(ContificoClientError):
    """Raised when the API returns an error payload."""

    def __init__(self, status_code: int, detail: str, payload: Any | None = None) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail
        self.payload = payload


class ContificoClient:
    """Small helper around the Contifico REST API."""

    DEFAULT_BASE_URL = "https://api.contifico.com/sistema/api/v1"
    DEFAULT_PAGE_SIZE = 200

    def __init__(
        self,
        *,
        api_key: str,
        api_token: str,
        base_url: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        api_key = (api_key or "").strip()
        api_token = (api_token or "").strip()
        if not api_key:
            raise ContificoConfigurationError(
                "CONTIFICO_API_KEY es obligatorio para comunicarse con la API."
            )
        if not api_token:
            raise ContificoConfigurationError(
                "CONTIFICO_API_TOKEN es obligatorio para comunicarse con la API."
            )

        self.api_key = api_key
        self.api_token = api_token
        self.base_url = (base_url or self.DEFAULT_BASE_URL).rstrip("/")
        self.timeout = timeout

    def _request(
        self,
        method: str,
        endpoint: str,
        *,
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        headers = {
            "Authorization": self.api_key,
            "X-Api-Token": self.api_token,
            "Accept": "application/json",
            "Content-Type": "application/json; charset=UTF-8",
        }
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                timeout=self.timeout,
            )
        except requests.RequestException as exc:  # pragma: no cover - network failure path
            logger.exception("Contifico transport error %s %s", method, url)
            raise ContificoTransportError(
                f"No se pudo conectar con Contífico: {exc}".rstrip()
            ) from exc

        if response.status_code >= 400:
            raise ContificoAPIError(
                response.status_code,
                self._extract_error_message(response),
                payload=self._safe_json(response),
            )

        if not response.content:
            return None
        return self._safe_json(response)

    @staticmethod
    def _safe_json(response: requests.Response) -> Any | None:
        try:
            return response.json()
        except ValueError:  # pragma: no cover - depende de terceros
            return None

    @staticmethod
    def _extract_error_message(response: requests.Response) -> str:
        payload = ContificoClient._safe_json(response)
        if isinstance(payload, dict):
            for key in ("mensaje", "message", "detail"):
                value = payload.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
        text = response.text.strip()
        if text:
            return text
        return f"Error {response.status_code} al comunicarse con Contífico"

    def _iterate_endpoint(
        self,
        endpoint: str,
        *,
        updated_since: Optional[datetime] = None,
        page_size: int | None = None,
        extra_params: Optional[Dict[str, Any]] = None,
    ) -> Iterator[Dict[str, Any]]:
        size = page_size or self.DEFAULT_PAGE_SIZE
        page = 1
        while True:
            params: Dict[str, Any] = {
                "page": page,
                "page_size": size,
                # Algunos despliegues siguen usando los alias históricos.
                "result_page": page,
                "result_size": size,
            }
            if updated_since is not None:
                params["fecha_modificacion__gte"] = updated_since.isoformat()
            if extra_params:
                params.update(extra_params)

            payload = self._request("GET", endpoint, params=params)
            if payload is None:
                break
            if not isinstance(payload, list):
                raise ContificoAPIError(
                    200,
                    f"El formato de respuesta para {endpoint} no es el esperado.",
                    payload=payload,
                )
            if not payload:
                break
            for item in payload:
                if isinstance(item, dict):
                    yield item
            if len(payload) < size:
                break
            page += 1

    def iter_products(
        self,
        *,
        updated_since: Optional[datetime] = None,
        page_size: int | None = None,
    ) -> Iterable[Dict[str, Any]]:
        """Yield product catalog entries from Contífico."""

        return self._iterate_endpoint(
            "producto/",
            updated_since=updated_since,
            page_size=page_size,
        )

    def iter_purchases(
        self,
        *,
        updated_since: Optional[datetime] = None,
        page_size: int | None = None,
    ) -> Iterable[Dict[str, Any]]:
        """Yield purchase documents registered in Contífico."""

        return self._iterate_endpoint(
            "compra/",
            updated_since=updated_since,
            page_size=page_size,
        )

    def iter_sales(
        self,
        *,
        updated_since: Optional[datetime] = None,
        page_size: int | None = None,
    ) -> Iterable[Dict[str, Any]]:
        """Yield sales documents registered in Contífico."""

        return self._iterate_endpoint(
            "venta/",
            updated_since=updated_since,
            page_size=page_size,
        )

    def iter_warehouses(
        self,
        *,
        updated_since: Optional[datetime] = None,
        page_size: int | None = None,
    ) -> Iterable[Dict[str, Any]]:
        """Yield warehouse definitions configured in Contífico."""

        # El endpoint de bodegas no soporta filtros de fecha, pero mantenemos la firma homogénea.
        return self._iterate_endpoint(
            "bodega/",
            updated_since=updated_since,
            page_size=page_size,
        )
