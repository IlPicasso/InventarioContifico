"""Client helpers for interacting with the Contifico API."""
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, Iterable, Iterator, Optional

import requests

logger = logging.getLogger(__name__)


def _serialise_for_log(data: Any, limit: int = 2000) -> str:
    """Return a JSON representation of ``data`` truncated for logging."""

    if data is None:
        return "null"
    try:
        rendered = json.dumps(data, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        rendered = repr(data)
    if len(rendered) > limit:
        return f"{rendered[:limit]}… (truncated)"
    return rendered


class ContificoClientError(RuntimeError):
    """Base error for Contifico client failures."""


class ContificoConfigurationError(ContificoClientError):
    """Raised when the client configuration is invalid."""


class ContificoTransportError(ContificoClientError):
    """Raised when the HTTP transport layer fails."""


class ContificoAPIError(ContificoClientError):
    """Raised when the API returns an error payload."""

    def __init__(
        self,
        status_code: int,
        detail: str,
        payload: Any | None = None,
        *,
        context: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail
        self.payload = payload
        self.context = context or {}

    def __str__(self) -> str:  # pragma: no cover - representation helper
        context_repr = f", contexto={self.context}" if self.context else ""
        return f"{self.detail} (status={self.status_code}{context_repr})"


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
        default_page_size: int | None = None,
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
        self.default_page_size = (
            default_page_size if default_page_size and default_page_size > 0 else self.DEFAULT_PAGE_SIZE
        )

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
        logger.debug(
            "Contifico request %s %s params=%s",
            method,
            url,
            _serialise_for_log(params or {}),
        )
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

        payload = self._safe_json(response)
        if response.status_code >= 400:
            logger.error(
                "Contifico API error %s %s status=%s params=%s body=%s",
                method,
                url,
                response.status_code,
                _serialise_for_log(params or {}),
                _serialise_for_log(payload),
            )
            raise ContificoAPIError(
                response.status_code,
                self._extract_error_message(response),
                payload=payload,
                context={
                    "method": method,
                    "url": url,
                    "params": params or {},
                },
            )

        if not response.content:
            logger.debug(
                "Contifico response %s %s status=%s body=<empty>",
                method,
                url,
                response.status_code,
            )
            return None
        logger.debug(
            "Contifico response %s %s status=%s body=%s",
            method,
            url,
            response.status_code,
            _serialise_for_log(payload),
        )
        return payload

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
        legacy_aliases: bool = True,
    ) -> Iterator[Dict[str, Any]]:
        size = page_size or self.default_page_size
        base_params: Dict[str, Any] = {}
        if updated_since is not None:
            base_params["fecha_modificacion__gte"] = updated_since.isoformat()
        if extra_params:
            base_params.update(extra_params)

        page = 1
        while True:
            params: Dict[str, Any] = {
                "page": page,
                "page_size": size,
            }
            if legacy_aliases:
                # Algunos despliegues siguen usando los alias históricos ``result_*``.
                params["result_page"] = page
                params["result_size"] = size
            if base_params:
                params.update(base_params)

            payload = self._request("GET", endpoint, params=params)
            if payload is None:
                break

            # Algunos endpoints de Contífico devuelven directamente la lista de
            # resultados (legacy) mientras que otros siguen la convención de un
            # objeto paginado con ``results`` y ``next``. Soportamos ambos para
            # mantener compatibilidad independientemente de la versión del API.
            has_next = False
            if isinstance(payload, list):
                items = payload
                has_next = len(items) >= size
            elif isinstance(payload, dict):
                results = payload.get("results")
                if not isinstance(results, list):
                    raise ContificoAPIError(
                        200,
                        f"El formato de respuesta para {endpoint} no es el esperado.",
                        payload=payload,
                        context={"endpoint": endpoint, "params": params},
                    )
                items = results
                next_url = payload.get("next")
                has_next = bool(next_url)
            else:
                raise ContificoAPIError(
                    200,
                    f"El formato de respuesta para {endpoint} no es el esperado.",
                    payload=payload,
                    context={"endpoint": endpoint, "params": params},
                )

            if not items:
                break

            for item in items:
                if isinstance(item, dict):
                    yield item

            if not has_next:
                break
            page += 1

    def _iterate_endpoint_candidates(
        self,
        endpoints: Iterable[str],
        *,
        updated_since: Optional[datetime] = None,
        page_size: int | None = None,
        extra_params: Optional[Dict[str, Any]] = None,
    ) -> Iterator[Dict[str, Any]]:
        """Iterate through a sequence of endpoints using the first one that works."""

        last_error: ContificoAPIError | None = None
        for endpoint in endpoints:
            try:
                yield from self._iterate_endpoint(
                    endpoint,
                    updated_since=updated_since,
                    page_size=page_size,
                    extra_params=extra_params,
                )
                return
            except ContificoAPIError as exc:
                if not self._should_retry_endpoint_candidate(exc):
                    raise
                logger.debug(
                    "Endpoint %s rechazado (%s), intentando alternativa",
                    endpoint,
                    exc,
                )
                last_error = exc
                continue

        if last_error is not None:
            raise last_error

    @staticmethod
    def _should_retry_endpoint_candidate(error: ContificoAPIError) -> bool:
        """Return True if a failed candidate should be retried with the next option."""

        if error.status_code == 404:
            return True

        if error.status_code == 400:
            # Algunos despliegues responden ``400 id incorrecto`` cuando el nuevo
            # endpoint ``documento/`` aún no está disponible para listados. En ese
            # caso debemos intentar con el alias histórico.
            message = ""
            payload = error.payload
            if isinstance(payload, dict):
                for key in ("mensaje", "message", "detail"):
                    value = payload.get(key)
                    if isinstance(value, str):
                        message = value
                        break
            if not message and isinstance(error.detail, str):
                message = error.detail
            if "id incorrecto" in message.lower():
                return True

        return False

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

        return self._iterate_endpoint_candidates(
            ("documento/compra/", "compra/"),
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

        return self._iterate_endpoint_candidates(
            ("documento/venta/", "venta/"),
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

    def iter_inventory_movements(
        self,
        *,
        updated_since: Optional[datetime] = None,
        page_size: int | None = None,
    ) -> Iterable[Dict[str, Any]]:
        """Yield inventory movement documents (traslados, ingresos, egresos)."""

        return self._iterate_endpoint(
            "movimiento-inventario/",
            updated_since=updated_since,
            page_size=page_size,
            legacy_aliases=False,
        )
