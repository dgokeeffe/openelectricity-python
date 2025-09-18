"""
OpenElectricity API Client

This module provides both synchronous and asynchronous clients for the OpenElectricity API.
"""

from datetime import datetime
from typing import Any, TypeVar

import httpx

from openelectricity.logging import get_logger
from openelectricity.models.facilities import FacilityResponse
from openelectricity.models.timeseries import TimeSeriesResponse
from openelectricity.models.user import OpennemUserResponse
from openelectricity.settings_schema import settings
from openelectricity.types import (
    DataInterval,
    DataMetric,
    DataPrimaryGrouping,
    DataSecondaryGrouping,
    MarketMetric,
    NetworkCode,
    UnitFueltechType,
    UnitStatusType,
)

T = TypeVar("T")
logger = get_logger("client")


class OpenElectricityError(Exception):
    """Base exception for OpenElectricity API errors."""

    pass


class APIError(OpenElectricityError):
    """Exception raised for API errors."""

    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"API Error {status_code}: {detail}")


class BaseOEClient:
    """
    Base client for the OpenElectricity API.

    Args:
        api_key: Optional API key for authentication. If not provided, will look for
                OPENELECTRICITY_API_KEY environment variable.
        base_url: Optional base URL for the API. Defaults to production API.
    """

    def __init__(self, api_key: str | None = None, base_url: str | None = None) -> None:
        # Ensure base_url has a trailing slash for aiohttp ClientSession
        if base_url:
            self.base_url = base_url.rstrip("/") + "/"
        else:
            self.base_url = settings.base_url
            if not self.base_url.endswith("/"):
                self.base_url += "/"
        self.api_key = api_key or settings.api_key

        if not self.api_key:
            raise OpenElectricityError(
                "API key must be provided either as argument or via OPENELECTRICITY_API_KEY environment variable"
            )

        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        logger.debug("Initialized client with base URL: %s", self.base_url)


class OEClient(BaseOEClient):
    """
    Unified client for the OpenElectricity API using httpx.
    
    This client can be used in both synchronous and asynchronous contexts:
    - For sync usage: client = OEClient(); response = client.get_facilities()
    - For async usage: async with OEClient() as client: response = await client.get_facilities()
    
    Features:
    - Uses httpx for both sync and async HTTP requests
    - Implements proper error handling and logging
    - Provides context manager support for both sync and async
    - Handles parameter validation and URL construction
    - Automatic client type detection based on usage context
    """

    def __init__(self, api_key: str | None = None, base_url: str | None = None) -> None:
        super().__init__(api_key, base_url)
        self._sync_client: httpx.Client | None = None
        self._async_client: httpx.AsyncClient | None = None
        self._is_async_context = False
        logger.debug("Initialized unified client")

    def get_spark_session(self) -> "SparkSession":
        """
        Get a Spark session that works in both Databricks and local environments.
        
        This method provides access to the centralized Spark session management
        from the spark_utils module.
        
        Returns:
            SparkSession: Configured Spark session
            
        Raises:
            ImportError: If PySpark is not available
            Exception: If unable to create Spark session
        """
        from openelectricity.spark_utils import get_spark_session
        return get_spark_session()

    def is_spark_available(self) -> bool:
        """
        Check if PySpark is available in the current environment.
        
        Returns:
            bool: True if PySpark can be imported, False otherwise
        """
        from openelectricity.spark_utils import is_spark_available
        return is_spark_available()

    def _ensure_sync_client(self) -> httpx.Client:
        """Ensure sync client is initialized and return it."""
        if self._sync_client is None:
            logger.debug("Creating new sync httpx client")
            self._sync_client = httpx.Client(
                headers=self.headers,
                timeout=httpx.Timeout(10.0, connect=5.0, read=30.0),
                limits=httpx.Limits(
                    max_keepalive_connections=50,  # Increased for better connection reuse
                    max_connections=200,  # Increased for higher concurrency
                    keepalive_expiry=30.0  # Keep connections alive longer
                ),
                http2=True,  # Enable HTTP/2 for better performance
                follow_redirects=True,
                verify=True
            )
        return self._sync_client

    async def _ensure_async_client(self) -> httpx.AsyncClient:
        """Ensure async client is initialized and return it."""
        if self._async_client is None or self._async_client.is_closed:
            logger.debug("Creating new async httpx client")
            self._async_client = httpx.AsyncClient(
                headers=self.headers,
                timeout=httpx.Timeout(10.0, connect=5.0, read=30.0),
                limits=httpx.Limits(
                    max_keepalive_connections=50,  # Increased for better connection reuse
                    max_connections=200,  # Increased for higher concurrency
                    keepalive_expiry=30.0  # Keep connections alive longer
                ),
                http2=True,  # Enable HTTP/2 for better performance
                follow_redirects=True,
                verify=True
            )
        return self._async_client

    def _handle_response(self, response: httpx.Response) -> dict[str, Any] | list[dict[str, Any]]:
        """Handle API response and raise appropriate errors."""
        if not response.is_success:
            try:
                detail = response.json().get("detail", response.reason_phrase)
            except Exception:
                detail = response.reason_phrase
            logger.error("API error: %s - %s", response.status_code, detail)
            raise APIError(response.status_code, detail)

        logger.debug("Received successful response: %s", response.status_code)

        # Add this line to see the raw JSON response
        raw_json = response.json()
        logger.debug("Raw JSON response: %s", raw_json)

        return raw_json

    def _build_url(self, endpoint: str) -> str:
        """Build full URL from endpoint."""
        # Ensure endpoint starts with / and remove any double slashes
        if not endpoint.startswith('/'):
            endpoint = '/' + endpoint
        return f"{self.base_url.rstrip('/')}/v4{endpoint}"

    def _clean_params(self, params: dict[str, Any]) -> dict[str, Any]:
        """Remove None values from parameters."""
        return {k: v for k, v in params.items() if v is not None}

    def preload_client(self) -> None:
        """
        Preload the sync client to avoid initialization overhead on first request.
        This is particularly useful for performance-critical applications.
        """
        self._ensure_sync_client()
        logger.debug("Preloaded sync client for better performance")

    async def preload_async_client(self) -> None:
        """
        Preload the async client to avoid initialization overhead on first request.
        This is particularly useful for performance-critical applications.
        """
        await self._ensure_async_client()
        logger.debug("Preloaded async client for better performance")

    def get_facilities(
        self,
        facility_code: list[str] | None = None,
        status_id: list[UnitStatusType] | None = None,
        fueltech_id: list[UnitFueltechType] | None = None,
        network_id: list[str] | None = None,
        network_region: str | None = None,
    ) -> FacilityResponse:
        """Get a list of facilities."""
        logger.debug("Getting facilities")
        client = self._ensure_sync_client()

        params = {
            "facility_code": facility_code,
            "status_id": [s.value for s in status_id] if status_id else None,
            "fueltech_id": [f.value for f in fueltech_id] if fueltech_id else None,
            "network_id": network_id,
            "network_region": network_region,
        }
        params = self._clean_params(params)
        logger.debug("Request parameters: %s", params)

        url = self._build_url("/facilities/")
        response = client.get(url, params=params)
        data = self._handle_response(response)
        return FacilityResponse.model_validate(data)

    def get_network_data(
        self,
        network_code: NetworkCode,
        metrics: list[DataMetric],
        interval: DataInterval | None = None,
        date_start: datetime | None = None,
        date_end: datetime | None = None,
        primary_grouping: DataPrimaryGrouping | None = None,
        secondary_grouping: DataSecondaryGrouping | None = None,
    ) -> TimeSeriesResponse:
        """Get network data for specified metrics."""
        logger.debug(
            "Getting network data for %s (metrics: %s, interval: %s)",
            network_code,
            metrics,
            interval,
        )
        client = self._ensure_sync_client()

        params = {
            "metrics": [m.value for m in metrics],
            "interval": interval,
            "date_start": date_start.isoformat() if date_start else None,
            "date_end": date_end.isoformat() if date_end else None,
            "primary_grouping": primary_grouping,
            "secondary_grouping": secondary_grouping,
        }
        params = self._clean_params(params)
        logger.debug("Request parameters: %s", params)

        url = self._build_url(f"/data/network/{network_code}")
        response = client.get(url, params=params)
        data = self._handle_response(response)
        return TimeSeriesResponse.model_validate(data)

    def get_facility_data(
        self,
        network_code: NetworkCode,
        facility_code: str | list[str],
        metrics: list[DataMetric],
        interval: DataInterval | None = None,
        date_start: datetime | None = None,
        date_end: datetime | None = None,
    ) -> TimeSeriesResponse:
        """Get facility data for specified metrics."""
        logger.debug(
            "Getting facility data for %s/%s (metrics: %s, interval: %s)",
            network_code,
            facility_code,
            metrics,
            interval,
        )
        client = self._ensure_sync_client()

        params = {
            "facility_code": facility_code,
            "metrics": [m.value for m in metrics],
            "interval": interval,
            "date_start": date_start.isoformat() if date_start else None,
            "date_end": date_end.isoformat() if date_end else None,
        }
        params = self._clean_params(params)
        logger.debug("Request parameters: %s", params)

        url = self._build_url(f"/data/facilities/{network_code}")
        response = client.get(url, params=params)
        data = self._handle_response(response)
        return TimeSeriesResponse.model_validate(data)

    def get_market(
        self,
        network_code: NetworkCode,
        metrics: list[MarketMetric],
        interval: DataInterval | None = None,
        date_start: datetime | None = None,
        date_end: datetime | None = None,
        primary_grouping: DataPrimaryGrouping | None = None,
        network_region: str | None = None,
    ) -> TimeSeriesResponse:
        """Get market data for specified metrics."""
        logger.debug(
            "Getting market data for %s (metrics: %s, interval: %s, region: %s)",
            network_code,
            metrics,
            interval,
            network_region,
        )
        client = self._ensure_sync_client()

        params = {
            "metrics": [m.value for m in metrics],
            "interval": interval,
            "date_start": date_start.isoformat() if date_start else None,
            "date_end": date_end.isoformat() if date_end else None,
            "primary_grouping": primary_grouping,
            "network_region": network_region,
        }
        params = self._clean_params(params)
        logger.debug("Request parameters: %s", params)

        url = self._build_url(f"/market/network/{network_code}")
        response = client.get(url, params=params)
        data = self._handle_response(response)
        return TimeSeriesResponse.model_validate(data)

    def get_current_user(self) -> OpennemUserResponse:
        """Get current user information."""
        logger.debug("Getting current user information")
        client = self._ensure_sync_client()

        url = self._build_url("/me")
        response = client.get(url)
        data = self._handle_response(response)
        return OpennemUserResponse.model_validate(data)

    def close(self) -> None:
        """Close the underlying HTTP clients."""
        if self._sync_client:
            logger.debug("Closing sync httpx client")
            self._sync_client.close()
            self._sync_client = None

        if self._async_client and not self._async_client.is_closed:
            logger.debug("Closing async httpx client")
            # Note: async client should be closed with aclose() in async context
            # This is a fallback for sync close()
            import asyncio
            try:
                asyncio.run(self._async_client.aclose())
            except RuntimeError:
                # If we're already in an event loop, just mark as closed
                pass
            self._async_client = None

    def __enter__(self) -> "OEClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def __del__(self) -> None:
        """Ensure clients are closed when object is garbage collected."""
        self.close()

    # Async versions of all methods
    async def get_facilities_async(
        self,
        facility_code: list[str] | None = None,
        status_id: list[UnitStatusType] | None = None,
        fueltech_id: list[UnitFueltechType] | None = None,
        network_id: list[str] | None = None,
        network_region: str | None = None,
    ) -> FacilityResponse:
        """Get a list of facilities (async version)."""
        logger.debug("Getting facilities (async)")
        client = await self._ensure_async_client()

        params = {
            "facility_code": facility_code,
            "status_id": [s.value for s in status_id] if status_id else None,
            "fueltech_id": [f.value for f in fueltech_id] if fueltech_id else None,
            "network_id": network_id,
            "network_region": network_region,
        }
        params = self._clean_params(params)
        logger.debug("Request parameters: %s", params)

        url = self._build_url("/facilities/")
        response = await client.get(url, params=params)
        data = self._handle_response(response)
        return FacilityResponse.model_validate(data)

    async def get_network_data_async(
        self,
        network_code: NetworkCode,
        metrics: list[DataMetric],
        interval: DataInterval | None = None,
        date_start: datetime | None = None,
        date_end: datetime | None = None,
        primary_grouping: DataPrimaryGrouping | None = None,
        secondary_grouping: DataSecondaryGrouping | None = None,
    ) -> TimeSeriesResponse:
        """Get network data for specified metrics (async version)."""
        logger.debug(
            "Getting network data for %s (metrics: %s, interval: %s) (async)",
            network_code,
            metrics,
            interval,
        )
        client = await self._ensure_async_client()

        params = {
            "metrics": [m.value for m in metrics],
            "interval": interval,
            "date_start": date_start.isoformat() if date_start else None,
            "date_end": date_end.isoformat() if date_end else None,
            "primary_grouping": primary_grouping,
            "secondary_grouping": secondary_grouping,
        }
        params = self._clean_params(params)
        logger.debug("Request parameters: %s", params)

        url = self._build_url(f"/data/network/{network_code}")
        response = await client.get(url, params=params)
        data = self._handle_response(response)
        return TimeSeriesResponse.model_validate(data)

    async def get_facility_data_async(
        self,
        network_code: NetworkCode,
        facility_code: str | list[str],
        metrics: list[DataMetric],
        interval: DataInterval | None = None,
        date_start: datetime | None = None,
        date_end: datetime | None = None,
    ) -> TimeSeriesResponse:
        """Get facility data for specified metrics (async version)."""
        logger.debug(
            "Getting facility data for %s/%s (metrics: %s, interval: %s) (async)",
            network_code,
            facility_code,
            metrics,
            interval,
        )
        client = await self._ensure_async_client()

        params = {
            "facility_code": facility_code,
            "metrics": [m.value for m in metrics],
            "interval": interval,
            "date_start": date_start.isoformat() if date_start else None,
            "date_end": date_end.isoformat() if date_end else None,
        }
        params = self._clean_params(params)
        logger.debug("Request parameters: %s", params)

        url = self._build_url(f"/data/facilities/{network_code}")
        response = await client.get(url, params=params)
        data = self._handle_response(response)
        return TimeSeriesResponse.model_validate(data)

    async def get_market_async(
        self,
        network_code: NetworkCode,
        metrics: list[MarketMetric],
        interval: DataInterval | None = None,
        date_start: datetime | None = None,
        date_end: datetime | None = None,
        primary_grouping: DataPrimaryGrouping | None = None,
        network_region: str | None = None,
    ) -> TimeSeriesResponse:
        """Get market data for specified metrics (async version)."""
        logger.debug(
            "Getting market data for %s (metrics: %s, interval: %s, region: %s) (async)",
            network_code,
            metrics,
            interval,
            network_region,
        )
        client = await self._ensure_async_client()

        params = {
            "metrics": [m.value for m in metrics],
            "interval": interval,
            "date_start": date_start.isoformat() if date_start else None,
            "date_end": date_end.isoformat() if date_end else None,
            "primary_grouping": primary_grouping,
            "network_region": network_region,
        }
        params = self._clean_params(params)
        logger.debug("Request parameters: %s", params)

        url = self._build_url(f"/market/network/{network_code}")
        response = await client.get(url, params=params)
        data = self._handle_response(response)
        return TimeSeriesResponse.model_validate(data)

    async def get_current_user_async(self) -> OpennemUserResponse:
        """Get current user information (async version)."""
        logger.debug("Getting current user information (async)")
        client = await self._ensure_async_client()

        url = self._build_url("/me")
        response = await client.get(url)
        data = self._handle_response(response)
        return OpennemUserResponse.model_validate(data)

    async def aclose(self) -> None:
        """Close the async HTTP client."""
        if self._async_client and not self._async_client.is_closed:
            logger.debug("Closing async httpx client")
            await self._async_client.aclose()
            self._async_client = None

    async def __aenter__(self) -> "OEClient":
        await self._ensure_async_client()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.aclose()





