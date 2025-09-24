"""
OpenElectricity Python SDK

This package provides a Python client for interacting with the OpenElectricity API.
"""

from openelectricity.client import OEClient
from openelectricity.exceptions import (
    APIError,
    AuthenticationError,
    ClientError,
    NetworkError,
    OpenElectricityError,
    RateLimitError,
    RetryConfig,
    ServerError,
    TimeoutError,
)

__name__ = "openelectricity"

__version__ = "0.10.0"

__all__ = [
    "OEClient",
    "OpenElectricityError",
    "APIError",
    "AuthenticationError",
    "ClientError",
    "NetworkError",
    "RateLimitError",
    "ServerError",
    "TimeoutError",
    "RetryConfig",
]

# Optional imports for styling (won't fail if dependencies are missing)
# We don't actually import the module here, just expose it conditionally
try:
    import openelectricity.styles  # noqa: F401

    __all__.append("styles")
except ImportError:
    pass  # Styling module requires matplotlib/seaborn which are optional
