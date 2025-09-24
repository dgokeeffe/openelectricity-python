"""
Enhanced error handling for OpenElectricity API client.

This module provides a comprehensive error hierarchy and retry logic
for handling various API failure scenarios gracefully.
"""

import time
from typing import Any, Callable, TypeVar
from urllib.parse import urlparse

import httpx
from tenacity import (
    RetryError,
    Retrying,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_fixed,
)

from openelectricity.logging import get_logger

logger = get_logger("exceptions")

T = TypeVar("T")


class OpenElectricityError(Exception):
    """Base exception for OpenElectricity API errors."""

    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.details = details or {}


class APIError(OpenElectricityError):
    """Base exception for API-related errors."""

    def __init__(
        self,
        message: str,
        status_code: int | None = None,
        response_data: dict[str, Any] | None = None,
        url: str | None = None,
    ):
        super().__init__(message, {"status_code": status_code, "response_data": response_data, "url": url})
        self.status_code = status_code
        self.response_data = response_data
        self.url = url


class AuthenticationError(APIError):
    """Exception raised for authentication failures."""

    def __init__(self, message: str = "Authentication failed", **kwargs):
        super().__init__(message, **kwargs)


class RateLimitError(APIError):
    """Exception raised when rate limit is exceeded."""

    def __init__(
        self,
        message: str = "Rate limit exceeded",
        retry_after: int | None = None,
        **kwargs,
    ):
        super().__init__(message, **kwargs)
        self.retry_after = retry_after


class ServerError(APIError):
    """Exception raised for server-side errors (5xx)."""

    def __init__(self, message: str = "Server error", **kwargs):
        super().__init__(message, **kwargs)


class ClientError(APIError):
    """Exception raised for client-side errors (4xx)."""

    def __init__(self, message: str = "Client error", **kwargs):
        super().__init__(message, **kwargs)


class NetworkError(OpenElectricityError):
    """Exception raised for network-related errors."""

    def __init__(self, message: str = "Network error", **kwargs):
        super().__init__(message, **kwargs)


class TimeoutError(OpenElectricityError):
    """Exception raised for timeout errors."""

    def __init__(self, message: str = "Request timeout", **kwargs):
        super().__init__(message, **kwargs)


class ValidationError(OpenElectricityError):
    """Exception raised for data validation errors."""

    def __init__(self, message: str = "Validation error", **kwargs):
        super().__init__(message, **kwargs)


class RetryableError(OpenElectricityError):
    """Exception that indicates a request can be retried."""

    def __init__(self, message: str = "Retryable error", **kwargs):
        super().__init__(message, **kwargs)


def classify_httpx_error(error: httpx.HTTPError) -> OpenElectricityError:
    """
    Classify httpx errors into appropriate OpenElectricity error types.
    
    Args:
        error: The httpx error to classify
        
    Returns:
        Appropriate OpenElectricity error instance
    """
    if isinstance(error, httpx.TimeoutException):
        return TimeoutError(f"Request timeout: {error}")
    elif isinstance(error, httpx.ConnectError):
        return NetworkError(f"Connection error: {error}")
    elif isinstance(error, httpx.RequestError):
        return NetworkError(f"Request error: {error}")
    else:
        return NetworkError(f"HTTP error: {error}")


def classify_response_error(response: httpx.Response) -> APIError:
    """
    Classify HTTP response errors into appropriate API error types.
    
    Args:
        response: The HTTP response to classify
        
    Returns:
        Appropriate API error instance
    """
    status_code = response.status_code
    url = str(response.url)
    
    try:
        response_data = response.json()
        detail = response_data.get("detail", response.reason_phrase)
    except Exception:
        response_data = None
        detail = response.reason_phrase
    
    # Extract retry-after header for rate limiting
    retry_after = None
    if hasattr(response, 'headers') and "retry-after" in response.headers:
        try:
            retry_after = int(response.headers["retry-after"])
        except ValueError:
            pass
    
    if status_code == 401:
        return AuthenticationError(f"Authentication failed: {detail}", status_code=status_code, response_data=response_data, url=url)
    elif status_code == 403:
        return AuthenticationError(f"Access forbidden: {detail}", status_code=status_code, response_data=response_data, url=url)
    elif status_code == 429:
        return RateLimitError(
            f"Rate limit exceeded: {detail}",
            status_code=status_code,
            response_data=response_data,
            url=url,
            retry_after=retry_after,
        )
    elif 400 <= status_code < 500:
        return ClientError(f"Client error {status_code}: {detail}", status_code=status_code, response_data=response_data, url=url)
    elif 500 <= status_code < 600:
        return ServerError(f"Server error {status_code}: {detail}", status_code=status_code, response_data=response_data, url=url)
    else:
        return APIError(f"API error {status_code}: {detail}", status_code=status_code, response_data=response_data, url=url)


def is_retryable_error(error: Exception) -> bool:
    """
    Determine if an error is retryable.
    
    Args:
        error: The error to check
        
    Returns:
        True if the error is retryable, False otherwise
    """
    # Network errors are generally retryable
    if isinstance(error, (NetworkError, TimeoutError)):
        return True
    
    # Server errors (5xx) are retryable
    if isinstance(error, ServerError):
        return True
    
    # Rate limit errors are retryable
    if isinstance(error, RateLimitError):
        return True
    
    # Specific httpx errors that are retryable
    if isinstance(error, (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError)):
        return True
    
    # HTTP status codes that are retryable
    if isinstance(error, APIError) and error.status_code:
        # 5xx server errors
        if 500 <= error.status_code < 600:
            return True
        # 429 rate limiting
        if error.status_code == 429:
            return True
        # 408 request timeout
        if error.status_code == 408:
            return True
    
    return False


def get_retry_delay(error: Exception, attempt: int) -> float:
    """
    Calculate retry delay based on error type and attempt number.
    
    Args:
        error: The error that occurred
        attempt: The current attempt number (1-based)
        
    Returns:
        Delay in seconds before next retry
    """
    base_delay = 1.0
    
    # Rate limit errors use the retry-after header if available
    if isinstance(error, RateLimitError) and error.retry_after:
        return float(error.retry_after)
    
    # Exponential backoff for most retryable errors
    if isinstance(error, (ServerError, NetworkError, TimeoutError)):
        return min(base_delay * (2 ** (attempt - 1)), 60.0)  # Cap at 60 seconds
    
    # Default exponential backoff
    return min(base_delay * (2 ** (attempt - 1)), 30.0)  # Cap at 30 seconds


class RetryConfig:
    """Configuration for retry behavior."""
    
    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
    ):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter


def retry_with_config(
    config: RetryConfig | None = None,
    retryable_exceptions: tuple[type[Exception], ...] | None = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator factory for retry logic with custom configuration.
    
    Args:
        config: Retry configuration
        retryable_exceptions: Tuple of exception types that should be retried
        
    Returns:
        Decorator function
    """
    if config is None:
        config = RetryConfig()
    
    if retryable_exceptions is None:
        retryable_exceptions = (RetryableError, ServerError, NetworkError, TimeoutError, RateLimitError)
    
    def retry_decorator(func: Callable[..., T]) -> Callable[..., T]:
        @retry(
            stop=stop_after_attempt(config.max_attempts),
            wait=wait_exponential(
                multiplier=config.base_delay,
                max=config.max_delay,
                exp_base=config.exponential_base,
            ),
            retry=retry_if_exception_type(retryable_exceptions),
            reraise=True,
        )
        def wrapper(*args, **kwargs) -> T:
            return func(*args, **kwargs)
        
        return wrapper
    
    return retry_decorator


class CircuitBreaker:
    """Simple circuit breaker implementation."""
    
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        expected_exception: type[Exception] = Exception,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        
        self.failure_count = 0
        self.last_failure_time: float | None = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
    
    def call(self, func: Callable[..., T], *args, **kwargs) -> T:
        """
        Execute function with circuit breaker protection.
        
        Args:
            func: Function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments
            
        Returns:
            Function result
            
        Raises:
            CircuitBreakerOpenError: If circuit breaker is open
            Exception: If function execution fails
        """
        if self.state == "OPEN":
            if self._should_attempt_reset():
                self.state = "HALF_OPEN"
            else:
                raise CircuitBreakerOpenError("Circuit breaker is OPEN")
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except self.expected_exception as e:
            self._on_failure()
            raise e
    
    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset."""
        if self.last_failure_time is None:
            return True
        return time.time() - self.last_failure_time >= self.recovery_timeout
    
    def _on_success(self):
        """Handle successful execution."""
        self.failure_count = 0
        self.state = "CLOSED"
    
    def _on_failure(self):
        """Handle failed execution."""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"


class CircuitBreakerOpenError(OpenElectricityError):
    """Exception raised when circuit breaker is open."""
    
    def __init__(self, message: str = "Circuit breaker is open"):
        super().__init__(message)
