"""
Tests for enhanced error handling and retry logic.

This module tests the comprehensive error handling system including:
- Error classification and hierarchy
- Retry logic with exponential backoff
- Circuit breaker pattern
- Client error handling integration
"""

import time
from unittest.mock import Mock, patch

import httpx
import pytest
from tenacity import RetryError

from openelectricity.client import OEClient
from openelectricity.exceptions import (
    APIError,
    AuthenticationError,
    CircuitBreaker,
    CircuitBreakerOpenError,
    ClientError,
    NetworkError,
    OpenElectricityError,
    RateLimitError,
    RetryConfig,
    ServerError,
    TimeoutError,
    classify_httpx_error,
    classify_response_error,
    is_retryable_error,
)


class TestErrorClassification:
    """Test error classification functionality."""

    def test_classify_httpx_timeout_error(self):
        """Test classification of httpx timeout errors."""
        error = httpx.TimeoutException("Request timeout")
        classified_error = classify_httpx_error(error)
        
        assert isinstance(classified_error, TimeoutError)
        assert "Request timeout" in str(classified_error)

    def test_classify_httpx_connect_error(self):
        """Test classification of httpx connection errors."""
        error = httpx.ConnectError("Connection failed")
        classified_error = classify_httpx_error(error)
        
        assert isinstance(classified_error, NetworkError)
        assert "Connection error" in str(classified_error)

    def test_classify_httpx_request_error(self):
        """Test classification of httpx request errors."""
        error = httpx.RequestError("Request failed")
        classified_error = classify_httpx_error(error)
        
        assert isinstance(classified_error, NetworkError)
        assert "Request error" in str(classified_error)

    def test_classify_response_authentication_error(self):
        """Test classification of authentication errors."""
        response = Mock()
        response.status_code = 401
        response.reason_phrase = "Unauthorized"
        response.url = "https://api.example.com/test"
        response.headers = {}
        response.json.return_value = {"detail": "Invalid API key"}
        
        classified_error = classify_response_error(response)
        
        assert isinstance(classified_error, AuthenticationError)
        assert classified_error.status_code == 401
        assert "Invalid API key" in str(classified_error)

    def test_classify_response_rate_limit_error(self):
        """Test classification of rate limit errors."""
        response = Mock()
        response.status_code = 429
        response.reason_phrase = "Too Many Requests"
        response.url = "https://api.example.com/test"
        response.headers = {"retry-after": "60"}
        response.json.return_value = {"detail": "Rate limit exceeded"}
        
        classified_error = classify_response_error(response)
        
        assert isinstance(classified_error, RateLimitError)
        assert classified_error.status_code == 429
        assert classified_error.retry_after == 60
        assert "Rate limit exceeded" in str(classified_error)

    def test_classify_response_server_error(self):
        """Test classification of server errors."""
        response = Mock()
        response.status_code = 500
        response.reason_phrase = "Internal Server Error"
        response.url = "https://api.example.com/test"
        response.headers = {}
        response.json.return_value = {"detail": "Internal server error"}
        
        classified_error = classify_response_error(response)
        
        assert isinstance(classified_error, ServerError)
        assert classified_error.status_code == 500
        assert "Internal server error" in str(classified_error)

    def test_classify_response_client_error(self):
        """Test classification of client errors."""
        response = Mock()
        response.status_code = 400
        response.reason_phrase = "Bad Request"
        response.url = "https://api.example.com/test"
        response.headers = {}
        response.json.return_value = {"detail": "Invalid parameters"}
        
        classified_error = classify_response_error(response)
        
        assert isinstance(classified_error, ClientError)
        assert classified_error.status_code == 400
        assert "Invalid parameters" in str(classified_error)


class TestRetryableErrorDetection:
    """Test retryable error detection."""

    def test_network_errors_are_retryable(self):
        """Test that network errors are considered retryable."""
        error = NetworkError("Connection failed")
        assert is_retryable_error(error)

    def test_timeout_errors_are_retryable(self):
        """Test that timeout errors are considered retryable."""
        error = TimeoutError("Request timeout")
        assert is_retryable_error(error)

    def test_server_errors_are_retryable(self):
        """Test that server errors are considered retryable."""
        error = ServerError("Internal server error", status_code=500)
        assert is_retryable_error(error)

    def test_rate_limit_errors_are_retryable(self):
        """Test that rate limit errors are considered retryable."""
        error = RateLimitError("Rate limit exceeded", status_code=429)
        assert is_retryable_error(error)

    def test_authentication_errors_are_not_retryable(self):
        """Test that authentication errors are not retryable."""
        error = AuthenticationError("Invalid API key", status_code=401)
        assert not is_retryable_error(error)

    def test_client_errors_are_not_retryable(self):
        """Test that client errors are not retryable."""
        error = ClientError("Bad request", status_code=400)
        assert not is_retryable_error(error)

    def test_httpx_timeout_exception_is_retryable(self):
        """Test that httpx timeout exceptions are retryable."""
        error = httpx.TimeoutException("Request timeout")
        assert is_retryable_error(error)

    def test_httpx_connect_error_is_retryable(self):
        """Test that httpx connection errors are retryable."""
        error = httpx.ConnectError("Connection failed")
        assert is_retryable_error(error)


class TestCircuitBreaker:
    """Test circuit breaker functionality."""

    def test_circuit_breaker_closed_state(self):
        """Test circuit breaker in closed state."""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=1.0)
        
        # Should work normally
        result = cb.call(lambda: "success")
        assert result == "success"
        assert cb.state == "CLOSED"

    def test_circuit_breaker_opens_after_threshold(self):
        """Test circuit breaker opens after failure threshold."""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=1.0)
        
        # First failure
        with pytest.raises(ValueError):
            cb.call(lambda: (_ for _ in ()).throw(ValueError("test error")))
        assert cb.state == "CLOSED"
        
        # Second failure - should open circuit
        with pytest.raises(ValueError):
            cb.call(lambda: (_ for _ in ()).throw(ValueError("test error")))
        assert cb.state == "OPEN"

    def test_circuit_breaker_blocks_requests_when_open(self):
        """Test circuit breaker blocks requests when open."""
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=1.0)
        
        # Cause failure to open circuit
        with pytest.raises(ValueError):
            cb.call(lambda: (_ for _ in ()).throw(ValueError("test error")))
        
        # Should block subsequent requests
        with pytest.raises(CircuitBreakerOpenError):
            cb.call(lambda: "should not execute")

    def test_circuit_breaker_resets_after_timeout(self):
        """Test circuit breaker resets after recovery timeout."""
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.1)
        
        # Cause failure to open circuit
        with pytest.raises(ValueError):
            cb.call(lambda: (_ for _ in ()).throw(ValueError("test error")))
        
        # Wait for recovery timeout
        time.sleep(0.2)
        
        # Should allow requests again (half-open state)
        result = cb.call(lambda: "success")
        assert result == "success"
        assert cb.state == "CLOSED"

    def test_circuit_breaker_success_resets_failure_count(self):
        """Test successful request resets failure count."""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=1.0)
        
        # Cause one failure
        with pytest.raises(ValueError):
            cb.call(lambda: (_ for _ in ()).throw(ValueError("test error")))
        
        # Success should reset failure count
        result = cb.call(lambda: "success")
        assert result == "success"
        assert cb.failure_count == 0
        assert cb.state == "CLOSED"


class TestClientErrorHandling:
    """Test client error handling integration."""

    @pytest.fixture
    def client(self):
        """Create client with test configuration."""
        return OEClient(
            api_key="test-key",
            base_url="https://test.api.example.com",
            retry_config=RetryConfig(max_attempts=2, base_delay=0.1),
            enable_circuit_breaker=True,
        )

    def test_client_handles_authentication_error(self, client):
        """Test client handles authentication errors properly."""
        with patch.object(client, '_make_request_with_retry') as mock_request:
            # Mock authentication error response
            response = Mock()
            response.status_code = 401
            response.reason_phrase = "Unauthorized"
            response.url = "https://test.api.example.com/v4/facilities/"
            response.headers = {}
            response.json.return_value = {"detail": "Invalid API key"}
            response.is_success = False
            
            mock_request.return_value = response
            
            with pytest.raises(AuthenticationError) as exc_info:
                client.get_facilities()
            
            assert exc_info.value.status_code == 401
            assert "Invalid API key" in str(exc_info.value)

    def test_client_handles_rate_limit_error(self, client):
        """Test client handles rate limit errors properly."""
        with patch.object(client, '_make_request_with_retry') as mock_request:
            # Mock rate limit error response
            response = Mock()
            response.status_code = 429
            response.reason_phrase = "Too Many Requests"
            response.url = "https://test.api.example.com/v4/facilities/"
            response.headers = {"retry-after": "60"}
            response.json.return_value = {"detail": "Rate limit exceeded"}
            response.is_success = False
            
            mock_request.return_value = response
            
            with pytest.raises(RateLimitError) as exc_info:
                client.get_facilities()
            
            assert exc_info.value.status_code == 429
            assert exc_info.value.retry_after == 60

    def test_client_handles_server_error(self, client):
        """Test client handles server errors properly."""
        with patch.object(client, '_make_request_with_retry') as mock_request:
            # Mock server error response
            response = Mock()
            response.status_code = 500
            response.reason_phrase = "Internal Server Error"
            response.url = "https://test.api.example.com/v4/facilities/"
            response.headers = {}
            response.json.return_value = {"detail": "Internal server error"}
            response.is_success = False
            
            mock_request.return_value = response
            
            with pytest.raises(ServerError) as exc_info:
                client.get_facilities()
            
            assert exc_info.value.status_code == 500

    def test_client_retries_on_retryable_errors(self, client):
        """Test client retries on retryable errors."""
        call_count = 0
        
        def mock_request_with_retry(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            
            if call_count < 2:  # Fail first time, succeed second time
                response = Mock()
                response.status_code = 500
                response.reason_phrase = "Internal Server Error"
                response.url = "https://test.api.example.com/v4/facilities/"
                response.headers = {}
                response.json.return_value = {"detail": "Internal server error"}
                response.is_success = False
                return response
            else:
                # Success response
                response = Mock()
                response.status_code = 200
                response.headers = {}
                response.is_success = True
                response.json.return_value = {
                    "success": True,
                    "version": "v4.0",
                    "created_at": "2024-01-01T00:00:00Z",
                    "data": []
                }
                return response
        
        with patch.object(client, '_make_request_with_retry', side_effect=mock_request_with_retry):
            # Should succeed after retry
            result = client.get_facilities()
            assert result.success is True
            assert call_count == 2

    def test_client_circuit_breaker_blocks_requests(self, client):
        """Test client circuit breaker blocks requests when open."""
        # Test circuit breaker directly
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=1.0)
        
        # Mock a function that always fails
        def failing_function():
            raise ServerError("Test server error", status_code=500)
        
        # Cause failures to open circuit breaker
        for _ in range(3):  # Exactly the failure threshold
            with pytest.raises(ServerError):
                cb.call(failing_function)
        
        # Circuit breaker should now be open
        assert cb.state == "OPEN"
        
        # Next request should be blocked by circuit breaker
        with pytest.raises(CircuitBreakerOpenError):
            cb.call(failing_function)

    def test_client_without_circuit_breaker(self):
        """Test client without circuit breaker."""
        client = OEClient(
            api_key="test-key",
            base_url="https://test.api.example.com",
            enable_circuit_breaker=False,
        )
        
        assert client.circuit_breaker is None
        
        # Should work normally without circuit breaker
        with patch.object(client, '_make_request_with_retry') as mock_request:
            response = Mock()
            response.status_code = 200
            response.headers = {}
            response.is_success = True
            response.json.return_value = {
                "success": True,
                "version": "v4.0",
                "created_at": "2024-01-01T00:00:00Z",
                "data": []
            }
            mock_request.return_value = response
            
            result = client.get_facilities()
            assert result.success is True


class TestRetryConfiguration:
    """Test retry configuration options."""

    def test_custom_retry_config(self):
        """Test custom retry configuration."""
        config = RetryConfig(
            max_attempts=5,
            base_delay=2.0,
            max_delay=120.0,
            exponential_base=3.0,
            jitter=True,
        )
        
        assert config.max_attempts == 5
        assert config.base_delay == 2.0
        assert config.max_delay == 120.0
        assert config.exponential_base == 3.0
        assert config.jitter is True

    def test_default_retry_config(self):
        """Test default retry configuration."""
        config = RetryConfig()
        
        assert config.max_attempts == 3
        assert config.base_delay == 1.0
        assert config.max_delay == 60.0
        assert config.exponential_base == 2.0
        assert config.jitter is True


class TestErrorHierarchy:
    """Test error hierarchy and inheritance."""

    def test_error_inheritance(self):
        """Test that error classes inherit properly."""
        # Test base error
        base_error = OpenElectricityError("Base error")
        assert isinstance(base_error, Exception)
        
        # Test API error inheritance
        api_error = APIError("API error", status_code=400)
        assert isinstance(api_error, OpenElectricityError)
        assert isinstance(api_error, Exception)
        
        # Test specific error inheritance
        auth_error = AuthenticationError("Auth error", status_code=401)
        assert isinstance(auth_error, APIError)
        assert isinstance(auth_error, OpenElectricityError)
        assert isinstance(auth_error, Exception)

    def test_error_details_preservation(self):
        """Test that error details are preserved."""
        error = APIError(
            "Test error",
            status_code=500,
            response_data={"detail": "Server error"},
            url="https://api.example.com/test",
        )
        
        assert error.status_code == 500
        assert error.response_data == {"detail": "Server error"}
        assert error.url == "https://api.example.com/test"
        assert error.details["status_code"] == 500
