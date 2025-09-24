# Enhanced Error Handling and Retry Logic

The OpenElectricity Python client now includes comprehensive error handling and retry logic to make your applications more resilient and reliable.

## Error Hierarchy

The client provides a structured error hierarchy for better error handling:

```python
OpenElectricityError (base)
├── APIError (HTTP response errors)
│   ├── AuthenticationError (401, 403)
│   ├── RateLimitError (429)
│   ├── ClientError (4xx)
│   └── ServerError (5xx)
├── NetworkError (connection issues)
└── TimeoutError (request timeouts)
```

## Basic Error Handling

```python
from openelectricity import OEClient, AuthenticationError, RateLimitError, ServerError

client = OEClient(api_key="your_api_key")

try:
    response = client.get_network_data(
        network_code="NEM",
        metrics=["power"],
        interval="1d"
    )
    print(f"Success: {len(response.data)} series")
    
except AuthenticationError as e:
    print(f"Auth failed: {e}")
    print(f"Status: {e.status_code}")
    
except RateLimitError as e:
    print(f"Rate limited: {e}")
    print(f"Retry after: {e.retry_after} seconds")
    
except ServerError as e:
    print(f"Server error: {e}")
    print(f"Status: {e.status_code}")
```

## Retry Configuration

Configure retry behavior for different scenarios:

```python
from openelectricity import OEClient, RetryConfig

# Conservative retry for production
retry_config = RetryConfig(
    max_attempts=3,        # Try up to 3 times
    base_delay=1.0,        # Start with 1 second delay
    max_delay=30.0,        # Cap delay at 30 seconds
    exponential_base=2.0,   # Double delay each time
    jitter=True,           # Add randomness
)

client = OEClient(
    api_key="your_api_key",
    retry_config=retry_config
)
```

## Circuit Breaker Pattern

The circuit breaker prevents cascading failures by temporarily blocking requests when the service is down:

```python
client = OEClient(
    api_key="your_api_key",
    enable_circuit_breaker=True  # Enabled by default
)

# Circuit breaker automatically opens after repeated failures
# and blocks requests until the service recovers
```

## Error Types and Retry Behavior

| Error Type | Retryable | Description |
|------------|-----------|-------------|
| `NetworkError` | ✅ Yes | Connection issues, DNS failures |
| `TimeoutError` | ✅ Yes | Request timeouts |
| `ServerError` | ✅ Yes | 5xx HTTP status codes |
| `RateLimitError` | ✅ Yes | 429 status with retry-after |
| `AuthenticationError` | ❌ No | 401/403 status codes |
| `ClientError` | ❌ No | 4xx status codes (except 429) |

## Production Best Practices

### 1. Use Specific Error Handling

```python
try:
    data = client.get_network_data(...)
except AuthenticationError:
    # Handle auth issues - check API key
    pass
except RateLimitError as e:
    # Handle rate limiting - implement backoff
    time.sleep(e.retry_after)
except ServerError:
    # Handle server issues - retry automatically handled
    pass
```

### 2. Configure Appropriate Retry Settings

```python
# For critical operations
retry_config = RetryConfig(
    max_attempts=5,
    base_delay=2.0,
    max_delay=120.0
)

# For non-critical operations
retry_config = RetryConfig(
    max_attempts=2,
    base_delay=1.0,
    max_delay=10.0
)
```

### 3. Monitor Circuit Breaker State

```python
if client.circuit_breaker.state == "OPEN":
    print("Service is down, requests blocked")
    # Implement fallback strategy
```

### 4. Handle Rate Limiting Gracefully

```python
try:
    data = client.get_network_data(...)
except RateLimitError as e:
    # Wait for the specified time
    await asyncio.sleep(e.retry_after)
    # Retry the request
    data = client.get_network_data(...)
```

## Async Error Handling

The async client provides the same error handling capabilities:

```python
async with OEClient(api_key="your_api_key") as client:
    try:
        response = await client.get_network_data_async(...)
    except AuthenticationError as e:
        print(f"Async auth error: {e}")
    except RateLimitError as e:
        print(f"Async rate limit: {e}")
```

## Migration Guide

### Before (Old Error Handling)

```python
try:
    response = client.get_network_data(...)
except APIError as e:
    if e.status_code == 401:
        print("Auth failed")
    elif e.status_code == 429:
        print("Rate limited")
    elif e.status_code >= 500:
        print("Server error")
```

### After (New Error Handling)

```python
try:
    response = client.get_network_data(...)
except AuthenticationError:
    print("Auth failed")
except RateLimitError as e:
    print(f"Rate limited, retry after {e.retry_after}s")
except ServerError:
    print("Server error")
```

## Benefits

- **Better Error Classification**: Specific error types for different failure scenarios
- **Automatic Retries**: Built-in retry logic with exponential backoff
- **Circuit Breaker**: Prevents cascading failures during outages
- **Rate Limit Handling**: Automatic handling of rate limit responses
- **Production Ready**: Configurable retry behavior for different environments
- **Async Support**: Same error handling capabilities for async operations

## Examples

See `examples/error_handling_example.py` for comprehensive examples of all error handling features.
