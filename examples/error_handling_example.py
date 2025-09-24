"""
Example demonstrating enhanced error handling and retry logic.

This example shows how to use the improved error handling system
with retry logic, circuit breaker, and specific error types.
"""

import asyncio
from datetime import datetime, timedelta

from openelectricity import OEClient
from openelectricity.exceptions import (
    AuthenticationError,
    CircuitBreakerOpenError,
    RateLimitError,
    RetryConfig,
    ServerError,
)
from openelectricity.types import DataMetric, NetworkCode


def example_basic_error_handling():
    """Example of basic error handling with specific error types."""
    print("=== BASIC ERROR HANDLING EXAMPLE ===\n")
    
    client = OEClient(api_key="your_api_key_here")
    
    try:
        # This will raise specific error types based on HTTP status codes
        response = client.get_network_data(
            network_code=NetworkCode.NEM,
            metrics=[DataMetric.POWER],
            interval="1d",
        )
        print(f"Success: Got {len(response.data)} data series")
        
    except AuthenticationError as e:
        print(f"Authentication failed: {e}")
        print(f"Status code: {e.status_code}")
        print("Check your API key and permissions")
        
    except RateLimitError as e:
        print(f"Rate limit exceeded: {e}")
        print(f"Retry after: {e.retry_after} seconds")
        print("Consider implementing exponential backoff")
        
    except ServerError as e:
        print(f"Server error: {e}")
        print(f"Status code: {e.status_code}")
        print("This is a temporary server issue, retry later")
        
    except Exception as e:
        print(f"Unexpected error: {e}")


def example_custom_retry_configuration():
    """Example of custom retry configuration."""
    print("\n=== CUSTOM RETRY CONFIGURATION EXAMPLE ===\n")
    
    # Configure aggressive retry for critical operations
    retry_config = RetryConfig(
        max_attempts=5,        # Try up to 5 times
        base_delay=2.0,        # Start with 2 second delay
        max_delay=120.0,       # Cap delay at 2 minutes
        exponential_base=2.0,   # Double delay each time
        jitter=True,           # Add randomness to prevent thundering herd
    )
    
    client = OEClient(
        api_key="your_api_key_here",
        retry_config=retry_config,
    )
    
    print("Client configured with aggressive retry settings:")
    print(f"- Max attempts: {retry_config.max_attempts}")
    print(f"- Base delay: {retry_config.base_delay}s")
    print(f"- Max delay: {retry_config.max_delay}s")
    print(f"- Exponential base: {retry_config.exponential_base}")
    print(f"- Jitter: {retry_config.jitter}")
    
    try:
        response = client.get_network_data(
            network_code=NetworkCode.NEM,
            metrics=[DataMetric.POWER, DataMetric.ENERGY],
            interval="1h",
        )
        print(f"\nSuccess: Got {len(response.data)} data series")
        
    except Exception as e:
        print(f"\nFailed after all retries: {e}")


def example_circuit_breaker():
    """Example of circuit breaker pattern."""
    print("\n=== CIRCUIT BREAKER EXAMPLE ===\n")
    
    # Enable circuit breaker with custom settings
    client = OEClient(
        api_key="your_api_key_here",
        enable_circuit_breaker=True,
    )
    
    print("Circuit breaker enabled with default settings:")
    print(f"- Failure threshold: {client.circuit_breaker.failure_threshold}")
    print(f"- Recovery timeout: {client.circuit_breaker.recovery_timeout}s")
    print(f"- Current state: {client.circuit_breaker.state}")
    
    # Simulate multiple requests
    for i in range(7):
        try:
            response = client.get_network_data(
                network_code=NetworkCode.NEM,
                metrics=[DataMetric.POWER],
                interval="1d",
            )
            print(f"Request {i+1}: Success")
            
        except CircuitBreakerOpenError:
            print(f"Request {i+1}: Circuit breaker OPEN - request blocked")
            print("Waiting for circuit breaker to reset...")
            break
            
        except ServerError as e:
            print(f"Request {i+1}: Server error - {e.status_code}")
            print(f"Circuit breaker state: {client.circuit_breaker.state}")
            print(f"Failure count: {client.circuit_breaker.failure_count}")
            
        except Exception as e:
            print(f"Request {i+1}: Unexpected error - {e}")


def example_error_handling_without_circuit_breaker():
    """Example of error handling without circuit breaker."""
    print("\n=== NO CIRCUIT BREAKER EXAMPLE ===\n")
    
    # Disable circuit breaker for applications that handle failures differently
    client = OEClient(
        api_key="your_api_key_here",
        enable_circuit_breaker=False,
    )
    
    print("Circuit breaker disabled - all requests will be attempted")
    print(f"Circuit breaker: {client.circuit_breaker}")
    
    try:
        response = client.get_network_data(
            network_code=NetworkCode.NEM,
            metrics=[DataMetric.POWER],
            interval="1d",
        )
        print(f"Success: Got {len(response.data)} data series")
        
    except Exception as e:
        print(f"Error: {e}")


async def example_async_error_handling():
    """Example of async error handling."""
    print("\n=== ASYNC ERROR HANDLING EXAMPLE ===\n")
    
    client = OEClient(api_key="your_api_key_here")
    
    try:
        async with client:
            response = await client.get_network_data_async(
                network_code=NetworkCode.NEM,
                metrics=[DataMetric.POWER],
                interval="1d",
            )
            print(f"Async success: Got {len(response.data)} data series")
            
    except AuthenticationError as e:
        print(f"Async authentication failed: {e}")
        
    except RateLimitError as e:
        print(f"Async rate limit exceeded: {e}")
        print(f"Retry after: {e.retry_after} seconds")
        
    except ServerError as e:
        print(f"Async server error: {e}")
        
    except Exception as e:
        print(f"Async unexpected error: {e}")


def example_production_ready_configuration():
    """Example of production-ready error handling configuration."""
    print("\n=== PRODUCTION-READY CONFIGURATION EXAMPLE ===\n")
    
    # Production-ready retry configuration
    retry_config = RetryConfig(
        max_attempts=3,        # Conservative retry count
        base_delay=1.0,        # Start with 1 second delay
        max_delay=30.0,        # Cap delay at 30 seconds
        exponential_base=2.0,    # Standard exponential backoff
        jitter=True,           # Prevent thundering herd
    )
    
    client = OEClient(
        api_key="your_api_key_here",
        retry_config=retry_config,
        enable_circuit_breaker=True,  # Enable circuit breaker for resilience
    )
    
    print("Production-ready configuration:")
    print(f"- Retry attempts: {retry_config.max_attempts}")
    print(f"- Circuit breaker: {'Enabled' if client.circuit_breaker else 'Disabled'}")
    print(f"- Base delay: {retry_config.base_delay}s")
    print(f"- Max delay: {retry_config.max_delay}s")
    
    # Example of handling different error scenarios
    try:
        response = client.get_network_data(
            network_code=NetworkCode.NEM,
            metrics=[DataMetric.POWER, DataMetric.ENERGY],
            interval="1d",
            date_start=datetime.now() - timedelta(days=7),
            date_end=datetime.now(),
        )
        
        print(f"\n✅ Success: Retrieved {len(response.data)} data series")
        
        # Process the data
        for series in response.data:
            print(f"  - {series.metric}: {series.unit}")
            
    except AuthenticationError:
        print("\n❌ Authentication Error:")
        print("  - Check API key validity")
        print("  - Verify API key permissions")
        print("  - Contact support if issue persists")
        
    except RateLimitError as e:
        print(f"\n⚠️  Rate Limit Exceeded:")
        print(f"  - Retry after: {e.retry_after} seconds")
        print("  - Consider implementing request queuing")
        print("  - Review API usage patterns")
        
    except ServerError as e:
        print(f"\n🔧 Server Error ({e.status_code}):")
        print("  - Temporary server issue")
        print("  - Retry automatically handled")
        print("  - Monitor for patterns")
        
    except CircuitBreakerOpenError:
        print("\n🚫 Circuit Breaker Open:")
        print("  - Too many recent failures")
        print("  - Requests temporarily blocked")
        print("  - Will reset automatically")
        
    except Exception as e:
        print(f"\n💥 Unexpected Error: {e}")
        print("  - Log error details")
        print("  - Consider fallback strategies")
        print("  - Report to development team")


def main():
    """Run all examples."""
    print("OpenElectricity Enhanced Error Handling Examples")
    print("=" * 50)
    
    # Run synchronous examples
    example_basic_error_handling()
    example_custom_retry_configuration()
    example_circuit_breaker()
    example_error_handling_without_circuit_breaker()
    example_production_ready_configuration()
    
    # Run async example
    print("\n" + "=" * 50)
    asyncio.run(example_async_error_handling())
    
    print("\n" + "=" * 50)
    print("Examples completed!")


if __name__ == "__main__":
    main()
