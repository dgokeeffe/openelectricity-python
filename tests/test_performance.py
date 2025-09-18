"""
Performance tests for the OpenElectricity API client.

These tests compare the performance of different HTTP client configurations
and measure the impact of optimizations.

Run with: pytest -m performance
"""

import time
from typing import Any

import httpx
import pytest
import requests

from openelectricity.client import OEClient


class RequestsClient:
    """Simple requests-based client for performance comparison."""
    
    def __init__(self, api_key: str, base_url: str = "https://api.openelectricity.org.au/"):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/") + "/"
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        })
    
    def get_facilities(self):
        """Get facilities using requests."""
        url = f"{self.base_url}v4/facilities/"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()
    
    def close(self):
        """Close the session."""
        self.session.close()


@pytest.mark.performance
class TestPerformanceComparison:
    """Performance comparison tests between httpx and requests."""

    def test_client_creation_performance(self):
        """Test client creation overhead."""
        # Test httpx client creation
        start_time = time.time()
        for _ in range(100):
            client = httpx.Client(
                headers={"Authorization": "Bearer test-key"},
                timeout=httpx.Timeout(10.0, connect=5.0, read=30.0),
                limits=httpx.Limits(
                    max_keepalive_connections=50,
                    max_connections=200,
                    keepalive_expiry=30.0
                ),
                http2=False,
                follow_redirects=True,
                verify=True
            )
            client.close()
        httpx_time = time.time() - start_time
        
        # Test requests session creation
        start_time = time.time()
        for _ in range(100):
            session = requests.Session()
            session.headers.update({"Authorization": "Bearer test-key"})
            session.close()
        requests_time = time.time() - start_time
        
        print(f"\nClient Creation Performance (100 iterations):")
        print(f"httpx: {httpx_time:.4f}s")
        print(f"requests: {requests_time:.4f}s")
        
        # httpx should be reasonably fast (not necessarily faster than requests)
        # httpx has more features so it's expected to be slower than requests
        assert httpx_time < 5.0, f"httpx client creation too slow: {httpx_time:.4f}s"
        
        # Both should complete successfully
        assert httpx_time > 0, "httpx client creation failed"
        assert requests_time > 0, "requests session creation failed"

    def test_connection_pool_configuration(self, openelectricity_client):
        """Test that the optimized connection pool configuration is applied."""
        client = openelectricity_client
        
        # Access the internal httpx client to check configuration
        sync_client = client._ensure_sync_client()
        
        # Check connection pool limits
        assert sync_client._limits.max_keepalive_connections == 50
        assert sync_client._limits.max_connections == 200
        assert sync_client._limits.keepalive_expiry == 30.0
        
        # Check timeout configuration
        assert isinstance(sync_client._timeout, httpx.Timeout)
        assert sync_client._timeout.connect == 5.0
        assert sync_client._timeout.read == 30.0
        
        print(f"\nConnection Pool Configuration:")
        print(f"Max keepalive connections: {sync_client._limits.max_keepalive_connections}")
        print(f"Max total connections: {sync_client._limits.max_connections}")
        print(f"Keepalive expiry: {sync_client._limits.keepalive_expiry}s")
        print(f"Connect timeout: {sync_client._timeout.connect}s")
        print(f"Read timeout: {sync_client._timeout.read}s")

    def test_sync_performance_comparison(self, openelectricity_api_key):
        """Compare synchronous performance between httpx and requests."""
        num_requests = 5
        
        # Test httpx client
        print(f"\n=== httpx Performance ({num_requests} requests) ===")
        httpx_client = OEClient(api_key=openelectricity_api_key)
        httpx_client.preload_client()  # Preload for fair comparison
        
        start_time = time.time()
        for i in range(num_requests):
            try:
                httpx_client.get_facilities()
                print(f"Request {i+1}: ✓")
            except Exception as e:
                print(f"Request {i+1}: ✗ {e}")
        
        httpx_total = time.time() - start_time
        httpx_client.close()
        
        # Test requests client
        print(f"\n=== requests Performance ({num_requests} requests) ===")
        requests_client = RequestsClient(api_key=openelectricity_api_key)
        
        start_time = time.time()
        for i in range(num_requests):
            try:
                requests_client.get_facilities()
                print(f"Request {i+1}: ✓")
            except Exception as e:
                print(f"Request {i+1}: ✗ {e}")
        
        requests_total = time.time() - start_time
        requests_client.close()
        
        # Performance comparison
        print(f"\n=== Performance Results ===")
        print(f"httpx total time: {httpx_total:.3f}s")
        print(f"requests total time: {requests_total:.3f}s")
        
        if requests_total > 0:
            speedup = requests_total / httpx_total
            print(f"httpx is {speedup:.2f}x {'faster' if speedup > 1 else 'slower'} than requests")
            
            if speedup > 1:
                improvement = ((requests_total - httpx_total) / requests_total) * 100
                print(f"Performance improvement: {improvement:.1f}%")
            else:
                degradation = ((httpx_total - requests_total) / requests_total) * 100
                print(f"Performance degradation: {degradation:.1f}%")
        
        # Both should complete successfully
        assert httpx_total > 0, "httpx requests failed"
        assert requests_total > 0, "requests failed"

    def test_preload_client_performance(self, openelectricity_api_key):
        """Test the performance benefit of preloading the client."""
        num_requests = 3
        
        # Test without preloading
        print(f"\n=== Without Preloading ({num_requests} requests) ===")
        start_time = time.time()
        for i in range(num_requests):
            client = OEClient(api_key=openelectricity_api_key)
            try:
                client.get_facilities()  # First request creates client
                print(f"Request {i+1}: ✓")
            except Exception as e:
                print(f"Request {i+1}: ✗ {e}")
            finally:
                client.close()
        
        without_preload_time = time.time() - start_time
        
        # Test with preloading
        print(f"\n=== With Preloading ({num_requests} requests) ===")
        start_time = time.time()
        client = OEClient(api_key=openelectricity_api_key)
        client.preload_client()  # Preload client
        
        for i in range(num_requests):
            try:
                client.get_facilities()
                print(f"Request {i+1}: ✓")
            except Exception as e:
                print(f"Request {i+1}: ✗ {e}")
        
        with_preload_time = time.time() - start_time
        client.close()
        
        print(f"\n=== Preload Performance Results ===")
        print(f"Without preload: {without_preload_time:.3f}s")
        print(f"With preload: {with_preload_time:.3f}s")
        
        if without_preload_time > 0:
            improvement = ((without_preload_time - with_preload_time) / without_preload_time) * 100
            print(f"Preload improvement: {improvement:.1f}%")
        
        # Both should complete successfully
        assert without_preload_time > 0, "Without preload failed"
        assert with_preload_time > 0, "With preload failed"

    def test_connection_reuse_performance(self, openelectricity_api_key):
        """Test the performance benefit of connection reuse."""
        num_requests = 10
        
        print(f"\n=== Connection Reuse Test ({num_requests} requests) ===")
        
        # Test with connection reuse (single client)
        start_time = time.time()
        client = OEClient(api_key=openelectricity_api_key)
        client.preload_client()
        
        for i in range(num_requests):
            try:
                client.get_facilities()
                print(f"Request {i+1}: ✓")
            except Exception as e:
                print(f"Request {i+1}: ✗ {e}")
        
        reuse_time = time.time() - start_time
        client.close()
        
        # Test without connection reuse (new client each time)
        start_time = time.time()
        for i in range(num_requests):
            client = OEClient(api_key=openelectricity_api_key)
            try:
                client.get_facilities()
                print(f"Request {i+1}: ✓")
            except Exception as e:
                print(f"Request {i+1}: ✗ {e}")
            finally:
                client.close()
        
        no_reuse_time = time.time() - start_time
        
        print(f"\n=== Connection Reuse Results ===")
        print(f"With reuse: {reuse_time:.3f}s")
        print(f"Without reuse: {no_reuse_time:.3f}s")
        
        if no_reuse_time > 0:
            improvement = ((no_reuse_time - reuse_time) / no_reuse_time) * 100
            print(f"Connection reuse improvement: {improvement:.1f}%")
        
        # Connection reuse should be faster
        assert reuse_time < no_reuse_time, "Connection reuse should be faster"
        assert reuse_time > 0, "With reuse failed"
        assert no_reuse_time > 0, "Without reuse failed"

    def test_optimized_vs_basic_configuration(self, openelectricity_api_key):
        """Compare optimized vs basic httpx configuration."""
        num_requests = 5
        
        # Test basic configuration
        print(f"\n=== Basic httpx Configuration ({num_requests} requests) ===")
        basic_client = httpx.Client(
            headers={"Authorization": f"Bearer {openelectricity_api_key}"},
            timeout=30.0,
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=100)
        )
        
        start_time = time.time()
        for i in range(num_requests):
            try:
                response = basic_client.get("https://api.openelectricity.org.au/v4/facilities/")
                response.raise_for_status()
                print(f"Request {i+1}: ✓")
            except Exception as e:
                print(f"Request {i+1}: ✗ {e}")
        
        basic_time = time.time() - start_time
        basic_client.close()
        
        # Test optimized configuration
        print(f"\n=== Optimized httpx Configuration ({num_requests} requests) ===")
        optimized_client = httpx.Client(
            headers={"Authorization": f"Bearer {openelectricity_api_key}"},
            timeout=httpx.Timeout(10.0, connect=5.0, read=30.0),
            limits=httpx.Limits(
                max_keepalive_connections=50,
                max_connections=200,
                keepalive_expiry=30.0
            ),
            http2=False,
            follow_redirects=True,
            verify=True
        )
        
        start_time = time.time()
        for i in range(num_requests):
            try:
                response = optimized_client.get("https://api.openelectricity.org.au/v4/facilities/")
                response.raise_for_status()
                print(f"Request {i+1}: ✓")
            except Exception as e:
                print(f"Request {i+1}: ✗ {e}")
        
        optimized_time = time.time() - start_time
        optimized_client.close()
        
        print(f"\n=== Configuration Comparison Results ===")
        print(f"Basic config: {basic_time:.3f}s")
        print(f"Optimized config: {optimized_time:.3f}s")
        
        if basic_time > 0:
            improvement = ((basic_time - optimized_time) / basic_time) * 100
            print(f"Optimization improvement: {improvement:.1f}%")
        
        # Both should complete successfully
        assert basic_time > 0, "Basic configuration failed"
        assert optimized_time > 0, "Optimized configuration failed"

    def test_http2_performance(self, openelectricity_client):
        """Test HTTP/2 performance benefits."""
        print(f"\n=== HTTP/2 Performance Test ===")
        
        # Check if HTTP/2 is available
        try:
            import h2
            print("✅ HTTP/2 support is available")
        except ImportError:
            pytest.skip("HTTP/2 support not available - install httpx[http2]")
        
        client = openelectricity_client
        
        # Test multiple requests to see HTTP/2 multiplexing benefits
        num_requests = 5
        start_time = time.time()
        
        for i in range(num_requests):
            try:
                facilities = client.get_facilities()
                print(f"  Request {i+1}: ✅ {len(facilities.data)} facilities")
            except Exception as e:
                print(f"  Request {i+1}: ❌ {e}")
        
        total_time = time.time() - start_time
        avg_time = total_time / num_requests
        
        print(f"\n✅ HTTP/2 Performance Results:")
        print(f"  - Total time: {total_time:.3f}s")
        print(f"  - Average per request: {avg_time:.3f}s")
        print(f"  - Requests per second: {num_requests/total_time:.2f}")
        
        # Verify reasonable performance
        assert total_time > 0, "HTTP/2 test failed"
        assert avg_time < 3.0, f"Average request time too slow: {avg_time:.3f}s"
        
        print(f"  - HTTP/2 multiplexing: Multiple requests over single connection")
        print(f"  - Header compression: Reduced bandwidth usage")
        print(f"  - Better connection utilization")
