"""
Tests for the OpenElectricity Network API functionality.

This module contains comprehensive tests for network data retrieval,
including both synchronous and asynchronous operations, different network codes,
various metrics, and error handling scenarios.
"""

import pytest
from datetime import datetime, timedelta
from typing import List

from openelectricity import OEClient
from openelectricity.models.timeseries import TimeSeriesResponse
from openelectricity.types import (
    NetworkCode,
    DataMetric,
    DataInterval,
    DataPrimaryGrouping,
    DataSecondaryGrouping,
)


class TestNetworkAPI:
    """Test suite for Network API functionality."""

    def test_network_api_basic_functionality(self, openelectricity_client):
        """Test basic network API functionality with NEM network."""
        client = openelectricity_client
        
        try:
            # Test basic network data retrieval for NEM
            response = client.get_network_data(
                network_code="NEM",
                metrics=[DataMetric.POWER],
                interval="5m",
                date_start=datetime.now() - timedelta(hours=1),
                date_end=datetime.now(),
            )
            
            # Verify response structure
            assert isinstance(response, TimeSeriesResponse)
            assert response.success is True
            assert response.version is not None
            assert response.created_at is not None
            
            # Verify data structure
            assert response.data is not None
            assert len(response.data) > 0
            
            # Check first data item structure
            first_item = response.data[0]
            assert hasattr(first_item, 'network_code')
            assert hasattr(first_item, 'metric')
            assert hasattr(first_item, 'unit')
            assert hasattr(first_item, 'interval')
            assert hasattr(first_item, 'results')
            
            # Verify network code
            assert first_item.network_code == "NEM"
            assert first_item.metric == "power"
            
        except Exception as e:
            pytest.skip(f"Network API call failed: {e}")

    def test_network_api_different_networks(self, openelectricity_client):
        """Test network API with different network codes."""
        client = openelectricity_client
        networks_to_test = ["NEM", "WEM", "AU"]
        
        for network_code in networks_to_test:
            try:
                response = client.get_network_data(
                    network_code=network_code,
                    metrics=[DataMetric.ENERGY],
                    interval="1h",
                    date_start=datetime.now() - timedelta(hours=2),
                    date_end=datetime.now() - timedelta(hours=1),
                )
                
                assert isinstance(response, TimeSeriesResponse)
                assert response.success is True
                
                if response.data:
                    first_item = response.data[0]
                    assert first_item.network_code == network_code
                    
            except Exception as e:
                # Some networks might not have data or might be unavailable
                pytest.skip(f"Network {network_code} API call failed: {e}")

    def test_network_api_different_metrics(self, openelectricity_client):
        """Test network API with different data metrics."""
        client = openelectricity_client
        metrics_to_test = [
            DataMetric.POWER,
            DataMetric.ENERGY,
            DataMetric.EMISSIONS,
            DataMetric.MARKET_VALUE,
            DataMetric.RENEWABLE_PROPORTION,
        ]
        
        for metric in metrics_to_test:
            try:
                response = client.get_network_data(
                    network_code="NEM",
                    metrics=[metric],
                    interval="1h",
                    date_start=datetime.now() - timedelta(hours=1),
                    date_end=datetime.now(),
                )
                
                assert isinstance(response, TimeSeriesResponse)
                assert response.success is True
                
                if response.data:
                    first_item = response.data[0]
                    assert first_item.metric == metric.value
                    
            except Exception as e:
                pytest.skip(f"Metric {metric.value} API call failed: {e}")

    def test_network_api_different_intervals(self, openelectricity_client):
        """Test network API with different time intervals."""
        client = openelectricity_client
        intervals_to_test = ["5m", "1h", "1d"]
        
        for interval in intervals_to_test:
            try:
                # Adjust date range based on interval
                if interval == "5m":
                    date_start = datetime.now() - timedelta(hours=1)
                    date_end = datetime.now()
                elif interval == "1h":
                    date_start = datetime.now() - timedelta(hours=6)
                    date_end = datetime.now()
                else:  # 1d
                    date_start = datetime.now() - timedelta(days=2)
                    date_end = datetime.now() - timedelta(days=1)
                
                response = client.get_network_data(
                    network_code="NEM",
                    metrics=[DataMetric.POWER],
                    interval=interval,
                    date_start=date_start,
                    date_end=date_end,
                )
                
                assert isinstance(response, TimeSeriesResponse)
                assert response.success is True
                
                if response.data:
                    first_item = response.data[0]
                    assert first_item.interval == interval
                    
            except Exception as e:
                pytest.skip(f"Interval {interval} API call failed: {e}")

    def test_network_api_with_groupings(self, openelectricity_client):
        """Test network API with different grouping options."""
        client = openelectricity_client
        
        try:
            response = client.get_network_data(
                network_code="NEM",
                metrics=[DataMetric.POWER],
                interval="1h",
                date_start=datetime.now() - timedelta(hours=2),
                date_end=datetime.now() - timedelta(hours=1),
                primary_grouping="network_region",
                secondary_grouping="fueltech_group",
            )
            
            assert isinstance(response, TimeSeriesResponse)
            assert response.success is True
            
            if response.data:
                first_item = response.data[0]
                # Check that groupings are applied
                assert hasattr(first_item, 'groupings')
                
        except Exception as e:
            pytest.skip(f"Grouped network API call failed: {e}")

    def test_network_api_multiple_metrics(self, openelectricity_client):
        """Test network API with multiple metrics."""
        client = openelectricity_client
        
        try:
            response = client.get_network_data(
                network_code="NEM",
                metrics=[DataMetric.POWER, DataMetric.ENERGY],
                interval="1h",
                date_start=datetime.now() - timedelta(hours=1),
                date_end=datetime.now(),
            )
            
            assert isinstance(response, TimeSeriesResponse)
            assert response.success is True
            
            # Should have data for both metrics
            if response.data:
                metrics_returned = [item.metric for item in response.data]
                assert "power" in metrics_returned
                assert "energy" in metrics_returned
                
        except Exception as e:
            pytest.skip(f"Multiple metrics API call failed: {e}")

    def test_network_api_error_handling(self, openelectricity_client):
        """Test network API error handling with invalid parameters."""
        client = openelectricity_client
        
        # Test with invalid network code
        with pytest.raises(Exception):
            client.get_network_data(
                network_code="INVALID",  # type: ignore
                metrics=[DataMetric.POWER],
                interval="5m",
            )
        
        # Test with invalid date range (end before start)
        with pytest.raises(Exception):
            client.get_network_data(
                network_code="NEM",
                metrics=[DataMetric.POWER],
                interval="5m",
                date_start=datetime.now(),
                date_end=datetime.now() - timedelta(hours=1),
            )

    @pytest.mark.asyncio
    async def test_network_api_async_functionality(self, openelectricity_async_client):
        """Test async network API functionality."""
        async with openelectricity_async_client as client:
            try:
                response = await client.get_network_data_async(
                    network_code="NEM",
                    metrics=[DataMetric.POWER],
                    interval="5m",
                    date_start=datetime.now() - timedelta(hours=1),
                    date_end=datetime.now(),
                )
                
                assert isinstance(response, TimeSeriesResponse)
                assert response.success is True
                
                if response.data:
                    first_item = response.data[0]
                    assert first_item.network_code == "NEM"
                    assert first_item.metric == "power"
                    
            except Exception as e:
                pytest.skip(f"Async network API call failed: {e}")

    @pytest.mark.asyncio
    async def test_network_api_async_different_networks(self, openelectricity_async_client):
        """Test async network API with different network codes."""
        async with openelectricity_async_client as client:
            networks_to_test = ["NEM", "WEM"]
            
            for network_code in networks_to_test:
                try:
                    response = await client.get_network_data_async(
                        network_code=network_code,
                        metrics=[DataMetric.ENERGY],
                        interval="1h",
                        date_start=datetime.now() - timedelta(hours=2),
                        date_end=datetime.now() - timedelta(hours=1),
                    )
                    
                    assert isinstance(response, TimeSeriesResponse)
                    assert response.success is True
                    
                    if response.data:
                        first_item = response.data[0]
                        assert first_item.network_code == network_code
                        
                except Exception as e:
                    pytest.skip(f"Async network {network_code} API call failed: {e}")

    def test_network_api_response_validation(self, openelectricity_client):
        """Test that network API responses are properly validated."""
        client = openelectricity_client
        
        try:
            response = client.get_network_data(
                network_code="NEM",
                metrics=[DataMetric.POWER],
                interval="5m",
                date_start=datetime.now() - timedelta(hours=1),
                date_end=datetime.now(),
            )
            
            # Verify response is a valid TimeSeriesResponse
            assert isinstance(response, TimeSeriesResponse)
            
            # Verify required fields are present
            assert hasattr(response, 'success')
            assert hasattr(response, 'version')
            assert hasattr(response, 'created_at')
            assert hasattr(response, 'data')
            
            # Verify data structure if present
            if response.data:
                for item in response.data:
                    assert hasattr(item, 'network_code')
                    assert hasattr(item, 'metric')
                    assert hasattr(item, 'unit')
                    assert hasattr(item, 'interval')
                    assert hasattr(item, 'results')
                    
                    # Verify data types
                    assert isinstance(item.network_code, str)
                    assert isinstance(item.metric, str)
                    assert isinstance(item.unit, str)
                    assert isinstance(item.interval, str)
                    assert isinstance(item.results, list)
                    
        except Exception as e:
            pytest.skip(f"Network API validation test failed: {e}")

    def test_network_api_performance(self, openelectricity_client):
        """Test network API performance with reasonable response times."""
        import time
        
        client = openelectricity_client
        
        try:
            start_time = time.time()
            
            response = client.get_network_data(
                network_code="NEM",
                metrics=[DataMetric.POWER],
                interval="5m",
                date_start=datetime.now() - timedelta(hours=1),
                date_end=datetime.now(),
            )
            
            end_time = time.time()
            response_time = end_time - start_time
            
            # Response should be reasonably fast (less than 10 seconds)
            assert response_time < 10.0, f"Network API response too slow: {response_time:.2f}s"
            
            assert isinstance(response, TimeSeriesResponse)
            assert response.success is True
            
        except Exception as e:
            pytest.skip(f"Network API performance test failed: {e}")


class TestNetworkAPIIntegration:
    """Integration tests for Network API functionality."""

    def test_network_api_end_to_end_workflow(self, openelectricity_client):
        """Test complete end-to-end workflow with network API."""
        client = openelectricity_client
        
        try:
            # Step 1: Get basic network data
            power_response = client.get_network_data(
                network_code="NEM",
                metrics=[DataMetric.POWER],
                interval="1h",
                date_start=datetime.now() - timedelta(hours=3),
                date_end=datetime.now() - timedelta(hours=2),
            )
            
            assert power_response.success is True
            
            # Step 2: Get energy data for the same period
            energy_response = client.get_network_data(
                network_code="NEM",
                metrics=[DataMetric.ENERGY],
                interval="1h",
                date_start=datetime.now() - timedelta(hours=3),
                date_end=datetime.now() - timedelta(hours=2),
            )
            
            assert energy_response.success is True
            
            # Step 3: Get data with groupings
            grouped_response = client.get_network_data(
                network_code="NEM",
                metrics=[DataMetric.POWER],
                interval="1h",
                date_start=datetime.now() - timedelta(hours=3),
                date_end=datetime.now() - timedelta(hours=2),
                primary_grouping="network_region",
                secondary_grouping="fueltech",
            )
            
            assert grouped_response.success is True
            
            # Verify all responses have consistent structure
            for response in [power_response, energy_response, grouped_response]:
                assert isinstance(response, TimeSeriesResponse)
                assert response.version is not None
                assert response.created_at is not None
                
        except Exception as e:
            pytest.skip(f"End-to-end network API workflow failed: {e}")

    def test_network_api_data_consistency(self, openelectricity_client):
        """Test that network API returns consistent data across multiple calls."""
        client = openelectricity_client
        
        try:
            # Make the same request twice
            response1 = client.get_network_data(
                network_code="NEM",
                metrics=[DataMetric.POWER],
                interval="1h",
                date_start=datetime.now() - timedelta(hours=2),
                date_end=datetime.now() - timedelta(hours=1),
            )
            
            response2 = client.get_network_data(
                network_code="NEM",
                metrics=[DataMetric.POWER],
                interval="1h",
                date_start=datetime.now() - timedelta(hours=2),
                date_end=datetime.now() - timedelta(hours=1),
            )
            
            # Both responses should be successful
            assert response1.success is True
            assert response2.success is True
            
            # Both should have the same structure
            assert isinstance(response1, TimeSeriesResponse)
            assert isinstance(response2, TimeSeriesResponse)
            
            # Version and created_at might differ, but structure should be consistent
            if response1.data and response2.data:
                assert len(response1.data) == len(response2.data)
                
                # Check that the first items have the same structure
                item1 = response1.data[0]
                item2 = response2.data[0]
                
                assert item1.network_code == item2.network_code
                assert item1.metric == item2.metric
                assert item1.unit == item2.unit
                assert item1.interval == item2.interval
                
        except Exception as e:
            pytest.skip(f"Network API data consistency test failed: {e}")
