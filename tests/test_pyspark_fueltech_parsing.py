"""
Test fueltech_group parsing and PySpark DataFrame creation with real API data.

This test verifies that the OpenElectricity API correctly parses fueltech_group
data and creates the expected PySpark DataFrame structure.
"""

import os
import tempfile
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.testing.utils import assertDataFrameEqual

from openelectricity.client import OEClient
from openelectricity.models.timeseries import TimeSeriesResponse
from openelectricity.types import DataMetric, Network


class TestFueltechGroupParsing:
    """Test fueltech_group parsing with PySpark testing utilities."""

    @pytest.fixture
    def spark_session(self):
        """Create a test Spark session."""
        spark = SparkSession.builder \
            .appName("TestFueltechGroupParsing") \
            .master("local[1]") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()
        yield spark
        spark.stop()

    @pytest.fixture
    def mock_fueltech_response(self):
        """Mock response with fueltech_group data."""
        return {
            "success": True,
            "version": "v4.0",
            "created_at": "2025-09-18T22:00:00Z",
            "data": [
                {
                    "network_code": "NEM",
                    "metric": "power",
                    "unit": "MW",
                    "interval": "5m",
                    "start": "2025-09-18T21:00:00Z",
                    "end": "2025-09-18T22:00:00Z",
                    "groupings": ["network_region", "fueltech_group"],
                    "network_timezone_offset": "+10:00",
                    "results": [
                        {
                            "name": "power_NSW1_coal",
                            "date_start": "2025-09-18T21:00:00Z",
                            "date_end": "2025-09-18T22:00:00Z",
                            "columns": {
                                "network_region": "NSW1",
                                "fueltech_group": "coal"
                            },
                            "data": [
                                ["2025-09-18T21:00:00+10:00", 3000.5],
                                ["2025-09-18T21:05:00+10:00", 3100.2],
                                ["2025-09-18T21:10:00+10:00", 2950.8]
                            ]
                        },
                        {
                            "name": "power_NSW1_solar",
                            "date_start": "2025-09-18T21:00:00Z",
                            "date_end": "2025-09-18T22:00:00Z",
                            "columns": {
                                "network_region": "NSW1",
                                "fueltech_group": "solar"
                            },
                            "data": [
                                ["2025-09-18T21:00:00+10:00", 2000.0],
                                ["2025-09-18T21:05:00+10:00", 2100.5],
                                ["2025-09-18T21:10:00+10:00", 1950.3]
                            ]
                        },
                        {
                            "name": "power_QLD1_coal",
                            "date_start": "2025-09-18T21:00:00Z",
                            "date_end": "2025-09-18T22:00:00Z",
                            "columns": {
                                "network_region": "QLD1",
                                "fueltech_group": "coal"
                            },
                            "data": [
                                ["2025-09-18T21:00:00+10:00", 4000.2],
                                ["2025-09-18T21:05:00+10:00", 4100.8],
                                ["2025-09-18T21:10:00+10:00", 3950.1]
                            ]
                        },
                        {
                            "name": "power_QLD1_wind",
                            "date_start": "2025-09-18T21:00:00Z",
                            "date_end": "2025-09-18T22:00:00Z",
                            "columns": {
                                "network_region": "QLD1",
                                "fueltech_group": "wind"
                            },
                            "data": [
                                ["2025-09-18T21:00:00+10:00", 1500.7],
                                ["2025-09-18T21:05:00+10:00", 1600.3],
                                ["2025-09-18T21:10:00+10:00", 1450.9]
                            ]
                        }
                    ]
                }
            ]
        }

    @pytest.fixture
    def expected_schema(self):
        """Expected PySpark schema for fueltech_group data."""
        return StructType([
            StructField("interval", TimestampType(), True),
            StructField("network_region", StringType(), True),
            StructField("fueltech_group", StringType(), True),
            StructField("power", DoubleType(), True),
        ])

    @pytest.fixture
    def expected_data(self):
        """Expected data for fueltech_group parsing."""
        from datetime import datetime
        return [
            (datetime.fromisoformat("2025-09-18T21:00:00+10:00"), "NSW1", "coal", 3000.5),
            (datetime.fromisoformat("2025-09-18T21:05:00+10:00"), "NSW1", "coal", 3100.2),
            (datetime.fromisoformat("2025-09-18T21:10:00+10:00"), "NSW1", "coal", 2950.8),
            (datetime.fromisoformat("2025-09-18T21:00:00+10:00"), "NSW1", "solar", 2000.0),
            (datetime.fromisoformat("2025-09-18T21:05:00+10:00"), "NSW1", "solar", 2100.5),
            (datetime.fromisoformat("2025-09-18T21:10:00+10:00"), "NSW1", "solar", 1950.3),
            (datetime.fromisoformat("2025-09-18T21:00:00+10:00"), "QLD1", "coal", 4000.2),
            (datetime.fromisoformat("2025-09-18T21:05:00+10:00"), "QLD1", "coal", 4100.8),
            (datetime.fromisoformat("2025-09-18T21:10:00+10:00"), "QLD1", "coal", 3950.1),
            (datetime.fromisoformat("2025-09-18T21:00:00+10:00"), "QLD1", "wind", 1500.7),
            (datetime.fromisoformat("2025-09-18T21:05:00+10:00"), "QLD1", "wind", 1600.3),
            (datetime.fromisoformat("2025-09-18T21:10:00+10:00"), "QLD1", "wind", 1450.9),
        ]

    def test_fueltech_group_parsing_with_mock(self, spark_session, mock_fueltech_response, expected_schema, expected_data):
        """Test fueltech_group parsing with mocked API response."""
        # Create TimeSeriesResponse from mock data
        response = TimeSeriesResponse.model_validate(mock_fueltech_response)
        
        # Convert to records
        records = response.to_records()
        
        # Verify records structure
        assert len(records) == 12  # 4 fueltech groups × 3 time points
        
        # Check that all expected fields are present
        for record in records:
            assert "interval" in record
            assert "network_region" in record
            assert "fueltech_group" in record
            assert "power" in record
        
        # Create expected DataFrame
        expected_df = spark_session.createDataFrame(expected_data, expected_schema)
        
        # Create actual DataFrame from records
        actual_df = spark_session.createDataFrame(records, expected_schema)
        
        # Sort both DataFrames for comparison
        expected_df_sorted = expected_df.orderBy("interval", "network_region", "fueltech_group")
        actual_df_sorted = actual_df.orderBy("interval", "network_region", "fueltech_group")
        
        # Use PySpark testing utility to assert DataFrame equality
        assertDataFrameEqual(actual_df_sorted, expected_df_sorted)

    def test_fueltech_group_parsing_with_real_api(self, spark_session):
        """Test fueltech_group parsing with real API call (if API key available)."""
        api_key = os.getenv("OPENELECTRICITY_API_KEY")
        if not api_key:
            pytest.skip("OPENELECTRICITY_API_KEY not available for real API test")
        
        # Create client
        client = OEClient(api_key=api_key)
        
        # Get data with fueltech_group grouping
        try:
            response = client.get_network_data(
                network_code="NEM",
                metrics=[DataMetric.POWER],
                interval="5m",
                date_start=datetime.now() - timedelta(hours=1),
                date_end=datetime.now(),
                primary_grouping="network_region",
                secondary_grouping="fueltech_group"
            )
        except Exception as e:
            pytest.skip(f"Real API call failed: {e}")
        
        # Verify response structure
        assert response.success is True
        assert len(response.data) > 0
        
        # Check that fueltech_group grouping is applied
        first_series = response.data[0]
        assert "fueltech_group" in first_series.groupings
        
        # Convert to records
        records = response.to_records()
        assert len(records) > 0
        
        # Verify that fueltech_group column is present
        for record in records:
            assert "fueltech_group" in record
            assert "network_region" in record
            assert "power" in record
            assert record["fueltech_group"] is not None
            assert record["network_region"] is not None
        
        # Create DataFrame and verify schema
        df = spark_session.createDataFrame(records)
        
        # Check that expected columns are present
        columns = df.columns
        assert "interval" in columns
        assert "network_region" in columns
        assert "fueltech_group" in columns
        assert "power" in columns
        
        # Verify data types
        schema = df.schema
        assert schema["interval"].dataType == TimestampType()
        assert schema["network_region"].dataType == StringType()
        assert schema["fueltech_group"].dataType == StringType()
        assert schema["power"].dataType == DoubleType()
        
        # Verify that we have data for multiple fueltech groups
        fueltech_groups = df.select("fueltech_group").distinct().collect()
        assert len(fueltech_groups) > 1, "Should have multiple fueltech groups"
        
        # Verify that we have data for multiple regions
        regions = df.select("network_region").distinct().collect()
        assert len(regions) > 1, "Should have multiple network regions"

    def test_fueltech_group_parsing_with_multiple_metrics(self, spark_session):
        """Test fueltech_group parsing with multiple metrics."""
        # Create a new mock response with both power and energy metrics
        mock_response = {
            "success": True,
            "version": "v4.0",
            "created_at": "2025-09-18T22:00:00Z",
            "data": [
                {
                    "network_code": "NEM",
                    "metric": "power",
                    "unit": "MW",
                    "interval": "5m",
                    "start": "2025-09-18T21:00:00Z",
                    "end": "2025-09-18T22:00:00Z",
                    "groupings": ["network_region", "fueltech_group"],
                    "network_timezone_offset": "+10:00",
                    "results": [
                        {
                            "name": "power_NSW1_coal",
                            "date_start": "2025-09-18T21:00:00Z",
                            "date_end": "2025-09-18T22:00:00Z",
                            "columns": {
                                "network_region": "NSW1",
                                "fueltech_group": "coal"
                            },
                            "data": [
                                ["2025-09-18T21:00:00+10:00", 3000.5],
                                ["2025-09-18T21:05:00+10:00", 3100.2],
                                ["2025-09-18T21:10:00+10:00", 2950.8]
                            ]
                        },
                        {
                            "name": "power_NSW1_solar",
                            "date_start": "2025-09-18T21:00:00Z",
                            "date_end": "2025-09-18T22:00:00Z",
                            "columns": {
                                "network_region": "NSW1",
                                "fueltech_group": "solar"
                            },
                            "data": [
                                ["2025-09-18T21:00:00+10:00", 2000.0],
                                ["2025-09-18T21:05:00+10:00", 2100.5],
                                ["2025-09-18T21:10:00+10:00", 1950.3]
                            ]
                        }
                    ]
                },
                {
                    "network_code": "NEM",
                    "metric": "energy",
                    "unit": "MWh",
                    "interval": "5m",
                    "start": "2025-09-18T21:00:00Z",
                    "end": "2025-09-18T22:00:00Z",
                    "groupings": ["network_region", "fueltech_group"],
                    "network_timezone_offset": "+10:00",
                    "results": [
                        {
                            "name": "energy_NSW1_coal",
                            "date_start": "2025-09-18T21:00:00Z",
                            "date_end": "2025-09-18T22:00:00Z",
                            "columns": {
                                "network_region": "NSW1",
                                "fueltech_group": "coal"
                            },
                            "data": [
                                ["2025-09-18T21:00:00+10:00", 250.0],
                                ["2025-09-18T21:05:00+10:00", 258.3],
                                ["2025-09-18T21:10:00+10:00", 245.9]
                            ]
                        },
                        {
                            "name": "energy_NSW1_solar",
                            "date_start": "2025-09-18T21:00:00Z",
                            "date_end": "2025-09-18T22:00:00Z",
                            "columns": {
                                "network_region": "NSW1",
                                "fueltech_group": "solar"
                            },
                            "data": [
                                ["2025-09-18T21:00:00+10:00", 166.7],
                                ["2025-09-18T21:05:00+10:00", 175.0],
                                ["2025-09-18T21:10:00+10:00", 162.5]
                            ]
                        }
                    ]
                }
            ]
        }
        
        # Create TimeSeriesResponse from mock data
        response = TimeSeriesResponse.model_validate(mock_response)
        
        # Convert to records
        records = response.to_records()
        
        # Verify records structure
        assert len(records) == 6  # 2 fueltech groups × 3 time points
        
        # Check that both metrics are present
        for record in records:
            assert "power" in record
            assert "energy" in record
            assert "network_region" in record
            assert "fueltech_group" in record
        
        # Create DataFrame
        df = spark_session.createDataFrame(records)
        
        # Verify schema includes both metrics
        columns = df.columns
        assert "power" in columns
        assert "energy" in columns
        assert "network_region" in columns
        assert "fueltech_group" in columns
        
        # Verify data types
        schema = df.schema
        assert schema["power"].dataType == DoubleType()
        assert schema["energy"].dataType == DoubleType()

    def test_fueltech_group_parsing_edge_cases(self, spark_session):
        """Test fueltech_group parsing with edge cases."""
        # Test with missing fueltech_group in some records
        mock_response = {
            "success": True,
            "version": "v4.0",
            "created_at": "2025-09-18T22:00:00Z",
            "data": [
                {
                    "network_code": "NEM",
                    "metric": "power",
                    "unit": "MW",
                    "interval": "5m",
                    "start": "2025-09-18T21:00:00Z",
                    "end": "2025-09-18T22:00:00Z",
                    "groupings": ["network_region", "fueltech_group"],
                    "network_timezone_offset": "+10:00",
                    "results": [
                        {
                            "name": "power_NSW1_coal",
                            "date_start": "2025-09-18T21:00:00Z",
                            "date_end": "2025-09-18T22:00:00Z",
                            "columns": {
                                "network_region": "NSW1",
                                "fueltech_group": "coal"
                            },
                            "data": [
                                ["2025-09-18T21:00:00+10:00", 3000.5]
                            ]
                        },
                        {
                            "name": "power_NSW1",
                            "date_start": "2025-09-18T21:00:00Z",
                            "date_end": "2025-09-18T22:00:00Z",
                            "columns": {
                                "network_region": "NSW1"
                                # Missing fueltech_group
                            },
                            "data": [
                                ["2025-09-18T21:00:00+10:00", 5000.0]
                            ]
                        }
                    ]
                }
            ]
        }
        
        # Create TimeSeriesResponse from mock data
        response = TimeSeriesResponse.model_validate(mock_response)
        
        # Convert to records
        records = response.to_records()
        
        # Verify records structure
        assert len(records) == 2
        
        # Check that missing fueltech_group is handled gracefully
        for record in records:
            assert "network_region" in record
            assert "power" in record
            # fueltech_group may be None for some records
            if "fueltech_group" in record:
                assert record["fueltech_group"] is None or isinstance(record["fueltech_group"], str)
        
        # Create DataFrame
        df = spark_session.createDataFrame(records)
        
        # Verify schema
        columns = df.columns
        assert "network_region" in columns
        assert "power" in columns
        assert "fueltech_group" in columns

    def test_fueltech_group_parsing_performance(self, spark_session, mock_fueltech_response):
        """Test performance of fueltech_group parsing with larger dataset."""
        # Expand mock data to simulate larger dataset
        expanded_results = []
        for i in range(10):  # 10 regions
            for j in range(5):  # 5 fueltech groups per region
                for k in range(12):  # 12 time points
                    expanded_results.append({
                        "name": f"power_region{i}_fueltech{j}",
                        "date_start": "2025-09-18T21:00:00Z",
                        "date_end": "2025-09-18T22:00:00Z",
                        "columns": {
                            "network_region": f"region{i}",
                            "fueltech_group": f"fueltech{j}"
                        },
                        "data": [
                            [f"2025-09-18T21:{k:02d}:00+10:00", 1000.0 + i * 100 + j * 10 + k]
                        ]
                    })
        
        mock_fueltech_response["data"][0]["results"] = expanded_results
        
        # Create TimeSeriesResponse from mock data
        response = TimeSeriesResponse.model_validate(mock_fueltech_response)
        
        # Convert to records
        records = response.to_records()
        
        # Verify records structure
        assert len(records) == 600  # 10 regions × 5 fueltech groups × 12 time points
        
        # Create DataFrame
        df = spark_session.createDataFrame(records)
        
        # Verify schema
        columns = df.columns
        assert "network_region" in columns
        assert "fueltech_group" in columns
        assert "power" in columns
        
        # Verify data types
        schema = df.schema
        assert schema["network_region"].dataType == StringType()
        assert schema["fueltech_group"].dataType == StringType()
        assert schema["power"].dataType == DoubleType()
        
        # Verify that we have the expected number of unique combinations
        unique_combinations = df.select("network_region", "fueltech_group").distinct().count()
        assert unique_combinations == 50  # 10 regions × 5 fueltech groups
