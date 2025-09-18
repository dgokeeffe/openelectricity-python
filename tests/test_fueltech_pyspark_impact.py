"""
Test the impact of fueltech grouping on PySpark custom data source.

This test demonstrates how fueltech_group affects:
1. Data structure and volume
2. DataFrame schema
3. Processing performance
4. Data flattening behavior
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


class TestFueltechPySparkImpact:
    """Test how fueltech grouping impacts PySpark data source processing."""

    @pytest.fixture
    def spark_session(self):
        """Create SparkSession for testing."""
        spark = SparkSession.builder \
            .appName("TestFueltechPySparkImpact") \
            .master("local[1]") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()
        yield spark
        spark.stop()

    @pytest.fixture
    def mock_response_no_fueltech(self):
        """Mock response without fueltech grouping - just network_region."""
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
                    "groupings": ["network_region"],
                    "network_timezone_offset": "+10:00",
                    "results": [
                        {
                            "name": "power_NSW1",
                            "date_start": "2025-09-18T21:00:00Z",
                            "date_end": "2025-09-18T22:00:00Z",
                            "columns": {"network_region": "NSW1"},
                            "data": [
                                ["2025-09-18T21:00:00+10:00", 5000.5],
                                ["2025-09-18T21:05:00+10:00", 5100.2],
                                ["2025-09-18T21:10:00+10:00", 4950.8],
                            ]
                        },
                        {
                            "name": "power_QLD1",
                            "date_start": "2025-09-18T21:00:00Z",
                            "date_end": "2025-09-18T22:00:00Z",
                            "columns": {"network_region": "QLD1"},
                            "data": [
                                ["2025-09-18T21:00:00+10:00", 4000.2],
                                ["2025-09-18T21:05:00+10:00", 4100.8],
                                ["2025-09-18T21:10:00+10:00", 3950.1],
                            ]
                        }
                    ]
                }
            ]
        }

    @pytest.fixture
    def mock_response_with_fueltech(self):
        """Mock response with fueltech grouping - network_region + fueltech_group."""
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
                        # NSW1 fueltech breakdown
                        {
                            "name": "power_NSW1_coal",
                            "date_start": "2025-09-18T21:00:00Z",
                            "date_end": "2025-09-18T22:00:00Z",
                            "columns": {"network_region": "NSW1", "fueltech_group": "coal"},
                            "data": [
                                ["2025-09-18T21:00:00+10:00", 3000.5],
                                ["2025-09-18T21:05:00+10:00", 3100.2],
                                ["2025-09-18T21:10:00+10:00", 2950.8],
                            ]
                        },
                        {
                            "name": "power_NSW1_solar",
                            "date_start": "2025-09-18T21:00:00Z",
                            "date_end": "2025-09-18T22:00:00Z",
                            "columns": {"network_region": "NSW1", "fueltech_group": "solar"},
                            "data": [
                                ["2025-09-18T21:00:00+10:00", 2000.0],
                                ["2025-09-18T21:05:00+10:00", 2100.5],
                                ["2025-09-18T21:10:00+10:00", 1950.3],
                            ]
                        },
                        # QLD1 fueltech breakdown
                        {
                            "name": "power_QLD1_coal",
                            "date_start": "2025-09-18T21:00:00Z",
                            "date_end": "2025-09-18T22:00:00Z",
                            "columns": {"network_region": "QLD1", "fueltech_group": "coal"},
                            "data": [
                                ["2025-09-18T21:00:00+10:00", 2500.2],
                                ["2025-09-18T21:05:00+10:00", 2600.8],
                                ["2025-09-18T21:10:00+10:00", 2450.1],
                            ]
                        },
                        {
                            "name": "power_QLD1_wind",
                            "date_start": "2025-09-18T21:00:00Z",
                            "date_end": "2025-09-18T22:00:00Z",
                            "columns": {"network_region": "QLD1", "fueltech_group": "wind"},
                            "data": [
                                ["2025-09-18T21:00:00+10:00", 1500.0],
                                ["2025-09-18T21:05:00+10:00", 1500.0],
                                ["2025-09-18T21:10:00+10:00", 1500.0],
                            ]
                        }
                    ]
                }
            ]
        }

    def test_data_volume_comparison(self, spark_session, mock_response_no_fueltech, mock_response_with_fueltech):
        """Test how fueltech grouping affects data volume."""
        from openelectricity.pyspark_datasource import OpenElectricityStreamDataSource, OpenElectricityStreamReader
        
        # Test without fueltech grouping
        with patch('openelectricity.pyspark_datasource.OEClient') as mock_client:
            mock_client.return_value.get_network_data.return_value = TimeSeriesResponse.model_validate(mock_response_no_fueltech)
            
            with tempfile.TemporaryDirectory() as temp_dir:
                progress_path = os.path.join(temp_dir, "progress.json")
                
                data_source = OpenElectricityStreamDataSource({})
                schema = data_source.schema()
                
                reader = OpenElectricityStreamReader(
                    schema=schema,
                    options={
                        "network_code": "NEM",
                        "metrics": ["power"],
                        "interval": "5m",
                        "progress_path": progress_path,
                        "primary_grouping": "network_region"
                    }
                )
                
                # Create mock partition
                from openelectricity.pyspark_datasource import RangePartition
                partition = RangePartition("2025-09-18T21:00:00", "2025-09-18T22:00:00")
                
                # Read data
                rows_no_fueltech = list(reader.read(partition))
                
        # Test with fueltech grouping
        with patch('openelectricity.pyspark_datasource.OEClient') as mock_client:
            mock_client.return_value.get_network_data.return_value = TimeSeriesResponse.model_validate(mock_response_with_fueltech)
            
            with tempfile.TemporaryDirectory() as temp_dir:
                progress_path = os.path.join(temp_dir, "progress.json")
                
                data_source = OpenElectricityStreamDataSource({})
                schema = data_source.schema()
                
                reader = OpenElectricityStreamReader(
                    schema=schema,
                    options={
                        "network_code": "NEM",
                        "metrics": ["power"],
                        "interval": "5m",
                        "progress_path": progress_path,
                        "primary_grouping": "network_region",
                        "secondary_grouping": "fueltech_group"
                    }
                )
                
                # Create mock partition
                partition = RangePartition("2025-09-18T21:00:00", "2025-09-18T22:00:00")
                
                # Read data
                rows_with_fueltech = list(reader.read(partition))
        
        # Assertions
        print(f"Rows without fueltech: {len(rows_no_fueltech)}")
        print(f"Rows with fueltech: {len(rows_with_fueltech)}")
        
        # Without fueltech: 2 regions × 3 time points = 6 rows
        assert len(rows_no_fueltech) == 6
        
        # With fueltech: 4 fueltech combinations × 3 time points = 12 rows
        assert len(rows_with_fueltech) == 12
        
        # Fueltech grouping doubles the data volume
        assert len(rows_with_fueltech) == 2 * len(rows_no_fueltech)

    def test_schema_differences(self, spark_session, mock_response_no_fueltech, mock_response_with_fueltech):
        """Test how fueltech grouping affects DataFrame schema."""
        from openelectricity.pyspark_datasource import OpenElectricityStreamDataSource
        
        # Test without fueltech grouping
        with patch('openelectricity.pyspark_datasource.OEClient') as mock_client:
            mock_client.return_value.get_network_data.return_value = TimeSeriesResponse.model_validate(mock_response_no_fueltech)
            
            data_source = OpenElectricityStreamDataSource({})
            schema_no_fueltech = data_source.schema(spark_session)
            
        # Test with fueltech grouping
        with patch('openelectricity.pyspark_datasource.OEClient') as mock_client:
            mock_client.return_value.get_network_data.return_value = TimeSeriesResponse.model_validate(mock_response_with_fueltech)
            
            data_source = OpenElectricityStreamDataSource({})
            schema_with_fueltech = data_source.schema(spark_session)
        
        # Both schemas should be the same - fueltech_group column is always present
        # but will be null when not used
        assert schema_no_fueltech == schema_with_fueltech
        
        # Verify fueltech_group field exists
        field_names = [field.name for field in schema_no_fueltech.fields]
        assert "fueltech_group" in field_names
        assert "network_region" in field_names

    def test_fueltech_data_structure(self, spark_session, mock_response_with_fueltech):
        """Test the structure of fueltech data in PySpark DataFrame."""
        from openelectricity.pyspark_datasource import OpenElectricityStreamDataSource, OpenElectricityStreamReader
        
        with patch('openelectricity.pyspark_datasource.OEClient') as mock_client:
            mock_client.return_value.get_network_data.return_value = TimeSeriesResponse.model_validate(mock_response_with_fueltech)
            
            with tempfile.TemporaryDirectory() as temp_dir:
                progress_path = os.path.join(temp_dir, "progress.json")
                
                data_source = OpenElectricityStreamDataSource({})
                schema = data_source.schema()
                
                reader = OpenElectricityStreamReader(
                    schema=schema,
                    options={
                        "network_code": "NEM",
                        "metrics": ["power"],
                        "interval": "5m",
                        "progress_path": progress_path,
                        "primary_grouping": "network_region",
                        "secondary_grouping": "fueltech_group"
                    }
                )
                
                # Create mock partition
                partition = RangePartition("2025-09-18T21:00:00", "2025-09-18T22:00:00")
                
                # Read data
                rows = list(reader.read(partition))
                
                # Convert to DataFrame
                df = spark_session.createDataFrame(rows, reader.schema)
                
                # Test fueltech_group values
                fueltech_values = df.select("fueltech_group").distinct().collect()
                fueltech_values = [row.fueltech_group for row in fueltech_values]
                
                expected_fueltechs = ["coal", "solar", "wind"]
                assert set(fueltech_values) == set(expected_fueltechs)
                
                # Test network_region values
                region_values = df.select("network_region").distinct().collect()
                region_values = [row.network_region for row in region_values]
                
                expected_regions = ["NSW1", "QLD1"]
                assert set(region_values) == set(expected_regions)
                
                # Test combinations
                combinations = df.select("network_region", "fueltech_group").distinct().collect()
                combinations = [(row.network_region, row.fueltech_group) for row in combinations]
                
                expected_combinations = [
                    ("NSW1", "coal"),
                    ("NSW1", "solar"),
                    ("QLD1", "coal"),
                    ("QLD1", "wind")
                ]
                assert set(combinations) == set(expected_combinations)

    def test_fueltech_aggregation_capability(self, spark_session, mock_response_with_fueltech):
        """Test PySpark's ability to aggregate fueltech data."""
        from openelectricity.pyspark_datasource import OpenElectricityStreamDataSource, OpenElectricityStreamReader
        
        with patch('openelectricity.pyspark_datasource.OEClient') as mock_client:
            mock_client.return_value.get_network_data.return_value = TimeSeriesResponse.model_validate(mock_response_with_fueltech)
            
            with tempfile.TemporaryDirectory() as temp_dir:
                progress_path = os.path.join(temp_dir, "progress.json")
                
                data_source = OpenElectricityStreamDataSource({})
                schema = data_source.schema()
                
                reader = OpenElectricityStreamReader(
                    schema=schema,
                    options={
                        "network_code": "NEM",
                        "metrics": ["power"],
                        "interval": "5m",
                        "progress_path": progress_path,
                        "primary_grouping": "network_region",
                        "secondary_grouping": "fueltech_group"
                    }
                )
                
                # Create mock partition
                partition = RangePartition("2025-09-18T21:00:00", "2025-09-18T22:00:00")
                
                # Read data
                rows = list(reader.read(partition))
                
                # Convert to DataFrame
                df = spark_session.createDataFrame(rows, reader.schema)
                
                # Test aggregation by region (sum across fueltechs)
                region_totals = df.groupBy("network_region", "interval").sum("power").collect()
                
                # Should have 2 regions × 3 time points = 6 aggregated rows
                assert len(region_totals) == 6
                
                # Test aggregation by fueltech (sum across regions)
                fueltech_totals = df.groupBy("fueltech_group", "interval").sum("power").collect()
                
                # Should have 3 fueltechs × 3 time points = 9 aggregated rows
                assert len(fueltech_totals) == 9
                
                # Verify specific aggregations
                nsw1_coal_total = df.filter(
                    (df.network_region == "NSW1") & 
                    (df.fueltech_group == "coal")
                ).agg({"power": "sum"}).collect()[0][0]
                
                # Should be 3000.5 + 3100.2 + 2950.8 = 9051.5
                assert abs(nsw1_coal_total - 9051.5) < 0.1

    def test_fueltech_performance_impact(self, spark_session):
        """Test the performance impact of fueltech grouping."""
        import time
        
        # Create a larger mock response with more fueltechs
        large_mock_response = {
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
                    "results": []
                }
            ]
        }
        
        # Generate data for 5 regions × 8 fueltechs × 12 time points = 480 rows
        regions = ["NSW1", "VIC1", "QLD1", "SA1", "TAS1"]
        fueltechs = ["coal", "gas", "solar", "wind", "hydro", "battery", "biomass", "oil"]
        
        for region in regions:
            for fueltech in fueltechs:
                result = {
                    "name": f"power_{region}_{fueltech}",
                    "date_start": "2025-09-18T21:00:00Z",
                    "date_end": "2025-09-18T22:00:00Z",
                    "columns": {"network_region": region, "fueltech_group": fueltech},
                    "data": [
                        [f"2025-09-18T21:{i:02d}:00+10:00", 1000.0 + i * 10] 
                        for i in range(0, 60, 5)  # 12 time points
                    ]
                }
                large_mock_response["data"][0]["results"].append(result)
        
        from openelectricity.pyspark_datasource import OpenElectricityStreamDataSource, OpenElectricityStreamReader
        
        with patch('openelectricity.pyspark_datasource.OEClient') as mock_client:
            mock_client.return_value.get_network_data.return_value = TimeSeriesResponse.model_validate(large_mock_response)
            
            with tempfile.TemporaryDirectory() as temp_dir:
                progress_path = os.path.join(temp_dir, "progress.json")
                
                data_source = OpenElectricityStreamDataSource({})
                schema = data_source.schema()
                
                reader = OpenElectricityStreamReader(
                    schema=schema,
                    options={
                        "network_code": "NEM",
                        "metrics": ["power"],
                        "interval": "5m",
                        "progress_path": progress_path,
                        "primary_grouping": "network_region",
                        "secondary_grouping": "fueltech_group"
                    }
                )
                
                # Measure processing time
                start_time = time.time()
                
                # Create mock partition
                partition = RangePartition("2025-09-18T21:00:00", "2025-09-18T22:00:00")
                
                # Read data
                rows = list(reader.read(partition))
                
                processing_time = time.time() - start_time
                
                # Convert to DataFrame
                df = spark_session.createDataFrame(rows, reader.schema)
                
                # Measure DataFrame operations
                start_time = time.time()
                
                # Test various operations
                df.count()
                df.select("fueltech_group").distinct().count()
                df.groupBy("network_region").sum("power").collect()
                
                df_processing_time = time.time() - start_time
                
                print(f"Data processing time: {processing_time:.3f}s")
                print(f"DataFrame operations time: {df_processing_time:.3f}s")
                print(f"Total rows processed: {len(rows)}")
                print(f"Unique fueltechs: {df.select('fueltech_group').distinct().count()}")
                print(f"Unique regions: {df.select('network_region').distinct().count()}")
                
                # Assertions
                assert len(rows) == 480  # 5 regions × 8 fueltechs × 12 time points
                assert df.select("fueltech_group").distinct().count() == 8
                assert df.select("network_region").distinct().count() == 5
                assert processing_time < 5.0  # Should process quickly
                assert df_processing_time < 2.0  # DataFrame operations should be fast

    def test_fueltech_data_quality(self, spark_session, mock_response_with_fueltech):
        """Test data quality aspects of fueltech grouping."""
        from openelectricity.pyspark_datasource import OpenElectricityStreamDataSource, OpenElectricityStreamReader
        
        with patch('openelectricity.pyspark_datasource.OEClient') as mock_client:
            mock_client.return_value.get_network_data.return_value = TimeSeriesResponse.model_validate(mock_response_with_fueltech)
            
            with tempfile.TemporaryDirectory() as temp_dir:
                progress_path = os.path.join(temp_dir, "progress.json")
                
                data_source = OpenElectricityStreamDataSource({})
                schema = data_source.schema()
                
                reader = OpenElectricityStreamReader(
                    schema=schema,
                    options={
                        "network_code": "NEM",
                        "metrics": ["power"],
                        "interval": "5m",
                        "progress_path": progress_path,
                        "primary_grouping": "network_region",
                        "secondary_grouping": "fueltech_group"
                    }
                )
                
                # Create mock partition
                partition = RangePartition("2025-09-18T21:00:00", "2025-09-18T22:00:00")
                
                # Read data
                rows = list(reader.read(partition))
                
                # Convert to DataFrame
                df = spark_session.createDataFrame(rows, reader.schema)
                
                # Test data completeness
                null_counts = df.select([
                    df.network_region.isNull().alias("null_regions"),
                    df.fueltech_group.isNull().alias("null_fueltechs"),
                    df.power.isNull().alias("null_power"),
                    df.interval.isNull().alias("null_intervals")
                ]).collect()[0]
                
                # No nulls should exist in key fields
                assert null_counts.null_regions == 0
                assert null_counts.null_fueltechs == 0
                assert null_counts.null_power == 0
                assert null_counts.null_intervals == 0
                
                # Test data consistency
                # All power values should be positive
                negative_power = df.filter(df.power < 0).count()
                assert negative_power == 0
                
                # All intervals should be valid timestamps
                invalid_intervals = df.filter(df.interval.isNull()).count()
                assert invalid_intervals == 0
                
                # Test fueltech naming consistency
                fueltech_names = df.select("fueltech_group").distinct().collect()
                fueltech_names = [row.fueltech_group for row in fueltech_names]
                
                # All fueltech names should be lowercase
                assert all(name.islower() for name in fueltech_names)
                
                # All fueltech names should be non-empty
                assert all(len(name) > 0 for name in fueltech_names)
