"""
Focused test for secondary grouping behavior in PySpark reader.

This test specifically examines how the PySpark reader behaves with secondary groupings
like fueltech_group, and what data structure it returns.
"""

import os
import tempfile
from datetime import datetime
from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.testing.utils import assertDataFrameEqual

from openelectricity.client import OEClient
from openelectricity.models.timeseries import TimeSeriesResponse
from openelectricity.pyspark_datasource import OpenElectricityStreamDataSource, OpenElectricityStreamReader, RangePartition


class TestSecondaryGroupingBehavior:
    """Test how secondary groupings affect PySpark reader behavior."""

    @pytest.fixture
    def spark_session(self):
        """Create SparkSession for testing."""
        spark = SparkSession.builder \
            .appName("TestSecondaryGroupingBehavior") \
            .master("local[1]") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()
        yield spark
        spark.stop()

    @pytest.fixture
    def mock_response_no_secondary_grouping(self):
        """Mock response with only primary grouping (network_region)."""
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
                            ]
                        }
                    ]
                }
            ]
        }

    @pytest.fixture
    def mock_response_with_secondary_grouping(self):
        """Mock response with primary + secondary grouping (network_region + fueltech_group)."""
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
                            ]
                        }
                    ]
                }
            ]
        }

    def test_data_structure_without_secondary_grouping(self, spark_session, mock_response_no_secondary_grouping):
        """Test data structure when no secondary grouping is used."""
        with patch('openelectricity.pyspark_datasource.OEClient') as mock_client:
            mock_client.return_value.get_network_data.return_value = TimeSeriesResponse.model_validate(mock_response_no_secondary_grouping)
            
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
                
                # Create partition and read data
                partition = RangePartition("2025-09-18T21:00:00", "2025-09-18T22:00:00")
                rows = list(reader.read(partition))
                
                # Convert to DataFrame
                df = spark_session.createDataFrame(rows, schema)
                
                print("=== DATA WITHOUT SECONDARY GROUPING ===")
                print(f"Total rows: {df.count()}")
                print(f"Schema fields: {[field.name for field in schema.fields]}")
                print("\nSample data:")
                df.show(10, truncate=False)
                
                # Verify data structure
                assert df.count() == 4  # 2 regions × 2 time points
                
                # Check that fueltech_group column exists but is null
                fueltech_values = df.select("fueltech_group").distinct().collect()
                assert len(fueltech_values) == 1  # Only null values
                assert fueltech_values[0].fueltech_group is None
                
                # Check network_region values
                region_values = df.select("network_region").distinct().collect()
                region_names = [row.network_region for row in region_values]
                assert set(region_names) == {"NSW1", "QLD1"}
                
                # Verify power values are aggregated (total for each region)
                nsw1_power = df.filter(df.network_region == "NSW1").select("power").collect()
                assert len(nsw1_power) == 2
                assert nsw1_power[0].power == 5000.5
                assert nsw1_power[1].power == 5100.2

    def test_data_structure_with_secondary_grouping(self, spark_session, mock_response_with_secondary_grouping):
        """Test data structure when secondary grouping (fueltech_group) is used."""
        with patch('openelectricity.pyspark_datasource.OEClient') as mock_client:
            mock_client.return_value.get_network_data.return_value = TimeSeriesResponse.model_validate(mock_response_with_secondary_grouping)
            
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
                
                # Create partition and read data
                partition = RangePartition("2025-09-18T21:00:00", "2025-09-18T22:00:00")
                rows = list(reader.read(partition))
                
                # Convert to DataFrame
                df = spark_session.createDataFrame(rows, schema)
                
                print("\n=== DATA WITH SECONDARY GROUPING ===")
                print(f"Total rows: {df.count()}")
                print(f"Schema fields: {[field.name for field in schema.fields]}")
                print("\nSample data:")
                df.show(10, truncate=False)
                
                # Verify data structure
                assert df.count() == 8  # 4 fueltech combinations × 2 time points
                
                # Check fueltech_group values
                fueltech_values = df.select("fueltech_group").distinct().collect()
                fueltech_names = [row.fueltech_group for row in fueltech_values]
                assert set(fueltech_names) == {"coal", "solar", "wind"}
                
                # Check network_region values
                region_values = df.select("network_region").distinct().collect()
                region_names = [row.network_region for row in region_values]
                assert set(region_names) == {"NSW1", "QLD1"}
                
                # Verify specific combinations exist
                combinations = df.select("network_region", "fueltech_group").distinct().collect()
                combination_tuples = [(row.network_region, row.fueltech_group) for row in combinations]
                expected_combinations = [
                    ("NSW1", "coal"),
                    ("NSW1", "solar"),
                    ("QLD1", "coal"),
                    ("QLD1", "wind")
                ]
                assert set(combination_tuples) == set(expected_combinations)
                
                # Verify power values are disaggregated (specific to each fueltech)
                nsw1_coal_power = df.filter(
                    (df.network_region == "NSW1") & (df.fueltech_group == "coal")
                ).select("power").collect()
                assert len(nsw1_coal_power) == 2
                assert nsw1_coal_power[0].power == 3000.5
                assert nsw1_coal_power[1].power == 3100.2

    def test_secondary_grouping_impact_on_data_volume(self, spark_session, mock_response_no_secondary_grouping, mock_response_with_secondary_grouping):
        """Test how secondary grouping affects data volume."""
        print("\n=== DATA VOLUME COMPARISON ===")
        
        # Test without secondary grouping
        with patch('openelectricity.pyspark_datasource.OEClient') as mock_client:
            mock_client.return_value.get_network_data.return_value = TimeSeriesResponse.model_validate(mock_response_no_secondary_grouping)
            
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
                
                partition = RangePartition("2025-09-18T21:00:00", "2025-09-18T22:00:00")
                rows_no_secondary = list(reader.read(partition))
                
        # Test with secondary grouping
        with patch('openelectricity.pyspark_datasource.OEClient') as mock_client:
            mock_client.return_value.get_network_data.return_value = TimeSeriesResponse.model_validate(mock_response_with_secondary_grouping)
            
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
                
                partition = RangePartition("2025-09-18T21:00:00", "2025-09-18T22:00:00")
                rows_with_secondary = list(reader.read(partition))
        
        print(f"Rows without secondary grouping: {len(rows_no_secondary)}")
        print(f"Rows with secondary grouping: {len(rows_with_secondary)}")
        print(f"Volume increase: {len(rows_with_secondary) / len(rows_no_secondary):.1f}x")
        
        # Assertions
        assert len(rows_no_secondary) == 4  # 2 regions × 2 time points
        assert len(rows_with_secondary) == 8  # 4 fueltech combinations × 2 time points
        assert len(rows_with_secondary) == 2 * len(rows_no_secondary)  # Doubled due to fueltech breakdown

    def test_secondary_grouping_aggregation_capability(self, spark_session, mock_response_with_secondary_grouping):
        """Test PySpark's ability to aggregate secondary grouping data back to primary level."""
        with patch('openelectricity.pyspark_datasource.OEClient') as mock_client:
            mock_client.return_value.get_network_data.return_value = TimeSeriesResponse.model_validate(mock_response_with_secondary_grouping)
            
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
                
                partition = RangePartition("2025-09-18T21:00:00", "2025-09-18T22:00:00")
                rows = list(reader.read(partition))
                
                df = spark_session.createDataFrame(rows, schema)
                
                print("\n=== AGGREGATION CAPABILITY TEST ===")
                
                # Test aggregation by region (sum across fueltechs)
                region_totals = df.groupBy("network_region", "interval").sum("power").collect()
                print("Region totals (sum across fueltechs):")
                for row in region_totals:
                    print(f"  {row.network_region} at {row.interval}: {row['sum(power)']}")
                
                # Test aggregation by fueltech (sum across regions)
                fueltech_totals = df.groupBy("fueltech_group", "interval").sum("power").collect()
                print("\nFueltech totals (sum across regions):")
                for row in fueltech_totals:
                    print(f"  {row.fueltech_group} at {row.interval}: {row['sum(power)']}")
                
                # Verify specific aggregations
                nsw1_total_first_time = df.filter(
                    (df.network_region == "NSW1") & 
                    (df.interval == datetime.fromisoformat("2025-09-18T21:00:00+10:00"))
                ).agg({"power": "sum"}).collect()[0][0]
                
                # Should be 3000.5 (coal) + 2000.0 (solar) = 5000.5
                expected_nsw1_total = 3000.5 + 2000.0
                assert abs(nsw1_total_first_time - expected_nsw1_total) < 0.1
                print(f"\nNSW1 total verification: {nsw1_total_first_time} ≈ {expected_nsw1_total}")

    def test_secondary_grouping_data_quality(self, spark_session, mock_response_with_secondary_grouping):
        """Test data quality aspects of secondary grouping."""
        with patch('openelectricity.pyspark_datasource.OEClient') as mock_client:
            mock_client.return_value.get_network_data.return_value = TimeSeriesResponse.model_validate(mock_response_with_secondary_grouping)
            
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
                
                partition = RangePartition("2025-09-18T21:00:00", "2025-09-18T22:00:00")
                rows = list(reader.read(partition))
                
                df = spark_session.createDataFrame(rows, schema)
                
                print("\n=== DATA QUALITY TEST ===")
                
                # Test data completeness
                null_counts = df.select([
                    df.network_region.isNull().alias("null_regions"),
                    df.fueltech_group.isNull().alias("null_fueltechs"),
                    df.power.isNull().alias("null_power"),
                    df.interval.isNull().alias("null_intervals")
                ]).collect()[0]
                
                print(f"Null counts - regions: {null_counts.null_regions}, fueltechs: {null_counts.null_fueltechs}, power: {null_counts.null_power}, intervals: {null_counts.null_intervals}")
                
                # No nulls should exist in key fields
                assert null_counts.null_regions == 0
                assert null_counts.null_fueltechs == 0
                assert null_counts.null_power == 0
                assert null_counts.null_intervals == 0
                
                # Test data consistency
                negative_power = df.filter(df.power < 0).count()
                assert negative_power == 0
                print(f"Negative power values: {negative_power}")
                
                # Test fueltech naming consistency
                fueltech_names = df.select("fueltech_group").distinct().collect()
                fueltech_names = [row.fueltech_group for row in fueltech_names]
                
                # All fueltech names should be lowercase
                assert all(name.islower() for name in fueltech_names)
                print(f"Fueltech names: {fueltech_names} (all lowercase: {all(name.islower() for name in fueltech_names)})")
                
                # All fueltech names should be non-empty
                assert all(len(name) > 0 for name in fueltech_names)
                print("All fueltech names are non-empty")
