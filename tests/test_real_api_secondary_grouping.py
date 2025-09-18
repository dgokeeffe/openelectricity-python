"""
Simple test for PySpark reader behavior with real API and secondary groupings.

This test focuses on how the PySpark reader behaves when hitting the real OpenElectricity API
and what data it returns around secondary_groupings like fueltech_group.
"""

import os
import tempfile
from datetime import datetime, timedelta

import pytest
from pyspark.sql import SparkSession

from openelectricity.pyspark_datasource import OpenElectricityStreamDataSource, OpenElectricityStreamReader, RangePartition


class TestRealAPISecondaryGrouping:
    """Test PySpark reader behavior with real API and secondary groupings."""

    @pytest.fixture
    def spark_session(self):
        """Create SparkSession for testing."""
        spark = SparkSession.builder \
            .appName("TestRealAPISecondaryGrouping") \
            .master("local[1]") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()
        yield spark
        spark.stop()

    @pytest.fixture
    def test_time_range(self):
        """Define a small time range for testing (last 2 hours)."""
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=2)
        return start_time, end_time

    def test_pyspark_reader_without_secondary_grouping(self, spark_session, test_time_range):
        """Test PySpark reader behavior without secondary grouping."""
        start_time, end_time = test_time_range
        
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
                    # No secondary_grouping specified
                }
            )
            
            # Create partition and read data
            partition = RangePartition(start_time.isoformat(), end_time.isoformat())
            rows = list(reader.read(partition))
            
            # Convert to DataFrame
            df = spark_session.createDataFrame(rows, schema)
            
            print("=== REAL API DATA WITHOUT SECONDARY GROUPING ===")
            print(f"Total rows: {df.count()}")
            print(f"Schema fields: {[field.name for field in schema.fields]}")
            print("\nSample data:")
            df.show(10, truncate=False)
            
            # Analyze the data structure
            if df.count() > 0:
                # Check fueltech_group column behavior
                fueltech_values = df.select("fueltech_group").distinct().collect()
                print(f"\nFueltech group values: {[row.fueltech_group for row in fueltech_values]}")
                
                # Check network_region values
                region_values = df.select("network_region").distinct().collect()
                region_names = [row.network_region for row in region_values]
                print(f"Network regions: {region_names}")
                
                # Check power values
                power_stats = df.select("power").describe().collect()
                print("\nPower statistics:")
                for row in power_stats:
                    print(f"  {row.summary}: {row.power}")
                
                # Verify that fueltech_group is null when not specified
                null_fueltech_count = df.filter(df.fueltech_group.isNull()).count()
                total_count = df.count()
                print(f"\nNull fueltech_group values: {null_fueltech_count}/{total_count}")
                
                assert null_fueltech_count == total_count, "All fueltech_group values should be null when secondary grouping is not specified"

    def test_pyspark_reader_with_secondary_grouping(self, spark_session, test_time_range):
        """Test PySpark reader behavior with secondary grouping (fueltech_group)."""
        start_time, end_time = test_time_range
        
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
            partition = RangePartition(start_time.isoformat(), end_time.isoformat())
            rows = list(reader.read(partition))
            
            # Convert to DataFrame
            df = spark_session.createDataFrame(rows, schema)
            
            print("\n=== REAL API DATA WITH SECONDARY GROUPING ===")
            print(f"Total rows: {df.count()}")
            print(f"Schema fields: {[field.name for field in schema.fields]}")
            print("\nSample data:")
            df.show(10, truncate=False)
            
            # Analyze the data structure
            if df.count() > 0:
                # Check fueltech_group column behavior
                fueltech_values = df.select("fueltech_group").distinct().collect()
                fueltech_names = [row.fueltech_group for row in fueltech_values if row.fueltech_group is not None]
                print(f"\nFueltech group values: {fueltech_names}")
                
                # Check network_region values
                region_values = df.select("network_region").distinct().collect()
                region_names = [row.network_region for row in region_values if row.network_region is not None]
                print(f"Network regions: {region_names}")
                
                # Check power values
                power_stats = df.select("power").describe().collect()
                print("\nPower statistics:")
                for row in power_stats:
                    print(f"  {row.summary}: {row.power}")
                
                # Verify that fueltech_group has actual values when specified
                non_null_fueltech_count = df.filter(df.fueltech_group.isNotNull()).count()
                total_count = df.count()
                print(f"\nNon-null fueltech_group values: {non_null_fueltech_count}/{total_count}")
                
                if non_null_fueltech_count > 0:
                    print("✓ Secondary grouping is working - fueltech_group has actual values")
                    
                    # Show some fueltech combinations
                    combinations = df.select("network_region", "fueltech_group").distinct().collect()
                    print(f"\nRegion-Fueltech combinations found:")
                    for row in combinations:
                        if row.network_region and row.fueltech_group:
                            print(f"  {row.network_region} - {row.fueltech_group}")
                else:
                    print("⚠ Secondary grouping may not be working - all fueltech_group values are null")

    def test_compare_data_volume_with_and_without_secondary_grouping(self, spark_session, test_time_range):
        """Compare data volume between with and without secondary grouping."""
        start_time, end_time = test_time_range
        
        print("\n=== DATA VOLUME COMPARISON ===")
        
        # Test without secondary grouping
        with tempfile.TemporaryDirectory() as temp_dir:
            progress_path = os.path.join(temp_dir, "progress.json")
            
            data_source = OpenElectricityStreamDataSource({})
            schema = data_source.schema()
            
            reader_no_secondary = OpenElectricityStreamReader(
                schema=schema,
                options={
                    "network_code": "NEM",
                    "metrics": ["power"],
                    "interval": "5m",
                    "progress_path": progress_path,
                    "primary_grouping": "network_region"
                }
            )
            
            partition = RangePartition(start_time.isoformat(), end_time.isoformat())
            rows_no_secondary = list(reader_no_secondary.read(partition))
        
        # Test with secondary grouping
        with tempfile.TemporaryDirectory() as temp_dir:
            progress_path = os.path.join(temp_dir, "progress.json")
            
            data_source = OpenElectricityStreamDataSource({})
            schema = data_source.schema()
            
            reader_with_secondary = OpenElectricityStreamReader(
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
            
            partition = RangePartition(start_time.isoformat(), end_time.isoformat())
            rows_with_secondary = list(reader_with_secondary.read(partition))
        
        print(f"Rows without secondary grouping: {len(rows_no_secondary)}")
        print(f"Rows with secondary grouping: {len(rows_with_secondary)}")
        
        if len(rows_no_secondary) > 0 and len(rows_with_secondary) > 0:
            volume_ratio = len(rows_with_secondary) / len(rows_no_secondary)
            print(f"Volume increase: {volume_ratio:.1f}x")
            
            if volume_ratio > 1:
                print("✓ Secondary grouping increases data volume as expected")
            else:
                print("⚠ Secondary grouping doesn't increase data volume - may not be working")
        else:
            print("⚠ No data returned from API - check API key and network availability")

    def test_aggregation_capability_with_real_data(self, spark_session, test_time_range):
        """Test PySpark's ability to aggregate secondary grouping data."""
        start_time, end_time = test_time_range
        
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
            
            partition = RangePartition(start_time.isoformat(), end_time.isoformat())
            rows = list(reader.read(partition))
            
            df = spark_session.createDataFrame(rows, schema)
            
            print("\n=== AGGREGATION CAPABILITY WITH REAL DATA ===")
            
            if df.count() > 0:
                # Test aggregation by region (sum across fueltechs)
                region_totals = df.groupBy("network_region").sum("power").collect()
                print("Region totals (sum across fueltechs):")
                for row in region_totals:
                    if row.network_region:
                        print(f"  {row.network_region}: {row['sum(power)']:.2f} MW")
                
                # Test aggregation by fueltech (sum across regions)
                fueltech_totals = df.groupBy("fueltech_group").sum("power").collect()
                print("\nFueltech totals (sum across regions):")
                for row in fueltech_totals:
                    if row.fueltech_group:
                        print(f"  {row.fueltech_group}: {row['sum(power)']:.2f} MW")
                
                # Test time-based aggregation
                time_totals = df.groupBy("interval").sum("power").collect()
                print(f"\nTime-based totals (sum across all regions and fueltechs):")
                for row in time_totals[:5]:  # Show first 5 time points
                    if row.interval:
                        print(f"  {row.interval}: {row['sum(power)']:.2f} MW")
                
                print("✓ PySpark aggregation capabilities work with real API data")
            else:
                print("⚠ No data available for aggregation testing")
