"""
Simple test for PySpark reader behavior with multiple metrics from the real API.

This test demonstrates how the PySpark reader behaves when requesting different
metrics available in the OpenElectricity API, showing the data structure and
values returned for each metric type.
"""

import os
import tempfile
from datetime import datetime, timedelta

import pytest
from pyspark.sql import SparkSession

from openelectricity.pyspark_datasource import OpenElectricityStreamDataSource, OpenElectricityStreamReader, RangePartition


class TestRealAPIMultipleMetrics:
    """Test PySpark reader behavior with multiple metrics from real API."""

    @pytest.fixture
    def spark_session(self):
        """Create SparkSession for testing."""
        spark = SparkSession.builder \
            .appName("TestRealAPIMultipleMetrics") \
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

    def test_pyspark_reader_with_power_metric(self, spark_session, test_time_range):
        """Test PySpark reader with power metric."""
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
            
            print("=== POWER METRIC ===")
            print(f"Total rows: {df.count()}")
            
            if df.count() > 0:
                # Check power values
                power_stats = df.select("power").describe().collect()
                print("Power statistics:")
                for row in power_stats:
                    print(f"  {row.summary}: {row.power}")
                
                # Check non-null power values
                non_null_power = df.filter(df.power.isNotNull()).count()
                print(f"Non-null power values: {non_null_power}/{df.count()}")
                
                if non_null_power > 0:
                    print("✓ Power metric is working - has actual values")
                else:
                    print("⚠ Power metric may not be working - all values are null")

    def test_pyspark_reader_with_energy_metric(self, spark_session, test_time_range):
        """Test PySpark reader with energy metric."""
        start_time, end_time = test_time_range
        
        with tempfile.TemporaryDirectory() as temp_dir:
            progress_path = os.path.join(temp_dir, "progress.json")
            
            data_source = OpenElectricityStreamDataSource({})
            schema = data_source.schema()
            
            reader = OpenElectricityStreamReader(
                schema=schema,
                options={
                    "network_code": "NEM",
                    "metrics": ["energy"],
                    "interval": "5m",
                    "progress_path": progress_path,
                    "primary_grouping": "network_region",
                    "secondary_grouping": "fueltech_group"
                }
            )
            
            partition = RangePartition(start_time.isoformat(), end_time.isoformat())
            rows = list(reader.read(partition))
            df = spark_session.createDataFrame(rows, schema)
            
            print("\n=== ENERGY METRIC ===")
            print(f"Total rows: {df.count()}")
            
            if df.count() > 0:
                # Check energy values
                energy_stats = df.select("energy").describe().collect()
                print("Energy statistics:")
                for row in energy_stats:
                    print(f"  {row.summary}: {row.energy}")
                
                # Check non-null energy values
                non_null_energy = df.filter(df.energy.isNotNull()).count()
                print(f"Non-null energy values: {non_null_energy}/{df.count()}")
                
                if non_null_energy > 0:
                    print("✓ Energy metric is working - has actual values")
                else:
                    print("⚠ Energy metric may not be working - all values are null")

    def test_pyspark_reader_with_emissions_metric(self, spark_session, test_time_range):
        """Test PySpark reader with emissions metric."""
        start_time, end_time = test_time_range
        
        with tempfile.TemporaryDirectory() as temp_dir:
            progress_path = os.path.join(temp_dir, "progress.json")
            
            data_source = OpenElectricityStreamDataSource({})
            schema = data_source.schema()
            
            reader = OpenElectricityStreamReader(
                schema=schema,
                options={
                    "network_code": "NEM",
                    "metrics": ["emissions"],
                    "interval": "5m",
                    "progress_path": progress_path,
                    "primary_grouping": "network_region",
                    "secondary_grouping": "fueltech_group"
                }
            )
            
            partition = RangePartition(start_time.isoformat(), end_time.isoformat())
            rows = list(reader.read(partition))
            df = spark_session.createDataFrame(rows, schema)
            
            print("\n=== EMISSIONS METRIC ===")
            print(f"Total rows: {df.count()}")
            
            if df.count() > 0:
                # Check emissions values
                emissions_stats = df.select("emissions").describe().collect()
                print("Emissions statistics:")
                for row in emissions_stats:
                    print(f"  {row.summary}: {row.emissions}")
                
                # Check non-null emissions values
                non_null_emissions = df.filter(df.emissions.isNotNull()).count()
                print(f"Non-null emissions values: {non_null_emissions}/{df.count()}")
                
                if non_null_emissions > 0:
                    print("✓ Emissions metric is working - has actual values")
                else:
                    print("⚠ Emissions metric may not be working - all values are null")

    def test_pyspark_reader_with_market_value_metric(self, spark_session, test_time_range):
        """Test PySpark reader with market_value metric."""
        start_time, end_time = test_time_range
        
        with tempfile.TemporaryDirectory() as temp_dir:
            progress_path = os.path.join(temp_dir, "progress.json")
            
            data_source = OpenElectricityStreamDataSource({})
            schema = data_source.schema()
            
            reader = OpenElectricityStreamReader(
                schema=schema,
                options={
                    "network_code": "NEM",
                    "metrics": ["market_value"],
                    "interval": "5m",
                    "progress_path": progress_path,
                    "primary_grouping": "network_region",
                    "secondary_grouping": "fueltech_group"
                }
            )
            
            partition = RangePartition(start_time.isoformat(), end_time.isoformat())
            rows = list(reader.read(partition))
            df = spark_session.createDataFrame(rows, schema)
            
            print("\n=== MARKET_VALUE METRIC ===")
            print(f"Total rows: {df.count()}")
            
            if df.count() > 0:
                # Check market_value values
                market_value_stats = df.select("market_value").describe().collect()
                print("Market value statistics:")
                for row in market_value_stats:
                    print(f"  {row.summary}: {row.market_value}")
                
                # Check non-null market_value values
                non_null_market_value = df.filter(df.market_value.isNotNull()).count()
                print(f"Non-null market_value values: {non_null_market_value}/{df.count()}")
                
                if non_null_market_value > 0:
                    print("✓ Market value metric is working - has actual values")
                else:
                    print("⚠ Market value metric may not be working - all values are null")

    def test_pyspark_reader_with_renewable_proportion_metric(self, spark_session, test_time_range):
        """Test PySpark reader with renewable_proportion metric."""
        start_time, end_time = test_time_range
        
        with tempfile.TemporaryDirectory() as temp_dir:
            progress_path = os.path.join(temp_dir, "progress.json")
            
            data_source = OpenElectricityStreamDataSource({})
            schema = data_source.schema()
            
            reader = OpenElectricityStreamReader(
                schema=schema,
                options={
                    "network_code": "NEM",
                    "metrics": ["renewable_proportion"],
                    "interval": "5m",
                    "progress_path": progress_path,
                    "primary_grouping": "network_region",
                    "secondary_grouping": "fueltech_group"
                }
            )
            
            partition = RangePartition(start_time.isoformat(), end_time.isoformat())
            rows = list(reader.read(partition))
            df = spark_session.createDataFrame(rows, schema)
            
            print("\n=== RENEWABLE_PROPORTION METRIC ===")
            print(f"Total rows: {df.count()}")
            
            if df.count() > 0:
                # Check renewable_proportion values
                renewable_stats = df.select("renewable_proportion").describe().collect()
                print("Renewable proportion statistics:")
                for row in renewable_stats:
                    print(f"  {row.summary}: {row.renewable_proportion}")
                
                # Check non-null renewable_proportion values
                non_null_renewable = df.filter(df.renewable_proportion.isNotNull()).count()
                print(f"Non-null renewable_proportion values: {non_null_renewable}/{df.count()}")
                
                if non_null_renewable > 0:
                    print("✓ Renewable proportion metric is working - has actual values")
                else:
                    print("⚠ Renewable proportion metric may not be working - all values are null")

    def test_pyspark_reader_with_storage_battery_metric(self, spark_session, test_time_range):
        """Test PySpark reader with storage_battery metric."""
        start_time, end_time = test_time_range
        
        with tempfile.TemporaryDirectory() as temp_dir:
            progress_path = os.path.join(temp_dir, "progress.json")
            
            data_source = OpenElectricityStreamDataSource({})
            schema = data_source.schema()
            
            reader = OpenElectricityStreamReader(
                schema=schema,
                options={
                    "network_code": "NEM",
                    "metrics": ["storage_battery"],
                    "interval": "5m",
                    "progress_path": progress_path,
                    "primary_grouping": "network_region",
                    "secondary_grouping": "fueltech_group"
                }
            )
            
            partition = RangePartition(start_time.isoformat(), end_time.isoformat())
            rows = list(reader.read(partition))
            df = spark_session.createDataFrame(rows, schema)
            
            print("\n=== STORAGE_BATTERY METRIC ===")
            print(f"Total rows: {df.count()}")
            
            if df.count() > 0:
                # Check storage_battery values
                storage_stats = df.select("storage_battery").describe().collect()
                print("Storage battery statistics:")
                for row in storage_stats:
                    print(f"  {row.summary}: {row.storage_battery}")
                
                # Check non-null storage_battery values
                non_null_storage = df.filter(df.storage_battery.isNotNull()).count()
                print(f"Non-null storage_battery values: {non_null_storage}/{df.count()}")
                
                if non_null_storage > 0:
                    print("✓ Storage battery metric is working - has actual values")
                else:
                    print("⚠ Storage battery metric may not be working - all values are null")

    def test_pyspark_reader_with_multiple_metrics(self, spark_session, test_time_range):
        """Test PySpark reader with multiple metrics at once."""
        start_time, end_time = test_time_range
        
        with tempfile.TemporaryDirectory() as temp_dir:
            progress_path = os.path.join(temp_dir, "progress.json")
            
            data_source = OpenElectricityStreamDataSource({})
            schema = data_source.schema()
            
            reader = OpenElectricityStreamReader(
                schema=schema,
                options={
                    "network_code": "NEM",
                    "metrics": ["power", "energy", "emissions"],
                    "interval": "5m",
                    "progress_path": progress_path,
                    "primary_grouping": "network_region",
                    "secondary_grouping": "fueltech_group"
                }
            )
            
            partition = RangePartition(start_time.isoformat(), end_time.isoformat())
            rows = list(reader.read(partition))
            df = spark_session.createDataFrame(rows, schema)
            
            print("\n=== MULTIPLE METRICS (power, energy, emissions) ===")
            print(f"Total rows: {df.count()}")
            
            if df.count() > 0:
                # Check each metric
                metrics_to_check = ["power", "energy", "emissions"]
                for metric in metrics_to_check:
                    non_null_count = df.filter(df[metric].isNotNull()).count()
                    print(f"Non-null {metric} values: {non_null_count}/{df.count()}")
                
                # Show sample data
                print("\nSample data with multiple metrics:")
                df.select("network_region", "fueltech_group", "power", "energy", "emissions").show(5, truncate=False)
                
                print("✓ Multiple metrics test completed")

    def test_compare_metric_data_availability(self, spark_session, test_time_range):
        """Compare data availability across different metrics."""
        start_time, end_time = test_time_range
        
        print("\n=== METRIC DATA AVAILABILITY COMPARISON ===")
        
        metrics_to_test = ["power", "energy", "emissions", "market_value", "renewable_proportion", "storage_battery"]
        results = {}
        
        for metric in metrics_to_test:
            with tempfile.TemporaryDirectory() as temp_dir:
                progress_path = os.path.join(temp_dir, "progress.json")
                
                data_source = OpenElectricityStreamDataSource({})
                schema = data_source.schema()
                
                reader = OpenElectricityStreamReader(
                    schema=schema,
                    options={
                        "network_code": "NEM",
                        "metrics": [metric],
                        "interval": "5m",
                        "progress_path": progress_path,
                        "primary_grouping": "network_region",
                        "secondary_grouping": "fueltech_group"
                    }
                )
                
                partition = RangePartition(start_time.isoformat(), end_time.isoformat())
                rows = list(reader.read(partition))
                df = spark_session.createDataFrame(rows, schema)
                
                total_rows = df.count()
                non_null_count = df.filter(df[metric].isNotNull()).count()
                availability_pct = (non_null_count / total_rows * 100) if total_rows > 0 else 0
                
                results[metric] = {
                    "total_rows": total_rows,
                    "non_null_count": non_null_count,
                    "availability_pct": availability_pct
                }
        
        # Print comparison table
        print(f"{'Metric':<20} {'Total Rows':<12} {'Non-Null':<10} {'Availability':<12}")
        print("-" * 60)
        for metric, data in results.items():
            print(f"{metric:<20} {data['total_rows']:<12} {data['non_null_count']:<10} {data['availability_pct']:.1f}%")
        
        # Identify most/least available metrics
        most_available = max(results.items(), key=lambda x: x[1]['availability_pct'])
        least_available = min(results.items(), key=lambda x: x[1]['availability_pct'])
        
        print(f"\nMost available metric: {most_available[0]} ({most_available[1]['availability_pct']:.1f}%)")
        print(f"Least available metric: {least_available[0]} ({least_available[1]['availability_pct']:.1f}%)")
