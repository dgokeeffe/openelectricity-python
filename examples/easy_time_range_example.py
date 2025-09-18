"""
Example showing how to easily control time ranges in PySpark data source.

This example demonstrates the new time range control options that make it
much easier to set custom start times without manually manipulating progress files.
"""

from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from openelectricity.pyspark_datasource import OpenElectricityStreamDataSource


def main():
    """Demonstrate easy time range control."""
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("EasyTimeRangeExample") \
        .master("local[1]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    
    try:
        print("=== EASY TIME RANGE CONTROL EXAMPLES ===\n")
        
        # Get the data source schema
        data_source = OpenElectricityStreamDataSource({})
        schema = data_source.schema()
        
        # Example 1: Start from 6 hours ago
        print("1. Starting from 6 hours ago:")
        df1 = spark.readStream \
            .format("openelectricity_stream") \
            .option("api_key", "your_api_key_here") \
            .option("network_code", "NEM") \
            .option("metrics", "power") \
            .option("interval", "5m") \
            .option("primary_grouping", "network_region") \
            .option("start_hours_ago", 6) \
            .option("batch_size_hours", 1) \
            .option("progress_path", "/tmp/openelectricity_progress/") \
            .schema(schema) \
            .load()
        
        print("   ✓ Will fetch data from 6 hours ago")
        print("   ✓ Each batch will be 1 hour of data")
        print("   ✓ Progress will be tracked automatically\n")
        
        # Example 2: Start from a specific time
        print("2. Starting from a specific time:")
        specific_time = datetime.now() - timedelta(hours=4)
        
        df2 = spark.readStream \
            .format("openelectricity_stream") \
            .option("api_key", "your_api_key_here") \
            .option("network_code", "NEM") \
            .option("metrics", "power") \
            .option("interval", "5m") \
            .option("primary_grouping", "network_region") \
            .option("start_time", specific_time.isoformat()) \
            .option("batch_size_hours", 2) \
            .option("progress_path", "/tmp/openelectricity_progress/") \
            .schema(schema) \
            .load()
        
        print(f"   ✓ Will fetch data from: {specific_time}")
        print("   ✓ Each batch will be 2 hours of data")
        print("   ✓ Progress will be tracked automatically\n")
        
        # Example 3: Reset progress and start fresh
        print("3. Resetting progress and starting fresh:")
        
        df3 = spark.readStream \
            .format("openelectricity_stream") \
            .option("api_key", "your_api_key_here") \
            .option("network_code", "NEM") \
            .option("metrics", "power") \
            .option("interval", "5m") \
            .option("primary_grouping", "network_region") \
            .option("start_hours_ago", 3) \
            .option("reset_progress", True) \
            .option("batch_size_hours", 1) \
            .option("progress_path", "/tmp/openelectricity_progress/") \
            .schema(schema) \
            .load()
        
        print("   ✓ Will reset any existing progress")
        print("   ✓ Will start fresh from 3 hours ago")
        print("   ✓ Each batch will be 1 hour of data\n")
        
        # Example 4: Multiple metrics with custom time range
        print("4. Multiple metrics with custom time range:")
        
        df4 = spark.readStream \
            .format("openelectricity_stream") \
            .option("api_key", "your_api_key_here") \
            .option("network_code", "NEM") \
            .option("metrics", "power,energy,emissions") \
            .option("interval", "5m") \
            .option("primary_grouping", "network_region") \
            .option("secondary_grouping", "fueltech_group") \
            .option("start_hours_ago", 2) \
            .option("batch_size_hours", 0.5) \
            .option("progress_path", "/tmp/openelectricity_progress/") \
            .schema(schema) \
            .load()
        
        print("   ✓ Will fetch power, energy, and emissions data")
        print("   ✓ With secondary grouping by fueltech")
        print("   ✓ Starting from 2 hours ago")
        print("   ✓ Each batch will be 30 minutes of data\n")
        
        # Example 5: Historical data analysis
        print("5. Historical data analysis (24 hours ago):")
        
        df5 = spark.readStream \
            .format("openelectricity_stream") \
            .option("api_key", "your_api_key_here") \
            .option("network_code", "NEM") \
            .option("metrics", "power") \
            .option("interval", "5m") \
            .option("primary_grouping", "network_region") \
            .option("start_hours_ago", 24) \
            .option("batch_size_hours", 4) \
            .option("progress_path", "/tmp/openelectricity_progress/") \
            .schema(schema) \
            .load()
        
        print("   ✓ Will fetch 24 hours of historical data")
        print("   ✓ Each batch will be 4 hours of data")
        print("   ✓ Good for analyzing daily patterns\n")
        
        print("=== SUMMARY OF NEW OPTIONS ===")
        print("• start_hours_ago: Start from N hours ago (easiest option)")
        print("• start_time: Start from specific ISO timestamp")
        print("• reset_progress: Reset progress tracking and start fresh")
        print("• batch_size_hours: Control how much data per batch")
        print("• progress_path: Where to store progress tracking")
        print("\n=== USAGE PATTERNS ===")
        print("• Quick start: Use start_hours_ago")
        print("• Precise control: Use start_time with ISO format")
        print("• Fresh start: Use reset_progress=True")
        print("• Historical analysis: Use larger start_hours_ago values")
        print("• Real-time: Use smaller batch_size_hours values")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
