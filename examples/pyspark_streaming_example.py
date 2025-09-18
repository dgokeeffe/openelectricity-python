"""
Example usage of the OpenElectricity PySpark Streaming Data Source

This example demonstrates how to use the OpenElectricity streaming data source
to read network data in real-time using PySpark Structured Streaming.
"""

import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

from openelectricity.pyspark_datasource import OpenElectricityStreamDataSource


def main():
    """Main function demonstrating the streaming data source usage."""
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("OpenElectricityStreamingExample") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Register the custom data source
    spark.dataSource.register(OpenElectricityStreamDataSource)
    
    # Configuration for the streaming job
    api_key = os.getenv("OPENELECTRICITY_API_KEY")
    if not api_key:
        raise ValueError("OPENELECTRICITY_API_KEY environment variable must be set")
    
    # Define streaming options
    stream_options = {
        "api_key": api_key,
        "network_code": "NEM",  # National Electricity Market
        "metrics": ["power", "energy", "emissions"],  # Metrics to fetch
        "interval": "5m",  # 5-minute intervals
        "batch_size_hours": 1,  # Fetch 1 hour of data per batch
        "progress_path": "/tmp/openelectricity_progress/",  # Progress tracking location
        "primary_grouping": "network_region",  # Group by network region
        "secondary_grouping": "fueltech",  # Secondary grouping by fuel technology
    }
    
    # Create streaming DataFrame
    streaming_df = spark.readStream \
        .format("openelectricity_stream") \
        .options(**stream_options) \
        .load()
    
    # Add processing timestamp
    processed_df = streaming_df.withColumn("processing_timestamp", current_timestamp())
    
    # Filter and transform data (example: only show non-null power values)
    filtered_df = processed_df.filter(col("power").isNotNull())
    
    # Write to console for demonstration (in production, write to Delta table or other sink)
    query = filtered_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 20) \
        .trigger(processingTime="30 seconds") \
        .start()
    
    print("Streaming job started. Press Ctrl+C to stop.")
    
    try:
        # Wait for termination
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping streaming job...")
        query.stop()
    
    spark.stop()


def example_delta_table_write():
    """
    Example of writing streaming data to a Delta table.
    
    This is more suitable for production use cases.
    """
    
    spark = SparkSession.builder \
        .appName("OpenElectricityDeltaStreaming") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    # Register the custom data source
    spark.dataSource.register(OpenElectricityStreamDataSource)
    
    # Configuration
    api_key = os.getenv("OPENELECTRICITY_API_KEY")
    stream_options = {
        "api_key": api_key,
        "network_code": "NEM",
        "metrics": ["power", "energy", "emissions", "market_value"],
        "interval": "5m",
        "batch_size_hours": 2,
        "progress_path": "/tmp/openelectricity_progress/",
        "primary_grouping": "network_region",
    }
    
    # Create streaming DataFrame
    streaming_df = spark.readStream \
        .format("openelectricity_stream") \
        .options(**stream_options) \
        .load()
    
    # Add metadata columns
    enriched_df = streaming_df.withColumn("ingestion_timestamp", current_timestamp())
    
    # Write to Delta table
    query = enriched_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/openelectricity_checkpoint/") \
        .toTable("openelectricity.nem_data")
    
    return query


def example_with_different_networks():
    """
    Example showing how to stream data from different networks.
    """
    
    spark = SparkSession.builder \
        .appName("MultiNetworkStreaming") \
        .getOrCreate()
    
    spark.dataSource.register(OpenElectricityStreamDataSource)
    
    api_key = os.getenv("OPENELECTRICITY_API_KEY")
    
    # Stream from NEM (National Electricity Market)
    nem_stream = spark.readStream \
        .format("openelectricity_stream") \
        .option("api_key", api_key) \
        .option("network_code", "NEM") \
        .option("metrics", "power,energy") \
        .option("interval", "5m") \
        .option("batch_size_hours", "1") \
        .option("progress_path", "/tmp/nem_progress/") \
        .load()
    
    # Stream from WEM (Western Electricity Market)
    wem_stream = spark.readStream \
        .format("openelectricity_stream") \
        .option("api_key", api_key) \
        .option("network_code", "WEM") \
        .option("metrics", "power,energy") \
        .option("interval", "5m") \
        .option("batch_size_hours", "1") \
        .option("progress_path", "/tmp/wem_progress/") \
        .load()
    
    # Union the streams
    combined_stream = nem_stream.union(wem_stream)
    
    # Write combined data
    query = combined_stream.writeStream \
        .format("console") \
        .outputMode("append") \
        .trigger(processingTime="1 minute") \
        .start()
    
    return query


if __name__ == "__main__":
    main()
