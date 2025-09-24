"""
Example showing append writes for backfill instead of union.

This example demonstrates how to write each chunk directly to storage
instead of unioning all DataFrames in memory.
"""

from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from openelectricity.backfill_utils import create_backfill_dataframe


def main():
    """Demonstrate append write approach for backfill."""
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("AppendBackfillExample") \
        .master("local[1]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    
    try:
        print("=== APPEND WRITE BACKFILL EXAMPLE ===\n")
        
        # Example 1: Append write approach (RECOMMENDED)
        print("1. Append write approach (memory efficient):")
        
        start_time = datetime.now() - timedelta(days=30)
        end_time = datetime.now() - timedelta(days=1)
        
        # Write each chunk directly to storage
        create_backfill_dataframe(
            spark=spark,
            start_time=start_time,
            end_time=end_time,
            api_key="your_api_key_here",
            network_code="NEM",
            metrics=["power"],
            interval="5m",
            chunk_days=4,
            primary_grouping="network_region",
            output_path="/tmp/backfill_data",  # Write directly to storage
            write_mode="overwrite"  # First chunk overwrites, rest append
        )
        
        print("   ✓ Each chunk written directly to /tmp/backfill_data")
        print("   ✓ Memory efficient - no large DataFrame unions")
        print("   ✓ Progress visible as each chunk completes")
        print("   ✓ Can resume if interrupted\n")
        
        # Example 2: Union approach (for comparison)
        print("2. Union approach (loads all data in memory):")
        
        df = create_backfill_dataframe(
            spark=spark,
            start_time=start_time,
            end_time=end_time,
            api_key="your_api_key_here",
            network_code="NEM",
            metrics=["power"],
            interval="5m",
            chunk_days=4,
            primary_grouping="network_region"
            # No output_path = union all chunks in memory
        )
        
        print("   ✓ All chunks unioned into single DataFrame")
        print("   ⚠️  Uses more memory")
        print("   ⚠️  No progress visibility until complete")
        print("   ⚠️  Must hold all data in memory\n")
        
        # Example 3: Different write modes
        print("3. Different write modes:")
        
        # Overwrite mode
        create_backfill_dataframe(
            spark=spark,
            start_time=start_time,
            end_time=end_time,
            api_key="your_api_key_here",
            output_path="/tmp/backfill_overwrite",
            write_mode="overwrite"  # First chunk overwrites, rest append
        )
        print("   ✓ Overwrite mode: First chunk overwrites, rest append")
        
        # Append mode
        create_backfill_dataframe(
            spark=spark,
            start_time=start_time,
            end_time=end_time,
            api_key="your_api_key_here",
            output_path="/tmp/backfill_append",
            write_mode="append"  # All chunks append
        )
        print("   ✓ Append mode: All chunks append")
        
        # Example 4: Resume interrupted backfill
        print("\n4. Resume interrupted backfill:")
        
        # If backfill was interrupted, you can resume from where you left off
        # by checking what data already exists and continuing from there
        
        print("   ✓ Check existing data in output_path")
        print("   ✓ Calculate remaining time range")
        print("   ✓ Continue backfill from last completed chunk")
        print("   ✓ Append new chunks to existing data\n")
        
        print("=== BENEFITS OF APPEND WRITE ===")
        print("• Memory efficient: No large DataFrame unions")
        print("• Progress visible: See each chunk complete")
        print("• Resumable: Can continue interrupted backfills")
        print("• Scalable: Works with any amount of historical data")
        print("• Fault tolerant: If one chunk fails, others still succeed")
        
        print("\n=== WHEN TO USE EACH APPROACH ===")
        print("• Append write: Large backfills, production use")
        print("• Union: Small backfills, interactive analysis")
        print("• Append write: When you want to save data permanently")
        print("• Union: When you want to process data immediately")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
