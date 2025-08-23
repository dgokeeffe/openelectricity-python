#!/usr/bin/env python
"""
Simple PySpark Example with OpenElectricity

This example demonstrates the new to_pyspark functionality
that automatically handles Spark session creation for both
Databricks and local environments.
"""

from openelectricity import OEClient
from openelectricity.types import MarketMetric
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def main():
    """Demonstrate the new to_pyspark functionality."""
    print("🚀 OpenElectricity PySpark Integration Demo")
    print("=" * 50)
    
    # Check if PySpark is available
    try:
        import pyspark
        print(f"✅ PySpark {pyspark.__version__} is available")
    except ImportError:
        print("❌ PySpark not available. Install with: uv add 'openelectricity[analysis]'")
        return
    
    # Initialize the client
    api_key = os.getenv("OPENELECTRICITY_API_KEY")
    if not api_key:
        print("❌ OPENELECTRICITY_API_KEY environment variable not set")
        return
    
    client = OEClient(api_key=api_key)
    
    # Test the new get_spark_session method
    print("\n🔧 Testing Spark session management...")
    try:
        spark = client.get_spark_session("OpenElectricity-Demo")
        print(f"✅ Successfully created Spark session: {spark.conf.get('spark.app.name')}")
    except Exception as e:
        print(f"❌ Failed to create Spark session: {e}")
        return
    
    # Fetch some data
    print("\n📊 Fetching market data...")
    try:
        response = client.get_market(
            network_code="NEM",
            metrics=[MarketMetric.PRICE, MarketMetric.DEMAND],
            interval="1h",
            date_start=datetime.now() - timedelta(days=1),
            date_end=datetime.now(),
            primary_grouping="network_region"
        )
        print(f"✅ Fetched {len(response.data)} time series")
        
        # Convert to PySpark DataFrame using the new method
        print("\n🔄 Converting to PySpark DataFrame...")
        spark_df = response.to_pyspark(spark_session=spark, app_name="OpenElectricity-Conversion")
        
        if spark_df is not None:
            print("✅ Successfully created PySpark DataFrame!")
            print(f"   Schema: {spark_df.schema}")
            print(f"   Row count: {spark_df.count()}")
            print(f"   Columns: {', '.join(spark_df.columns)}")
            
            # Show sample data
            print("\n📋 Sample data:")
            spark_df.show(5, truncate=False)
            
        else:
            print("❌ Failed to create PySpark DataFrame")
            
    except Exception as e:
        print(f"❌ Error during data fetch: {e}")
    
    # Test facilities data
    print("\n🏭 Testing facilities data conversion...")
    try:
        facilities_response = client.get_facilities(network_region="NSW1")
        print(f"✅ Fetched {len(facilities_response.data)} facilities")
        
        # Convert to PySpark DataFrame
        facilities_df = facilities_response.to_pyspark(spark_session=spark, app_name="OpenElectricity-Facilities")
        
        if facilities_df is not None:
            print("✅ Successfully created facilities PySpark DataFrame!")
            print(f"   Row count: {facilities_df.count()}")
            print(f"   Columns: {', '.join(facilities_df.columns)}")
            
            # Show sample data
            print("\n📋 Sample facilities data:")
            facilities_df.show(5, truncate=False)
            
        else:
            print("❌ Failed to create facilities PySpark DataFrame")
            
    except Exception as e:
        print(f"❌ Error during facilities fetch: {e}")
    
    print("\n🎉 Demo completed!")
    print("\n💡 Key features demonstrated:")
    print("   - Automatic Spark session management")
    print("   - Databricks vs local environment detection")
    print("   - Direct conversion to PySpark DataFrames")
    print("   - Proper error handling and logging")


if __name__ == "__main__":
    main()
