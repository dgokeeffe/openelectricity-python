#!/usr/bin/env python3
"""
Test script to identify which market metrics work with the get_market method.
"""

import asyncio
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

from openelectricity import AsyncOEClient
from openelectricity.settings_schema import settings
from openelectricity.types import MarketMetric

# Load environment variables from .env file
load_dotenv()


async def test_market_metric_combinations():
    """Test different combinations of market metrics to identify which ones work."""
    
    # Get API key from environment
    api_key = os.getenv("OPENELECTRICITY_API_KEY")
    if not api_key:
        print("❌ OPENELECTRICITY_API_KEY environment variable not set")
        print("Please create a .env file with your API key:")
        print("OPENELECTRICITY_API_KEY=your_api_key_here")
        return
    
    client = AsyncOEClient(api_key=api_key)
    
    # Test date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)
    
    # Individual market metrics to test
    all_market_metrics = [
        MarketMetric.PRICE,
        MarketMetric.DEMAND,
        MarketMetric.DEMAND_ENERGY,
        MarketMetric.CURTAILMENT_SOLAR,
        MarketMetric.CURTAILMENT_WIND,
    ]
    
    print("🔍 Testing individual market metrics...")
    
    # Test each market metric individually
    for metric in all_market_metrics:
        try:
            print(f"  Testing {metric.value}...", end=" ")
            response = await client.get_market(
                network_code="NEM",
                metrics=[metric],
                interval="5m",
                date_start=start_date,
                date_end=end_date,
                with_clerk=True,
            )
            print("✅ SUCCESS")
        except Exception as e:
            print(f"❌ FAILED: {e}")
    
    print("\n🔍 Testing market metric combinations...")
    
    # Test combinations of 2 market metrics
    for i, metric1 in enumerate(all_market_metrics):
        for metric2 in all_market_metrics[i+1:]:
            try:
                print(f"  Testing {metric1.value} + {metric2.value}...", end=" ")
                response = await client.get_market(
                    network_code="NEM",
                    metrics=[metric1, metric2],
                    interval="5m",
                    date_start=start_date,
                    date_end=end_date,
                    with_clerk=True,
                )
                print("✅ SUCCESS")
            except Exception as e:
                print(f"❌ FAILED: {e}")
    
    print("\n🔍 Testing all market metrics together...")
    
    # Test all market metrics together
    try:
        print("  Testing all market metrics...", end=" ")
        response = await client.get_market(
            network_code="NEM",
            metrics=all_market_metrics,
            interval="5m",
            date_start=start_date,
            date_end=end_date,
            with_clerk=True,
        )
        print("✅ SUCCESS")
    except Exception as e:
        print(f"❌ FAILED: {e}")
        
        # Try removing one metric at a time
        print("\n🔍 Testing all market metrics minus one at a time...")
        for i, metric in enumerate(all_market_metrics):
            test_metrics = all_market_metrics[:i] + all_market_metrics[i+1:]
            try:
                print(f"  Testing without {metric.value}...", end=" ")
                response = await client.get_market(
                    network_code="NEM",
                    metrics=test_metrics,
                    interval="5m",
                    date_start=start_date,
                    date_end=end_date,
                    with_clerk=True,
                )
                print("✅ SUCCESS - This metric was the problem!")
                print(f"  ❌ Problematic metric: {metric.value}")
                break
            except Exception as e:
                print(f"❌ Still fails: {e}")
    
    print("\n🔍 Testing different intervals...")
    
    # Test different intervals with working metrics
    intervals = ["5m", "1h", "1d"]
    working_metrics = [MarketMetric.PRICE]  # Start with a metric that likely works
    
    for interval in intervals:
        try:
            print(f"  Testing {interval} interval...", end=" ")
            response = await client.get_market(
                network_code="NEM",
                metrics=working_metrics,
                interval=interval,
                date_start=start_date,
                date_end=end_date,
                with_clerk=True,
            )
            print("✅ SUCCESS")
        except Exception as e:
            print(f"❌ FAILED: {e}")
    
    await client.close()


if __name__ == "__main__":
    asyncio.run(test_market_metric_combinations()) 