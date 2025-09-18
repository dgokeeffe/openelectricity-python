"""
Backfill utilities for handling API limits in PySpark data source.

This module provides utilities to backfill large amounts of historical data
by automatically chunking requests to respect API limits (e.g., 5-day maximum).

The backfill utility uses the same OpenElectricityStreamReader but automatically
chunks large time ranges into smaller requests.
"""

import os
import tempfile
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession
from pyspark.sql.datasource import DataSource
from pyspark.sql.types import StructType

from openelectricity.logging import get_logger

logger = get_logger("backfill_utils")


def generate_time_chunks(start_time: datetime, end_time: datetime, chunk_days: int = 4) -> List[tuple]:
    """
    Generate time chunks for backfilling large time ranges.
    
    Args:
        start_time: Start time for backfill
        end_time: End time for backfill
        chunk_days: Days per chunk (default: 4, max: 5)
        
    Returns:
        List of (start_time, end_time) tuples for each chunk
    """
    chunk_days = min(chunk_days, 5)  # Respect 5-day API limit
    chunks = []
    current_start = start_time
    
    while current_start < end_time:
        # Calculate chunk end time
        chunk_end = current_start + timedelta(days=chunk_days)
        
        # Don't exceed the overall end time
        if chunk_end > end_time:
            chunk_end = end_time
        
        chunks.append((current_start, chunk_end))
        
        # Move to next chunk
        current_start = chunk_end
    
    logger.info(f"Generated {len(chunks)} time chunks for {start_time} to {end_time}")
    for i, (start, end) in enumerate(chunks):
        duration = (end - start).total_seconds() / 3600 / 24
        logger.info(f"  Chunk {i+1}: {start} to {end} ({duration:.1f} days)")
    
    return chunks


def create_backfill_dataframe(
    spark: SparkSession,
    start_time: datetime,
    end_time: datetime,
    api_key: str,
    network_code: str = "NEM",
    metrics: List[str] = None,
    interval: str = "5m",
    chunk_days: int = 4,
    primary_grouping: str = None,
    secondary_grouping: str = None,
    progress_path: str = None
) -> "DataFrame":
    """
    Create a DataFrame for backfilling historical data using the existing streaming reader.
    
    This function automatically chunks the time range and uses the OpenElectricityStreamReader
    for each chunk to respect API limits, then unions all chunks into a single DataFrame.
    
    Args:
        spark: SparkSession
        start_time: Start time for backfill
        end_time: End time for backfill
        api_key: OpenElectricity API key
        network_code: Network code (NEM, WEM, AU)
        metrics: List of metrics to fetch
        interval: Data interval (5m, 1h, 1d, etc.)
        chunk_days: Days per chunk (default: 4, max: 5)
        primary_grouping: Primary grouping for data
        secondary_grouping: Secondary grouping for data
        progress_path: Path to store progress tracking
        
    Returns:
        DataFrame with historical data from all chunks
    """
    if metrics is None:
        metrics = ["power"]
    
    if progress_path is None:
        progress_path = "/tmp/openelectricity_backfill/"
    
    # Generate time chunks
    time_chunks = generate_time_chunks(start_time, end_time, chunk_days)
    
    logger.info(f"Creating DataFrame with {len(time_chunks)} chunks")
    dataframes = []
    
    for i, (chunk_start, chunk_end) in enumerate(time_chunks):
        logger.info(f"Processing chunk {i+1}/{len(time_chunks)}: {chunk_start} to {chunk_end}")
        
        # Use the existing streaming data source for each chunk
        chunk_df = spark.read \
            .format("openelectricity_stream") \
            .option("api_key", api_key) \
            .option("network_code", network_code) \
            .option("metrics", ",".join(metrics)) \
            .option("interval", interval) \
            .option("start_time", chunk_start.isoformat()) \
            .option("end_time", chunk_end.isoformat()) \
            .option("batch_size_hours", int(chunk_days * 24)) \
            .option("primary_grouping", primary_grouping or "") \
            .option("secondary_grouping", secondary_grouping or "") \
            .option("progress_path", os.path.join(progress_path, f"chunk_{i}")) \
            .option("reset_progress", True) \
            .load()
        
        dataframes.append(chunk_df)
        logger.info(f"✓ Chunk {i+1} loaded")
    
    # Union all DataFrames
    logger.info("Unioning all chunks into single DataFrame")
    if len(dataframes) == 1:
        return dataframes[0]
    else:
        from functools import reduce
        return reduce(lambda df1, df2: df1.union(df2), dataframes)


def estimate_backfill_time(start_time: datetime, end_time: datetime, chunk_days: int = 4) -> Dict[str, Any]:
    """
    Estimate the time and resources needed for a backfill.
    
    Args:
        start_time: Start time for backfill
        end_time: End time for backfill
        chunk_days: Days per chunk
        
    Returns:
        Dictionary with estimates
    """
    total_days = (end_time - start_time).total_seconds() / 3600 / 24
    num_chunks = int(total_days / chunk_days) + (1 if total_days % chunk_days > 0 else 0)
    
    # Estimate API calls (assuming 1 call per chunk)
    estimated_api_calls = num_chunks
    
    # Estimate data volume (rough estimate based on 5-minute intervals)
    if chunk_days == 4:
        records_per_chunk = 4 * 24 * 12  # 4 days * 24 hours * 12 (5-min intervals)
    else:
        records_per_chunk = chunk_days * 24 * 12
    
    estimated_records = num_chunks * records_per_chunk
    
    return {
        "total_days": total_days,
        "num_chunks": num_chunks,
        "chunk_days": chunk_days,
        "estimated_api_calls": estimated_api_calls,
        "estimated_records": estimated_records,
        "start_time": start_time,
        "end_time": end_time
    }
