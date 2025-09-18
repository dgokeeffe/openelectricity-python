"""
PySpark Custom Data Source for OpenElectricity API Streaming

This module provides a PySpark streaming data source that can read data from the OpenElectricity API
with progress tracking capabilities, enabling exactly-once processing guarantees.

Based on: https://community.databricks.com/t5/technical-blog/enhancing-the-new-pyspark-custom-data-sources-streaming-api/ba-p/75538
"""

import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, Iterator, List, Optional, Tuple

from pyspark.sql.datasource import DataSource, DataSourceReader, DataSourceStreamReader
from pyspark.sql.types import (
    DataType,
    StructField,
    StructType,
    StringType,
    DoubleType,
    TimestampType,
    IntegerType,
)

from openelectricity.client import OEClient
from openelectricity.logging import get_logger
from openelectricity.types import DataInterval, DataMetric, Network
from openelectricity.models.timeseries import TimeSeriesResponse

logger = get_logger("pyspark_datasource")


class OpenElectricityStreamDataSource(DataSource):
    """
    PySpark Custom Data Source for streaming OpenElectricity API data.
    
    This data source enables streaming reads from the OpenElectricity API with progress tracking,
    allowing for exactly-once processing guarantees when used with Structured Streaming.
    """
    
    def __init__(self, options: Dict[str, Any]):
        """Initialize the data source with options."""
        super().__init__(options)
    
    @classmethod
    def name(cls) -> str:
        """Returns the name of this custom data source."""
        return "openelectricity_stream"
    
    def schema(self) -> StructType:
        """
        Returns the schema of the data returned by this data source.
        
        The schema includes all possible fields that can be returned from the OpenElectricity API.
        """
        return StructType([
            StructField("interval", TimestampType(), True),
            StructField("network_code", StringType(), True),
            StructField("metric", StringType(), True),
            StructField("unit", StringType(), True),
            StructField("fueltech_group", StringType(), True),
            StructField("network_region", StringType(), True),
            StructField("unit_code", StringType(), True),
            StructField("power", DoubleType(), True),
            StructField("energy", DoubleType(), True),
            StructField("emissions", DoubleType(), True),
            StructField("market_value", DoubleType(), True),
            StructField("renewable_proportion", DoubleType(), True),
            StructField("storage_battery", DoubleType(), True),
            StructField("price", DoubleType(), True),
            StructField("demand", DoubleType(), True),
            StructField("demand_energy", DoubleType(), True),
            StructField("curtailment", DoubleType(), True),
            StructField("curtailment_energy", DoubleType(), True),
            StructField("curtailment_solar_utility", DoubleType(), True),
            StructField("curtailment_solar_utility_energy", DoubleType(), True),
            StructField("curtailment_wind", DoubleType(), True),
            StructField("curtailment_wind_energy", DoubleType(), True),
        ])
    
    def streamReader(self, schema: StructType, options: Dict[str, Any]) -> "OpenElectricityStreamReader":
        """Returns an instance of the stream reader."""
        return OpenElectricityStreamReader(schema, options)


class OpenElectricityStreamReader(DataSourceStreamReader):
    """
    Stream reader for OpenElectricity API data with progress tracking.
    
    This reader implements checkpointing-like capabilities to achieve progress tracking
    and exactly-once guarantees. When you pause and resume streaming jobs, it will resume
    reading from the last point onwards instead of reading from the beginning.
    """
    
    def __init__(self, schema: StructType, options: Dict[str, Any]):
        """
        Initialize the stream reader.
        
        Args:
            schema: The schema of the data to be read
            options: Configuration options including:
                - api_key: OpenElectricity API key
                - base_url: Base URL for the API (optional)
                - network_code: Network code (NEM, WEM, AU)
                - metrics: List of metrics to fetch
                - interval: Data interval (5m, 1h, 1d, etc.)
                - progress_path: Path to store progress tracking file
                - batch_size_hours: Hours of data to fetch per batch (default: 1)
                - primary_grouping: Primary grouping for data (optional)
                - secondary_grouping: Secondary grouping for data (optional)
                - start_time: Custom start time (ISO format string or datetime)
                - start_hours_ago: Start from N hours ago (overrides start_time)
                - reset_progress: Reset progress tracking (default: False)
        """
        self.schema = schema
        self.options = options
        
        # Initialize OpenElectricity client
        self.client = OEClient(
            api_key=options.get("api_key"),
            base_url=options.get("base_url")
        )
        
        # Configuration
        network_code_str = options.get("network_code", "NEM")
        self.network_code = Network(network_code_str)  # Convert to Network enum for validation
        self.network_code_str = network_code_str  # Keep string version for API calls
        self.metrics = [DataMetric(m) for m in options.get("metrics", ["power"])]  # Convert to DataMetric enums
        self.metrics_str = options.get("metrics", ["power"])  # Keep string version for API calls
        self.interval = options.get("interval", "5m")  # Keep as string since DataInterval is Literal
        self.batch_size_hours = int(options.get("batch_size_hours", 1))
        self.primary_grouping = options.get("primary_grouping")
        self.secondary_grouping = options.get("secondary_grouping")
        
        # Progress tracking
        self.progress_path = options.get("progress_path", "/tmp/openelectricity_progress/")
        self.progress_file = os.path.join(self.progress_path, "progress.json")
        
        # Ensure progress directory exists
        os.makedirs(self.progress_path, exist_ok=True)
        
        # Load progress
        self._load_progress()
        
        logger.info(f"Initialized OpenElectricityStreamReader for {self.network_code} with metrics {self.metrics}")
        
    def initialOffset(self) -> Dict[str, Any]:
        """
        Returns the initial start offset of the reader.
        
        Returns:
            Dictionary containing the initial timestamp offset
        """
        return {"timestamp": self.current_timestamp.isoformat()}
    
    def _load_progress(self) -> None:
        """Load progress from the progress file."""
        # Check if we should reset progress
        reset_progress = self.options.get("reset_progress", False)
        if reset_progress and os.path.exists(self.progress_file):
            logger.info("Resetting progress tracking")
            os.remove(self.progress_file)
        
        # Determine initial timestamp
        initial_timestamp = self._get_initial_timestamp()
        
        try:
            if os.path.exists(self.progress_file):
                with open(self.progress_file, 'r') as f:
                    progress_data = json.load(f)
                    self.current_timestamp = datetime.fromisoformat(progress_data.get('timestamp', 
                        initial_timestamp.isoformat()))
            else:
                # Use the determined initial timestamp
                self.current_timestamp = initial_timestamp
                
            logger.info(f"Loaded progress: current timestamp = {self.current_timestamp}")
        except Exception as e:
            logger.warning(f"Failed to load progress, using initial timestamp: {e}")
            self.current_timestamp = initial_timestamp
    
    def _get_initial_timestamp(self) -> datetime:
        """Determine the initial timestamp based on options."""
        # Check for start_hours_ago (takes precedence)
        start_hours_ago = self.options.get("start_hours_ago")
        if start_hours_ago is not None:
            hours = float(start_hours_ago)
            return datetime.now() - timedelta(hours=hours)
        
        # Check for start_time
        start_time = self.options.get("start_time")
        if start_time is not None:
            if isinstance(start_time, str):
                return datetime.fromisoformat(start_time)
            elif isinstance(start_time, datetime):
                return start_time
            else:
                logger.warning(f"Invalid start_time format: {start_time}, using default")
        
        # Default: start from 1 hour ago
        return datetime.now() - timedelta(hours=1)
    
    def _save_progress(self) -> None:
        """Save progress to the progress file."""
        try:
            progress_data = {"timestamp": self.current_timestamp.isoformat()}
            with open(self.progress_file, 'w') as f:
                json.dump(progress_data, f)
            logger.debug(f"Saved progress: timestamp = {self.current_timestamp}")
        except Exception as e:
            logger.error(f"Failed to save progress: {e}")
    
    def latestOffset(self) -> Dict[str, Any]:
        """
        Returns the current latest offset that the next microbatch will read to.
        
        Returns:
            Dictionary containing the end timestamp for the current batch
        """
        # Calculate end timestamp for this batch
        end_timestamp = self.current_timestamp + timedelta(hours=self.batch_size_hours)
        return {"timestamp": end_timestamp.isoformat()}
    
    def partitions(self, start: Dict[str, Any], end: Dict[str, Any]) -> List["RangePartition"]:
        """
        Plans the partitioning of the current microbatch defined by start and end offset.
        
        Args:
            start: Start offset dictionary
            end: End offset dictionary
            
        Returns:
            List of InputPartition objects
        """
        return [RangePartition(start["timestamp"], end["timestamp"])]
    
    def commit(self, end: Dict[str, Any]) -> None:
        """
        This is invoked when the query has finished processing data before end offset.
        
        Args:
            end: End offset dictionary
        """
        # Update current timestamp to the end of the processed batch
        self.current_timestamp = datetime.fromisoformat(end["timestamp"])
        self._save_progress()
        logger.info(f"Committed batch, updated timestamp to: {self.current_timestamp}")
    
    def read(self, partition: "RangePartition") -> Iterator[Tuple]:
        """
        Takes a partition as input and reads an iterator of tuples from the data source.
        
        Args:
            partition: The partition to read from
            
        Yields:
            Tuples representing rows of data
        """
        start_timestamp = datetime.fromisoformat(partition.start)
        end_timestamp = datetime.fromisoformat(partition.end)
        
        logger.info(f"Reading data from {start_timestamp} to {end_timestamp}")
        
        try:
            # Fetch data from OpenElectricity API
            response: TimeSeriesResponse = self.client.get_network_data(
                network_code=self.network_code_str,  # NetworkCode is Literal, so pass string
                metrics=self.metrics,  # DataMetric enums are expected
                interval=self.interval,  # DataInterval is Literal, so pass string
                date_start=start_timestamp,
                date_end=end_timestamp,
                primary_grouping=self.primary_grouping,
                secondary_grouping=self.secondary_grouping,
            )
            
            # Convert to records and yield tuples
            records = response.to_records()
            logger.info(f"Retrieved {len(records)} records from API")
            
            for record in records:
                # Convert record to tuple matching the schema
                yield self._record_to_tuple(record)
                
        except Exception as e:
            logger.error(f"Error reading data from API: {e}")
            # Yield empty tuple to maintain schema consistency
            yield self._empty_tuple()
    
    def _record_to_tuple(self, record: Dict[str, Any]) -> Tuple:
        """
        Convert a record dictionary to a tuple matching the schema.
        
        Args:
            record: Dictionary record from the API
            
        Returns:
            Tuple matching the schema order
        """
        # Create a tuple with all schema fields, filling in values from record
        return (
            record.get("interval"),
            self.network_code_str,  # Use string version instead of enum for PySpark serialization
            record.get("metric"),
            record.get("unit"),
            record.get("fueltech_group"),
            record.get("network_region"),
            record.get("unit_code"),
            record.get("power"),
            record.get("energy"),
            record.get("emissions"),
            record.get("market_value"),
            record.get("renewable_proportion"),
            record.get("storage_battery"),
            record.get("price"),
            record.get("demand"),
            record.get("demand_energy"),
            record.get("curtailment"),
            record.get("curtailment_energy"),
            record.get("curtailment_solar_utility"),
            record.get("curtailment_solar_utility_energy"),
            record.get("curtailment_wind"),
            record.get("curtailment_wind_energy"),
        )
    
    def _empty_tuple(self) -> Tuple:
        """Return an empty tuple with None values for all schema fields."""
        return tuple([None] * 22)  # Fixed number of fields in our schema


class RangePartition:
    """Simple partition class for timestamp ranges."""
    
    def __init__(self, start: str, end: str):
        self.start = start
        self.end = end