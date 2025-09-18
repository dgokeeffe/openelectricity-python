# PySpark Streaming with OpenElectricity API

This document describes how to use the OpenElectricity PySpark Custom Data Source for streaming data ingestion from the OpenElectricity API.

## Overview

The OpenElectricity PySpark streaming data source enables real-time data ingestion from the OpenElectricity API using PySpark Structured Streaming. It includes progress tracking capabilities that provide exactly-once processing guarantees, meaning that when you pause and resume streaming jobs, they will continue from where they left off instead of restarting from the beginning.

## Features

- **Real-time streaming**: Continuously ingest data from the OpenElectricity API
- **Progress tracking**: Automatic checkpointing to prevent data loss
- **Exactly-once processing**: Guarantees that data is processed exactly once
- **Configurable batching**: Control the amount of data fetched per batch
- **Multiple networks**: Support for NEM, WEM, and AU networks
- **Flexible metrics**: Choose from various data and market metrics
- **Error handling**: Robust error handling with logging

## Installation

Ensure you have the required dependencies:

```bash
uv add pyspark
uv add openelectricity[analysis]
```

## Basic Usage

### 1. Register the Data Source

```python
from pyspark.sql import SparkSession
from openelectricity.pyspark_datasource import OpenElectricityStreamDataSource

spark = SparkSession.builder.appName("OpenElectricityStreaming").getOrCreate()
spark.dataSource.register(OpenElectricityStreamDataSource)
```

### 2. Configure Streaming Options

```python
stream_options = {
    "api_key": "your_api_key_here",
    "network_code": "NEM",  # NEM, WEM, or AU
    "metrics": ["power", "energy", "emissions"],
    "interval": "5m",  # 5m, 1h, 1d, etc.
    "batch_size_hours": 1,  # Hours of data per batch
    "progress_path": "/tmp/openelectricity_progress/",
    "primary_grouping": "network_region",
    "secondary_grouping": "fueltech",
}
```

### 3. Create Streaming DataFrame

```python
streaming_df = spark.readStream \
    .format("openelectricity_stream") \
    .options(**stream_options) \
    .load()
```

### 4. Process and Write Data

```python
# Add processing timestamp
processed_df = streaming_df.withColumn("processing_timestamp", current_timestamp())

# Write to console (for testing)
query = processed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()
```

## Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `api_key` | string | Required | OpenElectricity API key |
| `base_url` | string | Production API | Base URL for the API |
| `network_code` | string | "NEM" | Network code (NEM, WEM, AU) |
| `metrics` | list | ["power"] | List of metrics to fetch |
| `interval` | string | "5m" | Data interval (5m, 1h, 1d, etc.) |
| `batch_size_hours` | int | 1 | Hours of data per batch |
| `progress_path` | string | "/tmp/openelectricity_progress/" | Path for progress tracking |
| `primary_grouping` | string | None | Primary grouping (network, network_region) |
| `secondary_grouping` | string | None | Secondary grouping (fueltech, fueltech_group, status, renewable) |

## Available Metrics

### Data Metrics
- `power`: Power generation/consumption
- `energy`: Energy generation/consumption
- `emissions`: Carbon emissions
- `market_value`: Market value of energy
- `renewable_proportion`: Proportion of renewable energy
- `storage_battery`: Battery storage data

### Market Metrics
- `price`: Electricity price
- `demand`: Electricity demand
- `demand_energy`: Energy demand
- `curtailment`: Energy curtailment
- `curtailment_energy`: Curtailed energy
- `curtailment_solar_utility`: Solar utility curtailment
- `curtailment_solar_utility_energy`: Solar utility curtailed energy
- `curtailment_wind`: Wind curtailment
- `curtailment_wind_energy`: Wind curtailed energy

## Data Schema

The streaming data source returns data with the following schema:

```python
StructType([
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
```

## Progress Tracking

The data source automatically tracks progress by storing timestamps in a JSON file. This enables:

- **Resume capability**: Jobs can be paused and resumed without data loss
- **Exactly-once processing**: Each data point is processed exactly once
- **Fault tolerance**: Automatic recovery from failures

Progress is stored in the `progress_path` directory as `progress.json`:

```json
{
    "timestamp": "2024-01-15T10:30:00"
}
```

## Production Examples

### Writing to Delta Table

```python
query = streaming_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/path/to/checkpoint") \
    .toTable("openelectricity.nem_data")
```

### Multiple Networks

```python
# Stream from multiple networks
nem_stream = spark.readStream \
    .format("openelectricity_stream") \
    .option("network_code", "NEM") \
    .option("api_key", api_key) \
    .load()

wem_stream = spark.readStream \
    .format("openelectricity_stream") \
    .option("network_code", "WEM") \
    .option("api_key", api_key) \
    .load()

combined_stream = nem_stream.union(wem_stream)
```

### Custom Processing

```python
# Add custom transformations
processed_df = streaming_df \
    .filter(col("power").isNotNull()) \
    .withColumn("power_mw", col("power") / 1000) \
    .withColumn("processing_timestamp", current_timestamp())

# Write to multiple sinks
query1 = processed_df.writeStream \
    .format("delta") \
    .toTable("openelectricity.processed_data")

query2 = processed_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "openelectricity") \
    .start()
```

## Error Handling

The data source includes comprehensive error handling:

- **API errors**: Automatic retry with exponential backoff
- **Network timeouts**: Configurable timeout settings
- **Data validation**: Graceful handling of malformed data
- **Progress tracking**: Safe progress file operations

## Performance Considerations

- **Batch size**: Larger batches reduce API calls but increase memory usage
- **Interval**: Shorter intervals provide more real-time data but increase processing overhead
- **Grouping**: Additional groupings increase data volume but provide more granular insights
- **Checkpointing**: Use fast storage for checkpoint locations

## Monitoring

Monitor your streaming jobs using:

- **Spark UI**: View job progress and performance metrics
- **Logs**: Check application logs for errors and warnings
- **Progress files**: Monitor progress tracking files for data continuity

## Troubleshooting

### Common Issues

1. **API Key Errors**: Ensure your API key is valid and has sufficient permissions
2. **Progress File Errors**: Check file permissions and disk space
3. **Memory Issues**: Reduce batch size or increase Spark memory allocation
4. **Network Timeouts**: Increase timeout settings for slow networks

### Debug Mode

Enable debug logging:

```python
import logging
logging.getLogger("openelectricity.pyspark_datasource").setLevel(logging.DEBUG)
```

## Best Practices

1. **Use appropriate batch sizes**: Balance between API efficiency and memory usage
2. **Monitor progress files**: Ensure they're not corrupted or deleted
3. **Handle failures gracefully**: Implement proper error handling and retry logic
4. **Use checkpointing**: Always use checkpoint locations for production jobs
5. **Monitor API limits**: Be aware of API rate limits and quotas

## Examples

See the `examples/pyspark_streaming_example.py` file for complete working examples including:

- Basic streaming setup
- Delta table integration
- Multi-network streaming
- Custom data processing

## Support

For issues and questions:

1. Check the logs for error messages
2. Verify API key and network connectivity
3. Review the progress tracking files
4. Consult the OpenElectricity API documentation
5. Open an issue in the project repository
