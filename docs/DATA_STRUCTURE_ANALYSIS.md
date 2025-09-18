# OpenElectricity API Data Structure Analysis

## Overview

The OpenElectricity API returns data in a hierarchical structure that changes based on the metrics requested and grouping options. This document explains how the data shape varies and how it's processed by the PySpark streaming reader.

## Core Data Structure

### TimeSeriesResponse
The top-level response contains:
```python
{
    "success": bool,
    "version": str,
    "created_at": str,
    "data": [NetworkTimeSeries]  # Array of time series
}
```

### NetworkTimeSeries
Each time series represents one metric:
```python
{
    "network_code": "NEM",           # Network identifier
    "metric": "power",               # The metric type
    "unit": "MW",                    # Unit of measurement
    "interval": "5m",               # Data interval
    "start": "2025-09-18T21:00:00Z", # Optional start time
    "end": "2025-09-18T22:00:00Z",   # Optional end time
    "groupings": ["network_region"], # Applied groupings
    "network_timezone_offset": "+10:00",
    "results": [TimeSeriesResult]    # Array of grouped results
}
```

### TimeSeriesResult
Each result represents a grouping (e.g., region, fueltech):
```python
{
    "name": "power_NSW1",                    # Descriptive name
    "date_start": "2025-09-18T21:00:00Z",   # Result start time
    "date_end": "2025-09-18T22:00:00Z",     # Result end time
    "columns": {                            # Grouping metadata
        "network_region": "NSW1"
    },
    "data": [                               # Time series data points
        ["2025-09-18T21:00:00+10:00", 1234.5],
        ["2025-09-18T21:05:00+10:00", 1245.2],
        ...
    ]
}
```

## How Data Shape Changes

### 1. Single Metric, No Grouping

**Request:**
```python
client.get_network_data(
    network_code="NEM",
    metrics=[DataMetric.POWER],
    interval="5m"
)
```

**Structure:**
- 1 `NetworkTimeSeries` (for power)
- 1 `TimeSeriesResult` (network-wide total)
- Data points: `[timestamp, value]`

**Example:**
```json
{
  "data": [{
    "network_code": "NEM",
    "metric": "power",
    "unit": "MW",
    "interval": "5m",
    "groupings": [],
    "results": [{
      "name": "power",
      "columns": {},
      "data": [
        ["2025-09-18T21:00:00+10:00", 25000.5],
        ["2025-09-18T21:05:00+10:00", 25100.2]
      ]
    }]
  }]
}
```

### 2. Single Metric, Primary Grouping (Network Region)

**Request:**
```python
client.get_network_data(
    network_code="NEM",
    metrics=[DataMetric.POWER],
    interval="5m",
    primary_grouping="network_region"
)
```

**Structure:**
- 1 `NetworkTimeSeries` (for power)
- Multiple `TimeSeriesResult` (one per region)
- Data points: `[timestamp, value]` per region

**Example:**
```json
{
  "data": [{
    "network_code": "NEM",
    "metric": "power",
    "unit": "MW",
    "interval": "5m",
    "groupings": ["network_region"],
    "results": [
      {
        "name": "power_NSW1",
        "columns": {"network_region": "NSW1"},
        "data": [["2025-09-18T21:00:00+10:00", 8000.5]]
      },
      {
        "name": "power_QLD1", 
        "columns": {"network_region": "QLD1"},
        "data": [["2025-09-18T21:00:00+10:00", 6000.2]]
      },
      {
        "name": "power_VIC1",
        "columns": {"network_region": "VIC1"},
        "data": [["2025-09-18T21:00:00+10:00", 5000.8]]
      }
    ]
  }]
}
```

### 3. Single Metric, Primary + Secondary Grouping

**Request:**
```python
client.get_network_data(
    network_code="NEM",
    metrics=[DataMetric.POWER],
    interval="5m",
    primary_grouping="network_region",
    secondary_grouping="fueltech_group"
)
```

**Structure:**
- 1 `NetworkTimeSeries` (for power)
- Multiple `TimeSeriesResult` (one per region-fueltech combination)
- Data points: `[timestamp, value]` per combination

**Example:**
```json
{
  "data": [{
    "network_code": "NEM",
    "metric": "power",
    "unit": "MW",
    "interval": "5m",
    "groupings": ["network_region", "fueltech_group"],
    "results": [
      {
        "name": "power_NSW1_coal",
        "columns": {
          "network_region": "NSW1",
          "fueltech_group": "coal"
        },
        "data": [["2025-09-18T21:00:00+10:00", 3000.5]]
      },
      {
        "name": "power_NSW1_solar",
        "columns": {
          "network_region": "NSW1", 
          "fueltech_group": "solar"
        },
        "data": [["2025-09-18T21:00:00+10:00", 2000.0]]
      },
      {
        "name": "power_QLD1_coal",
        "columns": {
          "network_region": "QLD1",
          "fueltech_group": "coal"
        },
        "data": [["2025-09-18T21:00:00+10:00", 4000.2]]
      }
    ]
  }]
}
```

### 4. Multiple Metrics, No Grouping

**Request:**
```python
client.get_network_data(
    network_code="NEM",
    metrics=[DataMetric.POWER, DataMetric.ENERGY],
    interval="5m"
)
```

**Structure:**
- 2 `NetworkTimeSeries` (one for power, one for energy)
- 1 `TimeSeriesResult` per metric
- Data points: `[timestamp, value]` per metric

**Example:**
```json
{
  "data": [
    {
      "network_code": "NEM",
      "metric": "power",
      "unit": "MW",
      "interval": "5m",
      "groupings": [],
      "results": [{
        "name": "power",
        "columns": {},
        "data": [["2025-09-18T21:00:00+10:00", 25000.5]]
      }]
    },
    {
      "network_code": "NEM", 
      "metric": "energy",
      "unit": "MWh",
      "interval": "5m",
      "groupings": [],
      "results": [{
        "name": "energy",
        "columns": {},
        "data": [["2025-09-18T21:00:00+10:00", 2083.4]]
      }]
    }
  ]
}
```

### 5. Multiple Metrics, With Grouping

**Request:**
```python
client.get_network_data(
    network_code="NEM",
    metrics=[DataMetric.POWER, DataMetric.ENERGY],
    interval="5m",
    primary_grouping="network_region"
)
```

**Structure:**
- 2 `NetworkTimeSeries` (one for power, one for energy)
- Multiple `TimeSeriesResult` per metric (one per region)
- Data points: `[timestamp, value]` per metric-region combination

**Example:**
```json
{
  "data": [
    {
      "network_code": "NEM",
      "metric": "power",
      "unit": "MW", 
      "interval": "5m",
      "groupings": ["network_region"],
      "results": [
        {
          "name": "power_NSW1",
          "columns": {"network_region": "NSW1"},
          "data": [["2025-09-18T21:00:00+10:00", 8000.5]]
        },
        {
          "name": "power_QLD1",
          "columns": {"network_region": "QLD1"},
          "data": [["2025-09-18T21:00:00+10:00", 6000.2]]
        }
      ]
    },
    {
      "network_code": "NEM",
      "metric": "energy", 
      "unit": "MWh",
      "interval": "5m",
      "groupings": ["network_region"],
      "results": [
        {
          "name": "energy_NSW1",
          "columns": {"network_region": "NSW1"},
          "data": [["2025-09-18T21:00:00+10:00", 666.7]]
        },
        {
          "name": "energy_QLD1",
          "columns": {"network_region": "QLD1"},
          "data": [["2025-09-18T21:00:00+10:00", 500.0]]
        }
      ]
    }
  ]
}
```

## Data Transformation to Records

The `to_records()` method flattens this hierarchical structure into a tabular format:

### Input (Hierarchical)
```json
{
  "data": [{
    "network_code": "NEM",
    "metric": "power",
    "unit": "MW",
    "groupings": ["network_region"],
    "results": [
      {
        "name": "power_NSW1",
        "columns": {"network_region": "NSW1"},
        "data": [["2025-09-18T21:00:00+10:00", 8000.5]]
      },
      {
        "name": "power_QLD1", 
        "columns": {"network_region": "QLD1"},
        "data": [["2025-09-18T21:00:00+10:00", 6000.2]]
      }
    ]
  }]
}
```

### Output (Tabular Records)
```python
[
  {
    "interval": "2025-09-18T21:00:00+10:00",
    "network_region": "NSW1",
    "power": 8000.5
  },
  {
    "interval": "2025-09-18T21:00:00+10:00", 
    "network_region": "QLD1",
    "power": 6000.2
  }
]
```

## PySpark Schema Impact

The PySpark streaming reader uses a fixed schema with 22 fields to accommodate all possible metrics and groupings:

```python
schema = StructType([
    StructField("interval", TimestampType(), True),
    StructField("network_region", StringType(), True),
    StructField("fueltech_group", StringType(), True),
    StructField("fueltech", StringType(), True),
    StructField("status", StringType(), True),
    StructField("renewable", StringType(), True),
    StructField("power", DoubleType(), True),
    StructField("energy", DoubleType(), True),
    StructField("emissions", DoubleType(), True),
    StructField("market_value", DoubleType(), True),
    StructField("renewable_proportion", DoubleType(), True),
    StructField("storage_battery", DoubleType(), True),
    # ... additional fields
])
```

### Key Points:

1. **Metrics become columns**: Each requested metric becomes a column in the final DataFrame
2. **Groupings become columns**: Each grouping dimension becomes a column
3. **Sparse data**: Not all columns will have values for every row
4. **Null handling**: Missing values are represented as `None`/`null`
5. **Wide format**: The schema is designed to be "wide" to accommodate all possible combinations

## Performance Considerations

### Data Volume Impact

- **No grouping**: Minimal data (1 result per metric)
- **Primary grouping**: Scales with number of regions (~5-8 for NEM)
- **Secondary grouping**: Scales multiplicatively (regions × fueltechs)
- **Multiple metrics**: Linear scaling with number of metrics

### Memory Usage

- Each grouping level multiplies the data volume
- Multiple metrics create additional columns but don't multiply rows
- The PySpark schema is fixed-size regardless of actual data

### Network Considerations

- Larger groupings = more API calls or larger responses
- Consider batching for high-volume scenarios
- Use appropriate intervals to balance granularity vs. volume

## Best Practices

1. **Start simple**: Begin with single metrics and no grouping
2. **Add groupings gradually**: Test performance impact of each grouping level
3. **Monitor data volume**: Watch for exponential growth with multiple groupings
4. **Use appropriate intervals**: Balance data granularity with processing requirements
5. **Handle nulls**: Design downstream processing to handle sparse data gracefully
