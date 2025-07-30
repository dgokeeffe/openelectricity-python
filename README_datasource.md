# OpenElectricity PySpark Data Source

This repository contains a custom PySpark data source for the OpenElectricity API, allowing you to read electricity and energy data directly into Spark DataFrames.

## Files Created

### 1. `openelectricity_source.py`
The main data source implementation based on PySpark's `DataSource` and `DataSourceReader` classes, **built using the actual OpenElectricity SDK models**.

**Features:**
- ✅ **SDK-accurate schema** based on actual `TimeSeriesResponse`, `NetworkTimeSeries`, `TimeSeriesResult`, and `FacilityResponse` models
- ✅ **Two data sources**: `openelectricity` for time series data, `openelectricity_facilities` for facility metadata
- ✅ Proper flattening of nested SDK structures (APIResponse → NetworkTimeSeries → TimeSeriesResult → TimeSeriesDataPoint)
- ✅ Batch data reading (not streaming)
- ✅ Automatic authentication using Bearer tokens
- ✅ Comprehensive error handling with specific 403 auth error messages
- ✅ Support for both network and facility data endpoints

### 2. `openelectricity_example.py` (Updated)
Updated examples showing how to use the data source with your actual API key.

### 3. `test_openelectricity_api.py`
Diagnostic script to troubleshoot authentication issues.

## Quick Start

### 1. Register the Data Sources

```python
from pyspark.sql import SparkSession
from openelectricity_source import OpenElectricityDataSource, OpenElectricityFacilityDataSource

# Create Spark session
spark = SparkSession.builder \
    .appName("OpenElectricityExample") \
    .master("local[*]") \
    .getOrCreate()

# Register both data sources
spark.dataSource.register(OpenElectricityDataSource)  # For time series data
spark.dataSource.register(OpenElectricityFacilityDataSource)  # For facility metadata
```

### 2. Time Series Data Usage

```python
# Define options for time series data
options = {
    "access_token": "oe_3ZmKbwX5xoakFVmbVy3AgaWL",
    "network_code": "NEM",
    "metrics": "energy,power",
    "interval": "1h",
    "date_start": "2024-01-01T00:00:00Z",
    "date_end": "2024-01-02T00:00:00Z",
    "secondary_grouping": "fueltech_group"
}

# Create DataFrame for time series data
df = spark.read.format("openelectricity").options(**options).load()
df.show()
```

### 3. Facility Metadata Usage

```python
# Define options for facility metadata
options = {
    "access_token": "oe_3ZmKbwX5xoakFVmbVy3AgaWL",
    "network_id": "NEM",
    "status_id": "operating",
    "fueltech_id": "solar_utility,wind"
}

# Create DataFrame for facility metadata
df = spark.read.format("openelectricity_facilities").options(**options).load()
df.show()
```

## API Options

### Time Series Data (`openelectricity`)

#### Required Parameters
- **`access_token`**: Your OpenElectricity API key (format: `oe_...`)
- **`date_start`**: Start time in ISO format (e.g., `"2024-01-01T00:00:00Z"`)
- **`date_end`**: End time in ISO format

#### Optional Parameters
- **`network_code`**: Network code (default: `"NEM"`)
  - Options: `"NEM"`, `"WEM"`, `"AU"`
- **`facility_code`**: Specific facility code (optional)
  - If provided, queries facility endpoint instead of network endpoint
- **`metrics`**: Comma-separated metrics (default: `"energy"`)
  - Options: `"energy"`, `"power"`, `"price"`, `"emissions"`
- **`interval`**: Time interval (default: `"1h"`)
  - Options: `"5m"`, `"1h"`, `"1d"`, `"7d"`, `"1M"`, `"3M"`, `"season"`, `"1y"`, `"fy"`
- **`primary_grouping`**: Primary grouping
  - Options: `"network"`, `"network_region"`
- **`secondary_grouping`**: Secondary grouping
  - Options: `"fueltech"`, `"fueltech_group"`, `"status"`, `"renewable"`

### Facility Metadata (`openelectricity_facilities`)

#### Required Parameters
- **`access_token`**: Your OpenElectricity API key (format: `oe_...`)

#### Optional Parameters (all are filters)
- **`facility_code`**: Comma-separated facility codes
- **`status_id`**: Comma-separated status IDs
  - Options: `"committed"`, `"operating"`, `"retired"`
- **`fueltech_id`**: Comma-separated fuel technology IDs
  - Options: `"battery"`, `"coal_black"`, `"coal_brown"`, `"gas_ccgt"`, `"hydro"`, `"solar_utility"`, `"wind"`, etc.
- **`network_id`**: Comma-separated network IDs
  - Options: `"NEM"`, `"WEM"`, `"AU"`
- **`network_region`**: Specific network region

## Data Schemas

### Time Series Data Schema (`openelectricity`)
Based on flattened `TimeSeriesResponse` → `NetworkTimeSeries` → `TimeSeriesResult` → `TimeSeriesDataPoint`:

```
root
 |-- version: string (nullable = true)                    # APIResponse
 |-- created_at: timestamp (nullable = true)              # APIResponse  
 |-- success: boolean (nullable = true)                   # APIResponse
 |-- error: string (nullable = true)                      # APIResponse
 |-- total_records: integer (nullable = true)             # APIResponse
 |-- network_code: string (nullable = true)               # NetworkTimeSeries
 |-- metric: string (nullable = true)                     # NetworkTimeSeries
 |-- unit: string (nullable = true)                       # NetworkTimeSeries
 |-- interval: string (nullable = true)                   # NetworkTimeSeries
 |-- series_start: timestamp (nullable = true)            # NetworkTimeSeries
 |-- series_end: timestamp (nullable = true)              # NetworkTimeSeries
 |-- groupings: array<string> (nullable = true)           # NetworkTimeSeries
 |-- network_timezone_offset: string (nullable = true)    # NetworkTimeSeries
 |-- result_name: string (nullable = true)                # TimeSeriesResult
 |-- result_date_start: timestamp (nullable = true)       # TimeSeriesResult
 |-- result_date_end: timestamp (nullable = true)         # TimeSeriesResult
 |-- unit_code: string (nullable = true)                  # TimeSeriesColumns
 |-- fueltech_group: string (nullable = true)             # TimeSeriesColumns
 |-- network_region: string (nullable = true)             # TimeSeriesColumns
 |-- timestamp: timestamp (nullable = true)               # TimeSeriesDataPoint
 |-- value: double (nullable = true)                      # TimeSeriesDataPoint
```

### Facility Metadata Schema (`openelectricity_facilities`)
Based on flattened `FacilityResponse` → `Facility` → `FacilityUnit`:

```
root
 |-- version: string (nullable = true)                    # APIResponse
 |-- created_at: timestamp (nullable = true)              # APIResponse
 |-- success: boolean (nullable = true)                   # APIResponse
 |-- error: string (nullable = true)                      # APIResponse
 |-- total_records: integer (nullable = true)             # APIResponse
 |-- facility_code: string (nullable = true)              # Facility
 |-- facility_name: string (nullable = true)              # Facility
 |-- network_id: string (nullable = true)                 # Facility
 |-- network_region: string (nullable = true)             # Facility
 |-- facility_description: string (nullable = true)       # Facility
 |-- unit_code: string (nullable = true)                  # FacilityUnit
 |-- fueltech_id: string (nullable = true)                # FacilityUnit
 |-- status_id: string (nullable = true)                  # FacilityUnit
 |-- capacity_registered: double (nullable = true)        # FacilityUnit
 |-- emissions_factor_co2: double (nullable = true)       # FacilityUnit
 |-- data_first_seen: timestamp (nullable = true)         # FacilityUnit
 |-- data_last_seen: timestamp (nullable = true)          # FacilityUnit
 |-- dispatch_type: string (nullable = true)              # FacilityUnit
```

## Example Use Cases

### 1. Network Energy Data with Fuel Technology Grouping

```python
options = {
    "access_token": "oe_3ZmKbwX5xoakFVmbVy3AgaWL",
    "network_code": "NEM",
    "metrics": "energy",
    "interval": "1d",
    "date_start": "2024-01-01T00:00:00Z",
    "date_end": "2024-01-31T00:00:00Z",
    "secondary_grouping": "fueltech_group"
}

df = spark.read.format("openelectricity").options(**options).load()
df.groupBy("fueltech_group").sum("value").show()
```

### 2. Facility Metadata Analysis

```python
options = {
    "access_token": "oe_3ZmKbwX5xoakFVmbVy3AgaWL",
    "network_id": "NEM",
    "status_id": "operating",
    "fueltech_id": "solar_utility,wind"
}

df = spark.read.format("openelectricity_facilities").options(**options).load()

# Aggregate capacity by fuel technology
df.groupBy("fueltech_id") \
  .agg({"capacity_registered": "sum"}) \
  .show()
```

### 3. Specific Facility Time Series Data

```python
options = {
    "access_token": "oe_3ZmKbwX5xoakFVmbVy3AgaWL",
    "network_code": "NEM",
    "facility_code": "AGLHAL",
    "metrics": "power",
    "interval": "1h",
    "date_start": "2024-01-01T00:00:00Z",
    "date_end": "2024-01-02T00:00:00Z"
}

df = spark.read.format("openelectricity").options(**options).load()
df.select("timestamp", "value", "unit", "result_name").show()
```

### 3. Multiple Metrics Analysis

```python
options = {
    "access_token": "oe_3ZmKbwX5xoakFVmbVy3AgaWL",
    "network_code": "NEM",
    "metrics": "energy,power,price",
    "interval": "1h",
    "date_start": "2024-01-01T00:00:00Z",
    "date_end": "2024-01-01T12:00:00Z",
    "primary_grouping": "network_region"
}

df = spark.read.format("openelectricity").options(**options).load()

# Analyze by metric and region
df.groupBy("metric", "network_region") \
  .agg({"value": "avg", "value": "max", "value": "min"}) \
  .show()
```

## Troubleshooting

### 403 Forbidden Errors

If you're getting 403 errors, run the diagnostic script:

```bash
python test_openelectricity_api.py
```

This will test:
- Direct API calls with your credentials
- Different API key formats
- SDK functionality
- Common authentication issues

### Common Issues

1. **Invalid API Key**: Ensure your key starts with `oe_` and is active
2. **Missing Date Parameters**: Both `date_start` and `date_end` are required
3. **Rate Limiting**: Check if you're exceeding API rate limits
4. **Permissions**: Verify your API key has access to the requested data

## Dependencies

### Recommended Installation (uv)
```bash
# Install uv package manager (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install OpenElectricity with all dependencies
uv add openelectricity

# Or install with analysis features
uv add "openelectricity[analysis]"
```

### Alternative Installation (pip)
```bash
# Ensure packaging is up to date first
pip install --upgrade "packaging>=21.3"

# Install OpenElectricity
pip install openelectricity

# Additional dependencies for PySpark data sources
pip install "pyspark>=4.0.0" "pyarrow>=15.0.0" requests
```

### Troubleshooting
If you encounter installation issues (especially `ModuleNotFoundError: No module named 'packaging.licenses'`), see the [TROUBLESHOOTING.md](TROUBLESHOOTING.md) guide.

## Based on Template

This data source was created using the `predicthq_source_simple.py` as a template, adapted for the OpenElectricity API structure.

## API Reference

For complete API documentation, visit:
- [OpenElectricity API Docs](https://docs.openelectricity.org.au/api-reference/data/get-network-data)
- [Platform](https://platform.openelectricity.org.au) 