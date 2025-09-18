# OpenElectricity API Data Structure Diagram

## Hierarchical Structure

```
TimeSeriesResponse
├── success: bool
├── version: str
├── created_at: str
└── data: [NetworkTimeSeries]
    ├── NetworkTimeSeries #1 (e.g., power)
    │   ├── network_code: "NEM"
    │   ├── metric: "power"
    │   ├── unit: "MW"
    │   ├── interval: "5m"
    │   ├── groupings: ["network_region"]
    │   ├── network_timezone_offset: "+10:00"
    │   └── results: [TimeSeriesResult]
    │       ├── TimeSeriesResult #1 (NSW1)
    │       │   ├── name: "power_NSW1"
    │       │   ├── date_start: "2025-09-18T21:00:00Z"
    │       │   ├── date_end: "2025-09-18T22:00:00Z"
    │       │   ├── columns: {"network_region": "NSW1"}
    │       │   └── data: [TimeSeriesDataPoint]
    │       │       ├── ["2025-09-18T21:00:00+10:00", 8000.5]
    │       │       ├── ["2025-09-18T21:05:00+10:00", 8100.2]
    │       │       └── ...
    │       ├── TimeSeriesResult #2 (QLD1)
    │       │   ├── name: "power_QLD1"
    │       │   ├── columns: {"network_region": "QLD1"}
    │       │   └── data: [TimeSeriesDataPoint]
    │       └── ...
    └── NetworkTimeSeries #2 (e.g., energy)
        ├── metric: "energy"
        ├── unit: "MWh"
        └── results: [TimeSeriesResult]
            └── ...
```

## Data Shape Variations

### 1. Single Metric, No Grouping
```
TimeSeriesResponse
└── data: [NetworkTimeSeries]
    └── NetworkTimeSeries (power)
        └── results: [TimeSeriesResult]
            └── TimeSeriesResult (network total)
                └── data: [timestamp, value]
```

### 2. Single Metric, Primary Grouping
```
TimeSeriesResponse
└── data: [NetworkTimeSeries]
    └── NetworkTimeSeries (power)
        └── results: [TimeSeriesResult]
            ├── TimeSeriesResult (NSW1)
            ├── TimeSeriesResult (QLD1)
            ├── TimeSeriesResult (VIC1)
            └── ...
```

### 3. Single Metric, Primary + Secondary Grouping
```
TimeSeriesResponse
└── data: [NetworkTimeSeries]
    └── NetworkTimeSeries (power)
        └── results: [TimeSeriesResult]
            ├── TimeSeriesResult (NSW1_coal)
            ├── TimeSeriesResult (NSW1_solar)
            ├── TimeSeriesResult (QLD1_coal)
            └── ...
```

### 4. Multiple Metrics, No Grouping
```
TimeSeriesResponse
└── data: [NetworkTimeSeries]
    ├── NetworkTimeSeries (power)
    │   └── results: [TimeSeriesResult]
    │       └── TimeSeriesResult (network total)
    └── NetworkTimeSeries (energy)
        └── results: [TimeSeriesResult]
            └── TimeSeriesResult (network total)
```

### 5. Multiple Metrics, With Grouping
```
TimeSeriesResponse
└── data: [NetworkTimeSeries]
    ├── NetworkTimeSeries (power)
    │   └── results: [TimeSeriesResult]
    │       ├── TimeSeriesResult (NSW1)
    │       ├── TimeSeriesResult (QLD1)
    │       └── ...
    └── NetworkTimeSeries (energy)
        └── results: [TimeSeriesResult]
            ├── TimeSeriesResult (NSW1)
            ├── TimeSeriesResult (QLD1)
            └── ...
```

## Transformation to Tabular Format

### Input (Hierarchical)
```
NetworkTimeSeries (power)
└── results: [
    TimeSeriesResult (NSW1) → data: [["2025-09-18T21:00:00+10:00", 8000.5]]
    TimeSeriesResult (QLD1) → data: [["2025-09-18T21:00:00+10:00", 6000.2]]
]
```

### Output (Tabular Records)
```
[
  {"interval": "2025-09-18T21:00:00+10:00", "network_region": "NSW1", "power": 8000.5},
  {"interval": "2025-09-18T21:00:00+10:00", "network_region": "QLD1", "power": 6000.2}
]
```

## PySpark Schema Mapping

```
Fixed Schema (22 fields)
├── interval: TimestampType
├── network_region: StringType
├── fueltech_group: StringType
├── fueltech: StringType
├── status: StringType
├── renewable: StringType
├── power: DoubleType
├── energy: DoubleType
├── emissions: DoubleType
├── market_value: DoubleType
├── renewable_proportion: DoubleType
├── storage_battery: DoubleType
└── ... (additional fields)
```

## Data Volume Scaling

### No Grouping
- 1 NetworkTimeSeries per metric
- 1 TimeSeriesResult per metric
- Minimal data volume

### Primary Grouping (network_region)
- 1 NetworkTimeSeries per metric
- ~5-8 TimeSeriesResult per metric (NEM regions)
- 5-8x data volume increase

### Secondary Grouping (fueltech_group)
- 1 NetworkTimeSeries per metric
- ~5-8 regions × ~10 fueltech groups = ~50-80 TimeSeriesResult per metric
- 50-80x data volume increase

### Multiple Metrics
- N NetworkTimeSeries (one per metric)
- Linear scaling with number of metrics
- No multiplicative effect on rows, only columns
