"""
OpenElectricity Spark Utilities

This module provides clean, reusable functions for Spark session management
following Databricks best practices. It ensures consistent Spark session handling
across the SDK whether running in Databricks or local environments.

Key Features:
- Automatic detection of Databricks vs local environment
- Consistent Spark session configuration
- Proper error handling and logging
- Easy to test and maintain
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


def get_spark_session(app_name: str = "OpenElectricity") -> "SparkSession":
    """
    Get a Spark session that works in both Databricks and local environments.
    
    This function automatically detects the environment and creates an appropriate
    Spark session. In Databricks, it uses DatabricksSession for better integration.
    In local environments, it creates a standard SparkSession.
    
    Args:
        app_name: Name for the Spark application (default: "OpenElectricity")
        
    Returns:
        SparkSession: Configured Spark session
        
    Raises:
        Exception: If unable to create Spark session
        
    Example:
        >>> spark = get_spark_session("MyApp")
        >>> df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
    """
    try:
        # Try Databricks first (production environment)
        from databricks.connect import DatabricksSession
        
        logger.debug("Using DatabricksSession for Databricks environment")
        return DatabricksSession.builder.appName(app_name).getOrCreate()
        
    except ImportError:
        try:
            # Fall back to standard PySpark (local development)
            from pyspark.sql import SparkSession
            
            logger.debug("Using standard SparkSession for local environment")
            return SparkSession.builder.appName(app_name).getOrCreate()
            
        except ImportError:
            logger.error("PySpark not available. Install with: uv add 'openelectricity[analysis]'")
            raise ImportError(
                "PySpark is required for DataFrame conversion. "
                "Install it with: uv add 'openelectricity[analysis]'"
            ) from None
        except Exception as e:
            logger.error(f"Failed to create standard SparkSession: {e}")
            raise
    except Exception as e:
        logger.error(f"Failed to create DatabricksSession: {e}")
        raise


def create_spark_dataframe(data, schema=None, spark_session=None, app_name: str = "OpenElectricity") -> "Optional[DataFrame]":
    """
    Create a PySpark DataFrame from data with automatic Spark session management.
    
    This function handles the creation of a Spark session if one is not provided,
    making it easy to convert data to PySpark DataFrames without managing sessions manually.
    
    Args:
        data: Data to convert (list of records, pandas DataFrame, etc.)
        schema: Optional schema for the DataFrame
        spark_session: Optional existing Spark session
        app_name: Name for the Spark application if creating a new session
        
    Returns:
        PySpark DataFrame or None if conversion fails
        
    Example:
        >>> records = [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}]
        >>> df = create_spark_dataframe(records)
        >>> print(f"Created DataFrame with {df.count()} rows")
    """
    try:
        from pyspark.sql import DataFrame
        
        # Use provided session or create new one
        if spark_session is None:
            spark_session = get_spark_session(app_name)
        
        # Create DataFrame
        if schema:
            return spark_session.createDataFrame(data, schema)
        else:
            return spark_session.createDataFrame(data)
            
    except ImportError:
        logger.warning("PySpark not available. Install with: uv add 'openelectricity[analysis]'")
        return None
    except Exception as e:
        logger.error(f"Error creating PySpark DataFrame: {e}")
        return None


def is_spark_available() -> bool:
    """
    Check if PySpark is available in the current environment.
    
    Returns:
        bool: True if PySpark can be imported, False otherwise
        
    Example:
        >>> if is_spark_available():
        ...     print("PySpark is ready to use")
        ... else:
        ...     print("PySpark not available")
    """
    try:
        import pyspark
        return True
    except ImportError:
        return False


def get_spark_version() -> Optional[str]:
    """
    Get the version of PySpark if available.
    
    Returns:
        str: PySpark version string or None if not available
        
    Example:
        >>> version = get_spark_version()
        >>> print(f"PySpark version: {version}")
    """
    try:
        import pyspark
        return pyspark.__version__
    except ImportError:
        return None


def create_spark_dataframe_with_schema(data, schema, spark_session=None, app_name: str = "OpenElectricity"):
    """
    Create a PySpark DataFrame with explicit schema for better performance.
    
    Args:
        data: List of dictionaries or similar data structure
        schema: PySpark schema (StructType)
        spark_session: Optional PySpark session. If not provided, will create one.
        app_name: Name for the Spark application if creating a new session.
        
    Returns:
        PySpark DataFrame with explicit schema
    """
    if spark_session is None:
        spark_session = get_spark_session(app_name)
    
    return spark_session.createDataFrame(data, schema=schema)


def pydantic_field_to_spark_type(field_info, field_name: str):
    """
    Map a Pydantic field to the appropriate Spark type.
    
    Args:
        field_info: Pydantic field info from model fields
        field_name: Name of the field
        
    Returns:
        Appropriate PySpark data type
    """
    from pyspark.sql.types import StringType, DoubleType, IntegerType, BooleanType, TimestampType
    from typing import get_origin, get_args
    import datetime
    from enum import Enum
    
    # Get the annotation (type) from the field
    annotation = field_info.annotation
    
    # Handle Union types (like str | None)
    origin = get_origin(annotation)
    if origin is type(None) or origin is type(type(None)):
        return StringType()
    elif hasattr(annotation, '__origin__') and annotation.__origin__ is type(None):
        return StringType()
    elif origin is not None:
        args = get_args(annotation)
        # For Union types, get the non-None type
        non_none_types = [arg for arg in args if arg is not type(None)]
        if non_none_types:
            annotation = non_none_types[0]
    
    # Map basic Python types
    if annotation == str:
        return StringType()
    elif annotation == int:
        return IntegerType()
    elif annotation == float:
        return DoubleType()
    elif annotation == bool:
        return BooleanType()
    elif annotation == datetime.datetime or annotation is datetime.datetime:
        return TimestampType()  # Store as timestamp with UTC conversion
    
    # Handle Enum types (including custom enums)
    if hasattr(annotation, '__bases__') and any(issubclass(base, Enum) for base in annotation.__bases__):
        return StringType()
    
    # Handle List types
    if origin == list:
        return StringType()  # Store lists as JSON strings for now
    
    # Default to string for unknown types
    return StringType()


def create_schema_from_pydantic_model(model_class, flattened: bool = False):
    """
    Create a Spark schema directly from a Pydantic model class.
    
    Args:
        model_class: Pydantic model class
        flattened: Whether this is for flattened data (like facilities with units)
        
    Returns:
        PySpark StructType schema
    """
    from pyspark.sql.types import StructType, StructField
    
    schema_fields = []
    
    # Get model fields
    for field_name, field_info in model_class.model_fields.items():
        spark_type = pydantic_field_to_spark_type(field_info, field_name)
        schema_fields.append(StructField(field_name, spark_type, True))  # Allow nulls
    
    return StructType(schema_fields)


def create_facilities_flattened_schema():
    """
    Create a Spark schema for flattened facilities data (facility + unit fields).
    
    Returns:
        PySpark StructType schema optimized for facilities with units
    """
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType
    
    # Define schema based on the flattened structure from facilities to_pyspark
    # This includes fields from both Facility and FacilityUnit models
    schema_fields = [
        # Facility fields
        StructField('code', StringType(), True),
        StructField('name', StringType(), True), 
        StructField('network_id', StringType(), True),
        StructField('network_region', StringType(), True),
        StructField('description', StringType(), True),
        StructField('fueltech_id', StringType(), True),
        StructField('status_id', StringType(), True),
        StructField('capacity_registered', DoubleType(), True),  # Keep as number
        StructField('emissions_factor_co2', DoubleType(), True),  # Keep as number
        StructField('data_first_seen', TimestampType(), True),  # UTC timestamp
        StructField('data_last_seen', TimestampType(), True),   # UTC timestamp
        StructField('dispatch_type', StringType(), True),
    ]
    
    return StructType(schema_fields)


def infer_schema_from_data(data, sample_size: int = 100):
    """
    Infer PySpark schema from data with support for variant types.
    
    Args:
        data: List of dictionaries or similar data structure
        sample_size: Number of records to sample for schema inference
        
    Returns:
        PySpark StructType schema
    """
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, VariantType, BooleanType, IntegerType
    
    if not data:
        return StructType()
    
    # Sample data for schema inference
    sample_data = data[:min(sample_size, len(data))]
    
    # Analyze field types across the sample
    field_types = {}
    for record in sample_data:
        for key, value in record.items():
            if key not in field_types:
                field_types[key] = set()
            
            if value is None:
                field_types[key].add(type(None))
            else:
                field_types[key].add(type(value))
    
    # Create schema fields
    schema_fields = []
    for field_name, types in field_types.items():
        # Remove None type for schema definition
        types.discard(type(None))
        
        if not types:
            # All values are None, default to StringType
            schema_fields.append(StructField(field_name, StringType(), True))
        elif len(types) == 1:
            # Single type, use appropriate PySpark type
            value_type = list(types)[0]
            if value_type in (int, float):
                schema_fields.append(StructField(field_name, DoubleType(), True))
            elif value_type == str:
                schema_fields.append(StructField(field_name, StringType(), True))
            elif value_type == bool:
                schema_fields.append(StructField(field_name, BooleanType(), True))
            elif 'datetime' in str(value_type) or 'Timestamp' in str(value_type):
                # Handle datetime/timestamp types - store as TimestampType with UTC conversion
                schema_fields.append(StructField(field_name, TimestampType(), True))
            else:
                # Use string type for safety
                schema_fields.append(StructField(field_name, StringType(), True))
        else:
            # Multiple types, use string type for compatibility
            schema_fields.append(StructField(field_name, StringType(), True))
    
    return StructType(schema_fields)
