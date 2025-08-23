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
