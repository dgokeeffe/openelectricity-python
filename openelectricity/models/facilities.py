"""
Facility models for the OpenElectricity API.

This module contains models related to facility data and responses.
"""

from datetime import datetime
import datetime as dt

from pydantic import BaseModel, Field

from openelectricity.models.base import APIResponse
from openelectricity.types import NetworkCode, UnitFueltechType, UnitStatusType


def convert_field_value(key: str, value):
    """
    Convert field values with appropriate types for Spark compatibility.
    
    Args:
        key: Field name to help determine appropriate conversion
        value: Field value to convert
        
    Returns:
        Converted value optimized for Spark
    """
    if value is None:
        return None
    elif hasattr(value, 'value'):  # Enum objects
        return str(value)
    elif hasattr(value, 'isoformat'):  # Datetime objects
        # Convert timezone-aware datetime to UTC for TimestampType compatibility
        if hasattr(value, 'tzinfo') and value.tzinfo is not None:
            # Convert timezone-aware datetime to UTC
            return value.astimezone(dt.timezone.utc).replace(tzinfo=None)
        else:
            return value  # Already naive datetime, assume UTC
    elif isinstance(value, bool):
        return value  # Keep booleans
    elif isinstance(value, (int, float)) and key in ['capacity_registered', 'emissions_factor_co2']:
        return float(value)  # Keep numeric fields as numbers
    elif isinstance(value, (int, float)):
        return value  # Keep other numbers as-is
    else:
        return str(value)  # Convert everything else to string for safety


class FacilityUnit(BaseModel):
    """A unit within a facility."""

    code: str = Field(..., description="Unit code")
    fueltech_id: UnitFueltechType = Field(..., description="Fuel technology type")
    status_id: UnitStatusType = Field(..., description="Unit status")
    capacity_registered: float | None = Field(None, description="Registered capacity in MW")
    emissions_factor_co2: float | None = Field(None, description="CO2 emissions factor")
    data_first_seen: datetime | None = Field(None, description="When data was first seen for this unit")
    data_last_seen: datetime | None = Field(None, description="When data was last seen for this unit")
    dispatch_type: str = Field(..., description="Dispatch type")


class Facility(BaseModel):
    """A facility in the OpenElectricity system."""

    code: str = Field(..., description="Facility code")
    name: str = Field(..., description="Facility name")
    network_id: NetworkCode = Field(..., description="Network code")
    network_region: str = Field(..., description="Network region")
    description: str | None = Field(None, description="Facility description")
    units: list[FacilityUnit] = Field(..., description="Units within the facility")


class FacilityResponse(APIResponse[Facility]):
    """Response model for facility endpoints."""

    data: list[Facility]

    def to_pyspark(self, spark_session=None, app_name: str = "OpenElectricity") -> "Optional['DataFrame']":  # noqa: F821
        """
        Convert facility data into a PySpark DataFrame.

        Args:
            spark_session: Optional PySpark session. If not provided, will try to create one.
            app_name: Name for the Spark application if creating a new session.

        Returns:
            A PySpark DataFrame containing the facility data, or None if PySpark is not available
        """
        try:
            from openelectricity.spark_utils import create_spark_dataframe
            
            # Convert facilities to list of dictionaries
            if not self.data:
                return None
            
            # Debug logging to understand data structure
            import logging
            logger = logging.getLogger(__name__)
            logger.debug(f"Converting {len(self.data)} facilities to PySpark DataFrame")
            if self.data:
                logger.debug(f"First facility type: {type(self.data[0])}")
                if hasattr(self.data[0], 'units'):
                    logger.debug(f"First facility units type: {type(self.data[0].units)}")
                    if self.data[0].units:
                        logger.debug(f"First unit type: {type(self.data[0].units[0])}")
                
            # Convert each facility to dict, handling nested units
            records = []
            for i, facility in enumerate(self.data):
                try:
                    # Convert facility to dict
                    facility_dict = facility.model_dump()
                    
                    # Handle units - create separate records for each unit
                    units = facility_dict.get('units', [])
                    if units and isinstance(units, list):
                        for j, unit in enumerate(units):
                            try:
                                # Create combined record
                                record = {}
                                
                                # Add facility fields (excluding units) with proper type preservation
                                for key, value in facility_dict.items():
                                    if key != 'units':
                                        record[key] = convert_field_value(key, value)
                                
                                # Add unit fields with proper type preservation
                                for key, value in unit.items():
                                    record[key] = convert_field_value(key, value)
                                
                                records.append(record)
                                
                            except Exception as unit_error:
                                logger.warning(f"Error processing unit {j} of facility {i}: {unit_error}")
                                continue
                    else:
                        # No units, just add facility data
                        record = {}
                        for key, value in facility_dict.items():
                            if key != 'units':
                                record[key] = convert_field_value(key, value)
                        records.append(record)
                        
                except Exception as facility_error:
                    logger.warning(f"Error processing facility {i}: {facility_error}")
                    continue
            
            # Debug: Check if we have any records and their structure
            logger.debug(f"Created {len(records)} records for PySpark conversion")
            if records:
                logger.debug(f"First record keys: {list(records[0].keys())}")
                logger.debug(f"First record sample: {str(records[0])[:200]}...")
            
            # Try to create DataFrame using predefined schema optimized for facilities
            try:
                if spark_session is None:
                    from openelectricity.spark_utils import get_spark_session
                    spark_session = get_spark_session()
                
                # Use predefined schema aligned with Pydantic models for better performance
                from openelectricity.spark_utils import create_facilities_flattened_schema
                
                facilities_schema = create_facilities_flattened_schema()
                
                logger.debug(f"Creating PySpark DataFrame with {len(records)} records using predefined schema")
                logger.debug(f"Schema aligned with Pydantic models: {facilities_schema}")
                
                # Create DataFrame with predefined schema
                df = spark_session.createDataFrame(records, schema=facilities_schema)
                logger.debug(f"Successfully created PySpark DataFrame with {len(records)} records")
                return df
                
            except Exception as spark_error:
                logger.error(f"Error creating PySpark DataFrame: {spark_error}")
                import traceback
                logger.debug(f"Full error traceback: {traceback.format_exc()}")
                logger.info("Falling back to None - use to_pandas() for facilities data")
                return None
            
        except ImportError:
            # Log warning but don't raise error to maintain compatibility
            import logging
            logger = logging.getLogger(__name__)
            logger.warning("PySpark not available. Install with: uv add 'openelectricity[analysis]'")
            return None
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Error converting to PySpark DataFrame: {e}")
            return None

    def to_pandas(self) -> "pd.DataFrame":  # noqa: F821
        """
        Convert facility data into a Pandas DataFrame.

        Returns:
            A Pandas DataFrame containing the facility data
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "Pandas is required for DataFrame conversion. Install it with: uv add 'openelectricity[analysis]'"
            ) from None

        # Convert facilities to list of dictionaries
        if not self.data:
            return pd.DataFrame()
            
        # Convert each facility to dict, handling nested units
        records = []
        for facility in self.data:
            # Ensure we have a Pydantic model, not a dict
            if hasattr(facility, 'model_dump'):
                facility_dict = facility.model_dump()
            else:
                # If it's already a dict, use it directly
                facility_dict = dict(facility)
            
            # Flatten units if needed
            units = facility_dict.get('units', [])
            if units:
                for unit in units:
                    # Ensure unit is a Pydantic model
                    if hasattr(unit, 'model_dump'):
                        unit_dict = unit.model_dump()
                    else:
                        # If it's already a dict, use it directly
                        unit_dict = dict(unit)
                    
                    # Create a new dict to avoid recursion issues
                    combined = {}
                    # Add facility fields (excluding units)
                    for key, value in facility_dict.items():
                        if key != 'units':
                            combined[key] = value
                    # Add unit fields
                    for key, value in unit_dict.items():
                        combined[key] = value
                    
                    records.append(combined)
            else:
                # Create a copy to avoid modifying the original
                facility_copy = {}
                for key, value in facility_dict.items():
                    if key != 'units':
                        facility_copy[key] = value
                records.append(facility_copy)
        
        return pd.DataFrame(records)
