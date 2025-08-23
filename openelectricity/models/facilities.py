"""
Facility models for the OpenElectricity API.

This module contains models related to facility data and responses.
"""

from datetime import datetime

from pydantic import BaseModel, Field

from openelectricity.models.base import APIResponse
from openelectricity.types import NetworkCode, UnitFueltechType, UnitStatusType


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
                
            # Convert each facility to dict, handling nested units
            records = []
            for facility in self.data:
                facility_dict = facility.model_dump()
                # Flatten units if needed
                if facility_dict.get('units'):
                    for unit in facility_dict['units']:
                        unit_dict = unit.model_dump()
                        # Combine facility and unit data
                        combined = {**facility_dict, **unit_dict}
                        # Remove the nested units list
                        combined.pop('units', None)
                        records.append(combined)
                else:
                    records.append(facility_dict)
                
            return create_spark_dataframe(records, spark_session=spark_session, app_name=app_name)
            
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
