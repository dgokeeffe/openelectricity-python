"""
Tests for the PySpark streaming data source implementation.
"""

import json
import os
import tempfile
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from openelectricity.pyspark_datasource import (
    OpenElectricityStreamDataSource,
    OpenElectricityStreamReader,
    RangePartition,
)


class TestOpenElectricityStreamDataSource:
    """Test the OpenElectricityStreamDataSource class."""
    
    def test_name(self):
        """Test that the data source returns the correct name."""
        assert OpenElectricityStreamDataSource.name() == "openelectricity_stream"
    
    def test_schema(self):
        """Test that the schema is properly defined."""
        data_source = OpenElectricityStreamDataSource({})
        schema = data_source.schema()
        
        assert isinstance(schema, StructType)
        assert len(schema.fields) == 22  # All expected fields
        
        # Check some key fields
        field_names = [field.name for field in schema.fields]
        assert "interval" in field_names
        assert "network_code" in field_names
        assert "power" in field_names
        assert "energy" in field_names
    
    def test_stream_reader(self):
        """Test that stream reader is created correctly."""
        data_source = OpenElectricityStreamDataSource({})
        schema = data_source.schema()
        options = {"api_key": "test_key", "network_code": "NEM"}
        
        reader = data_source.streamReader(schema, options)
        assert isinstance(reader, OpenElectricityStreamReader)


class TestOpenElectricityStreamReader:
    """Test the OpenElectricityStreamReader class."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir
    
    @pytest.fixture
    def mock_client(self):
        """Create a mock OpenElectricity client."""
        with patch('openelectricity.pyspark_datasource.OEClient') as mock:
            client_instance = Mock()
            mock.return_value = client_instance
            yield client_instance
    
    @pytest.fixture
    def reader(self, temp_dir, mock_client):
        """Create a test reader instance."""
        schema = StructType([])
        options = {
            "api_key": "test_key",
            "network_code": "NEM",
            "metrics": ["power"],
            "interval": "5m",
            "batch_size_hours": 1,
            "progress_path": temp_dir,
        }
        return OpenElectricityStreamReader(schema, options)
    
    def test_initialization(self, reader):
        """Test reader initialization."""
        assert reader.network_code == "NEM"
        assert reader.metrics == ["power"]
        assert reader.interval == "5m"
        assert reader.batch_size_hours == 1
    
    def test_initial_offset(self, reader):
        """Test initial offset calculation."""
        offset = reader.initialOffset()
        assert "timestamp" in offset
        assert isinstance(offset["timestamp"], str)
    
    def test_progress_tracking(self, reader, temp_dir):
        """Test progress tracking functionality."""
        # Test saving progress
        test_timestamp = datetime.now()
        reader.current_timestamp = test_timestamp
        reader._save_progress()
        
        # Test loading progress
        reader.current_timestamp = datetime.now() - timedelta(hours=2)
        reader._load_progress()
        
        # Should have loaded the saved timestamp
        assert reader.current_timestamp == test_timestamp
    
    def test_latest_offset(self, reader):
        """Test latest offset calculation."""
        reader.current_timestamp = datetime(2024, 1, 1, 12, 0, 0)
        offset = reader.latestOffset()
        
        assert "timestamp" in offset
        end_timestamp = datetime.fromisoformat(offset["timestamp"])
        expected_end = reader.current_timestamp + timedelta(hours=1)
        assert end_timestamp == expected_end
    
    def test_partitions(self, reader):
        """Test partition creation."""
        start = {"timestamp": "2024-01-01T12:00:00"}
        end = {"timestamp": "2024-01-01T13:00:00"}
        
        partitions = reader.partitions(start, end)
        assert len(partitions) == 1
        assert isinstance(partitions[0], RangePartition)
        assert partitions[0].start == start["timestamp"]
        assert partitions[0].end == end["timestamp"]
    
    def test_commit(self, reader):
        """Test commit functionality."""
        end_timestamp = datetime.now()
        end_dict = {"timestamp": end_timestamp.isoformat()}
        
        reader.commit(end_dict)
        assert reader.current_timestamp == end_timestamp
    
    def test_record_to_tuple(self, reader):
        """Test record to tuple conversion."""
        record = {
            "interval": datetime.now(),
            "power": 100.5,
            "energy": 200.0,
            "network_region": "NSW1",
        }
        
        tuple_result = reader._record_to_tuple(record)
        assert isinstance(tuple_result, tuple)
        assert len(tuple_result) == 22  # All schema fields
    
    def test_empty_tuple(self, reader):
        """Test empty tuple creation."""
        empty_tuple = reader._empty_tuple()
        assert isinstance(empty_tuple, tuple)
        assert len(empty_tuple) == 22
        assert all(value is None for value in empty_tuple)


class TestRangePartition:
    """Test the RangePartition class."""
    
    def test_initialization(self):
        """Test partition initialization."""
        start = "2024-01-01T12:00:00"
        end = "2024-01-01T13:00:00"
        
        partition = RangePartition(start, end)
        assert partition.start == start
        assert partition.end == end


class TestIntegration:
    """Integration tests for the streaming data source."""
    
    @pytest.fixture
    def spark_session(self):
        """Create a test Spark session."""
        spark = SparkSession.builder \
            .appName("TestOpenElectricityStreaming") \
            .master("local[1]") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()
        yield spark
        spark.stop()
    
    def test_data_source_registration(self, spark_session):
        """Test that the data source can be registered with Spark."""
        spark_session.dataSource.register(OpenElectricityStreamDataSource)
        
        # Verify registration by checking if we can create a reader
        data_source = OpenElectricityStreamDataSource({})
        schema = data_source.schema()
        options = {"api_key": "test_key", "network_code": "NEM"}
        
        reader = data_source.streamReader(schema, options)
        assert isinstance(reader, OpenElectricityStreamReader)


if __name__ == "__main__":
    pytest.main([__file__])
