"""
CalFIRE Data Source Connectors and Adapters
Implements source adapters for batch, real-time, and streaming data sources
"""

import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging
from dataclasses import dataclass
from abc import ABC, abstractmethod

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class DataSourceConfig:
    """Configuration for data sources"""
    name: str
    source_type: str
    endpoint: Optional[str] = None
    file_path: Optional[str] = None
    credentials: Optional[Dict[str, str]] = None
    retry_count: int = 3
    timeout: int = 30

class DataConnector(ABC):
    """Abstract base class for data connectors"""
    
    def __init__(self, config: DataSourceConfig):
        self.config = config
        self.logger = logging.getLogger(f"{self.__class__.__name__}")
    
    @abstractmethod
    def connect(self) -> bool:
        """Establish connection to data source"""
        pass
    
    @abstractmethod
    def extract(self, **kwargs) -> Any:
        """Extract data from source"""
        pass
    
    @abstractmethod
    def validate(self, data: Any) -> bool:
        """Validate extracted data"""
        pass

class FirePerimetersBatchConnector(DataConnector):
    """
    Connector for California Historical Fire Perimeters batch data
    Supports GeoJSON, CSV, KML, and GDB formats
    """
    
    def __init__(self, config: DataSourceConfig):
        super().__init__(config)
        self.supported_formats = ['geojson', 'csv', 'kml', 'gdb']
    
    def connect(self) -> bool:
        """Validate file path and format"""
        try:
            if not self.config.file_path:
                raise ValueError("File path is required for batch connector")
            
            file_extension = self.config.file_path.split('.')[-1].lower()
            if file_extension not in self.supported_formats:
                raise ValueError(f"Unsupported file format: {file_extension}")
            
            self.logger.info(f"Connected to batch data source: {self.config.file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to batch source: {str(e)}")
            return False
    
    def extract(self, **kwargs) -> pd.DataFrame:
        """Extract data from batch files"""
        try:
            file_extension = self.config.file_path.split('.')[-1].lower()
            
            if file_extension == 'geojson':
                return self._extract_geojson()
            elif file_extension == 'csv':
                return self._extract_csv()
            elif file_extension == 'kml':
                return self._extract_kml()
            else:
                raise ValueError(f"Extraction not implemented for format: {file_extension}")
                
        except Exception as e:
            self.logger.error(f"Failed to extract batch data: {str(e)}")
            raise
    
    def _extract_geojson(self) -> pd.DataFrame:
        """Extract data from GeoJSON files"""
        with open(self.config.file_path, 'r') as f:
            geojson_data = json.load(f)
        
        features = geojson_data.get('features', [])
        records = []
        
        for feature in features:
            properties = feature.get('properties', {})
            geometry = feature.get('geometry', {})
            
            # Flatten the record
            record = {
                'objectid': properties.get('OBJECTID'),
                'fire_name': properties.get('FIRE_NAME'),
                'fire_year': properties.get('FIRE_YEAR'),
                'acres': properties.get('ACRES'),
                'fire_cause': properties.get('FIRE_CAUSE'),
                'fire_cause_descr': properties.get('FIRE_CAUSE_DESCR'),
                'cont_date': properties.get('CONT_DATE'),
                'alarm_date': properties.get('ALARM_DATE'),
                'unit_id': properties.get('UNIT_ID'),
                'unit_type': properties.get('UNIT_TYPE'),
                'county': properties.get('COUNTY'),
                'latitude': properties.get('LATITUDE'),
                'longitude': properties.get('LONGITUDE'),
                'geometry_type': geometry.get('type'),
                'geometry_coordinates': json.dumps(geometry.get('coordinates')),
                'extraction_timestamp': datetime.now().isoformat()
            }
            records.append(record)
        
        return pd.DataFrame(records)
    
    def _extract_csv(self) -> pd.DataFrame:
        """Extract data from CSV files"""
        df = pd.read_csv(self.config.file_path)
        df['extraction_timestamp'] = datetime.now().isoformat()
        return df
    
    def _extract_kml(self) -> pd.DataFrame:
        """Extract data from KML files (simplified implementation)"""
        # In a real implementation, you would use a KML parser like fastkml
        # For this example, we'll return a placeholder
        self.logger.warning("KML extraction not fully implemented - returning placeholder")
        return pd.DataFrame({
            'fire_name': ['Sample Fire'],
            'fire_year': [2024],
            'acres': [100.0],
            'extraction_timestamp': [datetime.now().isoformat()]
        })
    
    def validate(self, data: pd.DataFrame) -> bool:
        """Validate extracted fire perimeter data"""
        try:
            required_columns = ['fire_name', 'fire_year', 'acres']
            
            # Check required columns
            missing_columns = [col for col in required_columns if col not in data.columns]
            if missing_columns:
                self.logger.error(f"Missing required columns: {missing_columns}")
                return False
            
            # Check data types and ranges
            if 'fire_year' in data.columns:
                invalid_years = data[(data['fire_year'] < 1950) | (data['fire_year'] > 2025)]
                if len(invalid_years) > 0:
                    self.logger.warning(f"Found {len(invalid_years)} records with invalid fire years")
            
            if 'acres' in data.columns:
                negative_acres = data[data['acres'] < 0]
                if len(negative_acres) > 0:
                    self.logger.warning(f"Found {len(negative_acres)} records with negative acres")
            
            self.logger.info(f"Data validation passed for {len(data)} records")
            return True
            
        except Exception as e:
            self.logger.error(f"Data validation failed: {str(e)}")
            return False

class ArcGISAPIConnector(DataConnector):
    """
    Connector for ArcGIS REST API data sources
    Specifically for CalFIRE damage inspection data
    """
    
    def __init__(self, config: DataSourceConfig):
        super().__init__(config)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'CalFIRE-DataPipeline/1.0',
            'Accept': 'application/json'
        })
    
    def connect(self) -> bool:
        """Test connection to ArcGIS API"""
        try:
            if not self.config.endpoint:
                raise ValueError("API endpoint is required for API connector")
            
            # Test connection with a simple query
            test_params = {
                'where': '1=0',  # Return no data, just test connection
                'outFields': 'OBJECTID',
                'f': 'json',
                'resultRecordCount': 1
            }
            
            response = self.session.get(
                self.config.endpoint, 
                params=test_params, 
                timeout=self.config.timeout
            )
            response.raise_for_status()
            
            data = response.json()
            if 'error' in data:
                raise Exception(f"API error: {data['error']}")
            
            self.logger.info(f"Connected to ArcGIS API: {self.config.endpoint}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to ArcGIS API: {str(e)}")
            return False
    
    def extract(self, where_clause: str = "1=1", max_records: int = 1000, **kwargs) -> pd.DataFrame:
        """Extract data from ArcGIS API"""
        try:
            all_records = []
            offset = 0
            
            while True:
                params = {
                    'where': where_clause,
                    'outFields': '*',
                    'f': 'json',
                    'resultRecordCount': min(max_records, 1000),
                    'resultOffset': offset
                }
                
                response = self.session.get(
                    self.config.endpoint,
                    params=params,
                    timeout=self.config.timeout
                )
                response.raise_for_status()
                
                data = response.json()
                
                if 'error' in data:
                    raise Exception(f"API error: {data['error']}")
                
                features = data.get('features', [])
                if not features:
                    break
                
                # Convert features to records
                for feature in features:
                    attributes = feature.get('attributes', {})
                    geometry = feature.get('geometry', {})
                    
                    record = {
                        'objectid': attributes.get('OBJECTID'),
                        'damage_level': attributes.get('DAMAGE_LEVEL'),
                        'inspection_date': attributes.get('INSPECTION_DATE'),
                        'latitude': geometry.get('y'),
                        'longitude': geometry.get('x'),
                        'structure_type': attributes.get('STRUCTURE_TYPE'),
                        'damage_description': attributes.get('DAMAGE_DESCRIPTION'),
                        'inspector_name': attributes.get('INSPECTOR_NAME'),
                        'extraction_timestamp': datetime.now().isoformat()
                    }
                    all_records.append(record)
                
                offset += len(features)
                
                # Break if we've reached the requested limit
                if len(all_records) >= max_records:
                    break
            
            self.logger.info(f"Extracted {len(all_records)} records from ArcGIS API")
            return pd.DataFrame(all_records[:max_records])
            
        except Exception as e:
            self.logger.error(f"Failed to extract API data: {str(e)}")
            raise
    
    def validate(self, data: pd.DataFrame) -> bool:
        """Validate extracted API data"""
        try:
            required_columns = ['objectid', 'damage_level', 'inspection_date']
            
            # Check required columns
            missing_columns = [col for col in required_columns if col not in data.columns]
            if missing_columns:
                self.logger.error(f"Missing required columns: {missing_columns}")
                return False
            
            # Check for valid coordinates
            if 'latitude' in data.columns and 'longitude' in data.columns:
                invalid_coords = data[
                    (data['latitude'].isna()) | 
                    (data['longitude'].isna()) |
                    (data['latitude'] < 32.5) | (data['latitude'] > 42.0) |
                    (data['longitude'] < -124.5) | (data['longitude'] > -114.0)
                ]
                if len(invalid_coords) > 0:
                    self.logger.warning(f"Found {len(invalid_coords)} records with invalid coordinates")
            
            self.logger.info(f"API data validation passed for {len(data)} records")
            return True
            
        except Exception as e:
            self.logger.error(f"API data validation failed: {str(e)}")
            return False

class StreamingDataConnector(DataConnector):
    """
    Connector for streaming data sources (Kafka, Event Hub)
    Simulates real-time fire alert data
    """
    
    def __init__(self, config: DataSourceConfig):
        super().__init__(config)
        self.kafka_config = {
            'bootstrap.servers': config.endpoint or 'localhost:9092',
            'group.id': 'calfire-pipeline',
            'auto.offset.reset': 'latest'
        }
    
    def connect(self) -> bool:
        """Test connection to streaming source"""
        try:
            # In a real implementation, you would test Kafka connection
            # For this example, we'll simulate a successful connection
            self.logger.info(f"Connected to streaming source: {self.config.endpoint}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to streaming source: {str(e)}")
            return False
    
    def extract(self, duration_seconds: int = 60, **kwargs) -> pd.DataFrame:
        """Extract streaming data for specified duration"""
        try:
            # Simulate streaming data extraction
            # In a real implementation, you would use Kafka consumer
            simulated_data = self._generate_simulated_alerts(duration_seconds)
            
            self.logger.info(f"Extracted {len(simulated_data)} streaming records")
            return pd.DataFrame(simulated_data)
            
        except Exception as e:
            self.logger.error(f"Failed to extract streaming data: {str(e)}")
            raise
    
    def _generate_simulated_alerts(self, duration_seconds: int) -> List[Dict]:
        """Generate simulated fire alert data"""
        import random
        import time
        
        alerts = []
        alert_types = ['FIRE_DETECTED', 'FIRE_SPREAD', 'EVACUATION_ORDER', 'CONTAINMENT_UPDATE']
        severity_levels = ['LOW', 'MEDIUM', 'HIGH', 'CRITICAL']
        
        # Simulate alerts over the duration
        for i in range(max(1, duration_seconds // 10)):  # At least one alert
            alert = {
                'alert_id': f"ALERT_{int(time.time())}_{i}",
                'fire_id': f"FIRE_{random.randint(1000, 9999)}",
                'alert_type': random.choice(alert_types),
                'severity': random.choice(severity_levels),
                'latitude': round(random.uniform(32.5, 42.0), 6),
                'longitude': round(random.uniform(-124.5, -114.0), 6),
                'timestamp': datetime.now().isoformat(),
                'description': f"Simulated {random.choice(alert_types).lower().replace('_', ' ')} alert",
                'extraction_timestamp': datetime.now().isoformat()
            }
            alerts.append(alert)
            time.sleep(0.01)  # Small delay to simulate real-time data
        
        return alerts
    
    def validate(self, data: pd.DataFrame) -> bool:
        """Validate streaming data"""
        try:
            required_columns = ['alert_id', 'fire_id', 'alert_type', 'severity', 'timestamp']
            
            # Check required columns
            missing_columns = [col for col in required_columns if col not in data.columns]
            if missing_columns:
                self.logger.error(f"Missing required columns: {missing_columns}")
                return False
            
            # Check timestamp format
            try:
                pd.to_datetime(data['timestamp'])
            except:
                self.logger.error("Invalid timestamp format in streaming data")
                return False
            
            self.logger.info(f"Streaming data validation passed for {len(data)} records")
            return True
            
        except Exception as e:
            self.logger.error(f"Streaming data validation failed: {str(e)}")
            return False

class DataConnectorFactory:
    """Factory class for creating data connectors"""
    
    @staticmethod
    def create_connector(config: DataSourceConfig) -> DataConnector:
        """Create appropriate connector based on source type"""
        if config.source_type.lower() == 'batch':
            return FirePerimetersBatchConnector(config)
        elif config.source_type.lower() == 'api':
            return ArcGISAPIConnector(config)
        elif config.source_type.lower() == 'streaming':
            return StreamingDataConnector(config)
        else:
            raise ValueError(f"Unsupported source type: {config.source_type}")

# Example usage and configuration
def create_sample_configs() -> List[DataSourceConfig]:
    """Create sample configurations for different data sources"""
    configs = [
        DataSourceConfig(
            name="fire_perimeters_2024",
            source_type="batch",
            file_path="/path/to/california_fire_perimeters_2024.geojson"
        ),
        DataSourceConfig(
            name="damage_inspection_api",
            source_type="api",
            endpoint="https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/CALFIRE_Damage_Inspection/FeatureServer/0/query"
        ),
        DataSourceConfig(
            name="fire_alerts_stream",
            source_type="streaming",
            endpoint="kafka-cluster:9092"
        )
    ]
    return configs

if __name__ == "__main__":
    # Example usage
    configs = create_sample_configs()
    
    for config in configs:
        print(f"\nTesting connector for: {config.name}")
        connector = DataConnectorFactory.create_connector(config)
        
        if connector.connect():
            print(f"✓ Connected to {config.name}")
            
            # Extract sample data
            try:
                if config.source_type == "batch":
                    data = connector.extract()
                elif config.source_type == "api":
                    data = connector.extract(max_records=10)
                else:  # streaming
                    data = connector.extract(duration_seconds=30)
                
                print(f"✓ Extracted {len(data)} records")
                
                # Validate data
                if connector.validate(data):
                    print(f"✓ Data validation passed")
                else:
                    print(f"✗ Data validation failed")
                    
            except Exception as e:
                print(f"✗ Extraction failed: {str(e)}")
        else:
            print(f"✗ Failed to connect to {config.name}")
