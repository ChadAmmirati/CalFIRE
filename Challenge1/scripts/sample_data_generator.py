"""
CalFIRE Sample Data Generator
Generates realistic sample data for testing the pipeline
"""

import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any
import random
import os

class CalFIRESampleDataGenerator:
    """Generate realistic sample data for CalFIRE pipeline testing"""
    
    def __init__(self, seed: int = 42):
        random.seed(seed)
        np.random.seed(seed)
        
        # California counties and their approximate coordinates
        self.counties = {
            'Los Angeles': {'lat': 34.0522, 'lon': -118.2437},
            'San Diego': {'lat': 32.7157, 'lon': -117.1611},
            'Orange': {'lat': 33.7879, 'lon': -117.8531},
            'Riverside': {'lat': 33.9533, 'lon': -117.3962},
            'San Bernardino': {'lat': 34.1083, 'lon': -117.2898},
            'Santa Clara': {'lat': 37.3541, 'lon': -121.9552},
            'Alameda': {'lat': 37.6017, 'lon': -121.7195},
            'Sacramento': {'lat': 38.5816, 'lon': -121.4944},
            'Contra Costa': {'lat': 37.9199, 'lon': -122.3481},
            'Fresno': {'lat': 36.7378, 'lon': -119.7871},
            'Ventura': {'lat': 34.3705, 'lon': -119.1391},
            'San Francisco': {'lat': 37.7749, 'lon': -122.4194},
            'Kern': {'lat': 35.3433, 'lon': -118.7278},
            'San Mateo': {'lat': 37.4969, 'lon': -122.3331},
            'Tulare': {'lat': 36.2077, 'lon': -118.7811}
        }
        
        # Fire causes and their descriptions
        self.fire_causes = {
            'Human': ['Arson', 'Equipment Use', 'Smoking', 'Campfire', 'Debris Burning'],
            'Lightning': ['Lightning Strike', 'Dry Lightning'],
            'Vehicle': ['Vehicle Fire', 'Mechanical Failure'],
            'Powerline': ['Powerline Failure', 'Electrical Equipment'],
            'Railroad': ['Railroad Operations', 'Train Derailment'],
            'Unknown': ['Under Investigation', 'Undetermined']
        }
        
        # Damage levels
        self.damage_levels = ['MINOR', 'MODERATE', 'MAJOR', 'DESTROYED', 'UNKNOWN']
        
        # Structure types
        self.structure_types = ['Residential', 'Commercial', 'Industrial', 'Agricultural', 'Infrastructure']
    
    def generate_fire_perimeters_geojson(self, num_fires: int = 100, 
                                       start_year: int = 2020, 
                                       end_year: int = 2024) -> Dict[str, Any]:
        """Generate GeoJSON data for fire perimeters"""
        
        features = []
        
        for i in range(num_fires):
            # Select random county
            county = random.choice(list(self.counties.keys()))
            base_lat = self.counties[county]['lat']
            base_lon = self.counties[county]['lon']
            
            # Generate fire data
            fire_name = f"Sample Fire {i+1:03d}"
            fire_year = random.randint(start_year, end_year)
            acres = random.uniform(0.1, 50000)  # 0.1 to 50,000 acres
            
            # Select fire cause
            cause_category = random.choice(list(self.fire_causes.keys()))
            cause_description = random.choice(self.fire_causes[cause_category])
            
            # Generate dates
            alarm_date = datetime(fire_year, random.randint(1, 12), random.randint(1, 28))
            cont_date = alarm_date + timedelta(days=random.randint(1, 30))
            
            # Generate coordinates (small random offset from county center)
            lat_offset = random.uniform(-0.5, 0.5)
            lon_offset = random.uniform(-0.5, 0.5)
            latitude = base_lat + lat_offset
            longitude = base_lon + lon_offset
            
            # Generate simple polygon around the point
            polygon_coords = self._generate_polygon_coords(latitude, longitude, acres)
            
            # Create feature
            feature = {
                "type": "Feature",
                "properties": {
                    "OBJECTID": i + 1,
                    "FIRE_NAME": fire_name,
                    "FIRE_YEAR": fire_year,
                    "ACRES": round(acres, 2),
                    "FIRE_CAUSE": cause_category,
                    "FIRE_CAUSE_DESCR": cause_description,
                    "CONT_DATE": cont_date.strftime("%Y-%m-%d"),
                    "ALARM_DATE": alarm_date.strftime("%Y-%m-%d"),
                    "UNIT_ID": f"UNIT_{random.randint(100, 999)}",
                    "UNIT_TYPE": random.choice(['CAL FIRE', 'USFS', 'NPS', 'BLM']),
                    "COUNTY": county,
                    "LATITUDE": round(latitude, 6),
                    "LONGITUDE": round(longitude, 6)
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [polygon_coords]
                }
            }
            
            features.append(feature)
        
        geojson = {
            "type": "FeatureCollection",
            "features": features
        }
        
        return geojson
    
    def _generate_polygon_coords(self, center_lat: float, center_lon: float, 
                                acres: float) -> List[List[float]]:
        """Generate polygon coordinates around a center point"""
        
        # Convert acres to approximate radius in degrees
        # Rough approximation: 1 acre ≈ 0.0001 square degrees
        radius = np.sqrt(acres * 0.0001 / np.pi)
        
        # Generate 6-sided polygon
        num_sides = 6
        coords = []
        
        for i in range(num_sides + 1):  # +1 to close the polygon
            angle = 2 * np.pi * i / num_sides
            lat_offset = radius * np.cos(angle)
            lon_offset = radius * np.sin(angle)
            
            lat = center_lat + lat_offset
            lon = center_lon + lon_offset
            
            coords.append([lon, lat])  # GeoJSON uses [lon, lat] order
        
        return coords
    
    def generate_damage_inspection_data(self, num_inspections: int = 500) -> pd.DataFrame:
        """Generate damage inspection data"""
        
        data = []
        
        for i in range(num_inspections):
            # Select random county
            county = random.choice(list(self.counties.keys()))
            base_lat = self.counties[county]['lat']
            base_lon = self.counties[county]['lon']
            
            # Generate inspection data
            inspection_date = datetime.now() - timedelta(days=random.randint(1, 365))
            damage_level = random.choice(self.damage_levels)
            structure_type = random.choice(self.structure_types)
            
            # Generate coordinates
            lat_offset = random.uniform(-0.3, 0.3)
            lon_offset = random.uniform(-0.3, 0.3)
            latitude = base_lat + lat_offset
            longitude = base_lon + lon_offset
            
            # Generate damage description
            damage_description = self._generate_damage_description(damage_level, structure_type)
            
            record = {
                'OBJECTID': i + 1,
                'DAMAGE_LEVEL': damage_level,
                'INSPECTION_DATE': inspection_date.strftime("%Y-%m-%d %H:%M:%S"),
                'LATITUDE': round(latitude, 6),
                'LONGITUDE': round(longitude, 6),
                'STRUCTURE_TYPE': structure_type,
                'DAMAGE_DESCRIPTION': damage_description,
                'INSPECTOR_NAME': f"Inspector {random.randint(1, 50)}",
                'COUNTY': county
            }
            
            data.append(record)
        
        return pd.DataFrame(data)
    
    def _generate_damage_description(self, damage_level: str, structure_type: str) -> str:
        """Generate realistic damage description"""
        
        descriptions = {
            'MINOR': [
                'Minor smoke damage to exterior',
                'Slight charring on siding',
                'Minor heat damage to roof',
                'Light smoke staining'
            ],
            'MODERATE': [
                'Significant smoke and heat damage',
                'Partial structural damage',
                'Extensive smoke damage throughout',
                'Moderate fire damage to multiple areas'
            ],
            'MAJOR': [
                'Severe structural damage',
                'Extensive fire damage throughout structure',
                'Major structural compromise',
                'Significant fire and smoke damage'
            ],
            'DESTROYED': [
                'Structure completely destroyed',
                'Total loss due to fire',
                'Structure burned to ground',
                'Complete destruction'
            ],
            'UNKNOWN': [
                'Damage assessment pending',
                'Under investigation',
                'Assessment in progress',
                'To be determined'
            ]
        }
        
        return random.choice(descriptions.get(damage_level, ['Unknown damage']))
    
    def generate_streaming_alerts(self, num_alerts: int = 100) -> pd.DataFrame:
        """Generate streaming fire alert data"""
        
        data = []
        alert_types = ['FIRE_DETECTED', 'FIRE_SPREAD', 'EVACUATION_ORDER', 'CONTAINMENT_UPDATE']
        severity_levels = ['LOW', 'MEDIUM', 'HIGH', 'CRITICAL']
        
        for i in range(num_alerts):
            # Select random county
            county = random.choice(list(self.counties.keys()))
            base_lat = self.counties[county]['lat']
            base_lon = self.counties[county]['lon']
            
            # Generate alert data
            alert_type = random.choice(alert_types)
            severity = random.choice(severity_levels)
            fire_id = f"FIRE_{random.randint(1000, 9999)}"
            
            # Generate coordinates
            lat_offset = random.uniform(-0.2, 0.2)
            lon_offset = random.uniform(-0.2, 0.2)
            latitude = base_lat + lat_offset
            longitude = base_lon + lon_offset
            
            # Generate timestamp (recent)
            timestamp = datetime.now() - timedelta(hours=random.randint(1, 72))
            
            # Generate description
            description = self._generate_alert_description(alert_type, severity)
            
            record = {
                'alert_id': f"ALERT_{int(timestamp.timestamp())}_{i}",
                'fire_id': fire_id,
                'alert_type': alert_type,
                'severity': severity,
                'latitude': round(latitude, 6),
                'longitude': round(longitude, 6),
                'timestamp': timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                'description': description,
                'county': county
            }
            
            data.append(record)
        
        return pd.DataFrame(data)
    
    def _generate_alert_description(self, alert_type: str, severity: str) -> str:
        """Generate realistic alert description"""
        
        descriptions = {
            'FIRE_DETECTED': [
                f'{severity} fire detected in the area',
                f'New fire ignition reported - {severity} severity',
                f'Fire detection alert - {severity} level threat'
            ],
            'FIRE_SPREAD': [
                f'Fire spreading rapidly - {severity} threat level',
                f'Fire growth detected - {severity} severity',
                f'Fire expansion alert - {severity} level'
            ],
            'EVACUATION_ORDER': [
                f'{severity} evacuation order issued',
                f'Mandatory evacuation - {severity} threat',
                f'Evacuation alert - {severity} level'
            ],
            'CONTAINMENT_UPDATE': [
                f'Containment update - {severity} progress',
                f'Fire containment status - {severity} level',
                f'Containment report - {severity} status'
            ]
        }
        
        return random.choice(descriptions.get(alert_type, ['Alert notification']))
    
    def save_sample_data(self, output_dir: str = "data/sample"):
        """Save all sample data to files"""
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate and save fire perimeters GeoJSON
        print("Generating fire perimeters GeoJSON...")
        fire_geojson = self.generate_fire_perimeters_geojson(num_fires=200)
        with open(f"{output_dir}/fire_perimeters_sample.geojson", 'w') as f:
            json.dump(fire_geojson, f, indent=2)
        
        # Generate and save damage inspection data
        print("Generating damage inspection data...")
        damage_df = self.generate_damage_inspection_data(num_inspections=1000)
        damage_df.to_csv(f"{output_dir}/damage_inspection_sample.csv", index=False)
        
        # Generate and save streaming alerts
        print("Generating streaming alerts...")
        alerts_df = self.generate_streaming_alerts(num_alerts=200)
        alerts_df.to_csv(f"{output_dir}/fire_alerts_sample.csv", index=False)
        
        # Create a summary file
        summary = {
            'generated_at': datetime.now().isoformat(),
            'files_created': [
                'fire_perimeters_sample.geojson',
                'damage_inspection_sample.csv',
                'fire_alerts_sample.csv'
            ],
            'record_counts': {
                'fire_perimeters': len(fire_geojson['features']),
                'damage_inspections': len(damage_df),
                'fire_alerts': len(alerts_df)
            },
            'data_ranges': {
                'fire_years': [2020, 2024],
                'counties': list(self.counties.keys()),
                'fire_causes': list(self.fire_causes.keys()),
                'damage_levels': self.damage_levels
            }
        }
        
        with open(f"{output_dir}/sample_data_summary.json", 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"Sample data generated successfully in '{output_dir}' directory!")
        print(f"Files created:")
        print(f"  - fire_perimeters_sample.geojson ({len(fire_geojson['features'])} records)")
        print(f"  - damage_inspection_sample.csv ({len(damage_df)} records)")
        print(f"  - fire_alerts_sample.csv ({len(alerts_df)} records)")
        print(f"  - sample_data_summary.json")

def main():
    """Generate sample data for testing"""
    generator = CalFIRESampleDataGenerator()
    generator.save_sample_data()

if __name__ == "__main__":
    main()
