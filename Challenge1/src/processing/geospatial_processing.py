"""
CalFIRE Geospatial Processing Module
Advanced geospatial operations using Mosaic and H3 libraries
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
import logging
from datetime import datetime
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class GeospatialConfig:
    """Configuration for geospatial operations"""
    h3_resolution: int = 8
    buffer_distance_meters: float = 1000.0
    coordinate_system: str = "EPSG:4326"
    enable_mosaic: bool = True
    enable_h3: bool = True

class GeospatialProcessor:
    """
    Advanced geospatial processing for wildfire data
    Leverages Databricks Mosaic and H3 libraries for high-performance spatial operations
    """
    
    def __init__(self, config: GeospatialConfig = None):
        self.config = config or GeospatialConfig()
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize Mosaic and H3 functions
        self._initialize_geospatial_libraries()
    
    def _initialize_geospatial_libraries(self):
        """Initialize Mosaic and H3 libraries"""
        try:
            # In Databricks, these would be available as built-in functions
            # For this example, we'll create mock implementations
            self.logger.info("Initializing geospatial libraries (Mosaic, H3)")
            
            # Mock Mosaic functions
            self.mosaic_functions = {
                'st_point': self._mock_st_point,
                'st_polygon': self._mock_st_polygon,
                'st_intersects': self._mock_st_intersects,
                'st_within': self._mock_st_within,
                'st_buffer': self._mock_st_buffer,
                'st_area': self._mock_st_area,
                'st_distance': self._mock_st_distance
            }
            
            # Mock H3 functions
            self.h3_functions = {
                'h3_longlatash3': self._mock_h3_longlatash3,
                'h3_h3tostring': self._mock_h3_h3tostring,
                'h3_h3togeo': self._mock_h3_h3togeo,
                'h3_h3togeoboundary': self._mock_h3_h3togeoboundary
            }
            
            self.logger.info("Geospatial libraries initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize geospatial libraries: {str(e)}")
            raise
    
    def enrich_fire_perimeters(self, fire_df: pd.DataFrame, damage_df: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich fire perimeter data with damage inspection data using spatial joins
        """
        try:
            self.logger.info(f"Enriching {len(fire_df)} fire perimeters with damage data")
            
            # Add H3 indices for efficient spatial operations
            fire_df = self._add_h3_indices(fire_df)
            damage_df = self._add_h3_indices(damage_df)
            
            # Perform spatial join
            enriched_df = self._spatial_join(fire_df, damage_df)
            
            # Add spatial statistics
            enriched_df = self._add_spatial_statistics(enriched_df)
            
            self.logger.info(f"Enrichment completed: {len(enriched_df)} enriched records")
            return enriched_df
            
        except Exception as e:
            self.logger.error(f"Failed to enrich fire perimeters: {str(e)}")
            raise
    
    def _add_h3_indices(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add H3 indices to DataFrame for spatial indexing"""
        try:
            if 'latitude' in df.columns and 'longitude' in df.columns:
                df['h3_index'] = df.apply(
                    lambda row: self.h3_functions['h3_longlatash3'](
                        row['longitude'], row['latitude'], self.config.h3_resolution
                    ), axis=1
                )
                self.logger.debug(f"Added H3 indices to {len(df)} records")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to add H3 indices: {str(e)}")
            return df
    
    def _spatial_join(self, fire_df: pd.DataFrame, damage_df: pd.DataFrame) -> pd.DataFrame:
        """Perform spatial join between fire perimeters and damage data"""
        try:
            # Group damage data by H3 index for efficient joining
            agg_dict = {
                'damage_level': lambda x: list(x),
                'inspection_date': lambda x: list(x)
            }
            
            # Add optional columns if they exist
            if 'structure_type' in damage_df.columns:
                agg_dict['structure_type'] = lambda x: list(x)
            if 'damage_description' in damage_df.columns:
                agg_dict['damage_description'] = lambda x: list(x)
            
            damage_by_h3 = damage_df.groupby('h3_index').agg(agg_dict).reset_index()
            
            # Rename columns to avoid conflicts
            new_columns = ['h3_index', 'damage_levels', 'inspection_dates']
            if 'structure_type' in damage_df.columns:
                new_columns.append('structure_types')
            if 'damage_description' in damage_df.columns:
                new_columns.append('damage_descriptions')
            
            damage_by_h3.columns = new_columns
            
            # Join with fire data
            enriched_df = fire_df.merge(
                damage_by_h3, 
                on='h3_index', 
                how='left'
            )
            
            # Add damage statistics
            enriched_df['damage_count'] = enriched_df['damage_levels'].apply(
                lambda x: len(x) if isinstance(x, list) else 0
            )
            
            enriched_df['has_damage'] = enriched_df['damage_count'] > 0
            
            # Calculate damage severity score
            enriched_df['damage_severity_score'] = enriched_df['damage_levels'].apply(
                self._calculate_damage_severity_score
            )
            
            return enriched_df
            
        except Exception as e:
            self.logger.error(f"Failed to perform spatial join: {str(e)}")
            raise
    
    def _calculate_damage_severity_score(self, damage_levels: List[str]) -> float:
        """Calculate damage severity score based on damage levels"""
        if not isinstance(damage_levels, list) or not damage_levels:
            return 0.0
        
        severity_weights = {
            'MINOR': 1.0,
            'MODERATE': 2.0,
            'MAJOR': 3.0,
            'DESTROYED': 4.0,
            'UNKNOWN': 0.5
        }
        
        total_score = sum(severity_weights.get(level, 0.0) for level in damage_levels)
        return total_score / len(damage_levels) if damage_levels else 0.0
    
    def _add_spatial_statistics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add spatial statistics to the DataFrame"""
        try:
            # Add fire area calculations (mock implementation)
            df['estimated_area_km2'] = df['acres'] * 0.00404686  # Convert acres to km²
            
            # Add spatial density metrics
            df['spatial_density'] = df['damage_count'] / (df['estimated_area_km2'] + 0.001)
            
            # Add temporal statistics
            if 'inspection_dates' in df.columns:
                df['latest_inspection'] = df['inspection_dates'].apply(
                    lambda x: max(x) if isinstance(x, list) and x else None
                )
                df['inspection_count'] = df['inspection_dates'].apply(
                    lambda x: len(x) if isinstance(x, list) else 0
                )
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to add spatial statistics: {str(e)}")
            return df
    
    def create_spatial_aggregations(self, df: pd.DataFrame, 
                                  group_columns: List[str] = None) -> pd.DataFrame:
        """Create spatial aggregations for analytics"""
        try:
            if group_columns is None:
                group_columns = ['fire_year', 'county']
            
            self.logger.info(f"Creating spatial aggregations grouped by {group_columns}")
            
            # Define aggregation functions
            agg_functions = {
                'fire_name': 'count',
                'acres': ['sum', 'mean', 'max'],
                'damage_count': ['sum', 'mean'],
                'damage_severity_score': ['mean', 'max'],
                'estimated_area_km2': 'sum',
                'spatial_density': 'mean'
            }
            
            # Perform aggregations
            aggregated_df = df.groupby(group_columns).agg(agg_functions).reset_index()
            
            # Flatten column names
            aggregated_df.columns = ['_'.join(col).strip() if col[1] else col[0] 
                                   for col in aggregated_df.columns]
            
            # Add derived metrics
            aggregated_df['fire_frequency'] = aggregated_df['fire_name_count']
            aggregated_df['total_area_burned'] = aggregated_df['acres_sum']
            aggregated_df['avg_fire_size'] = aggregated_df['acres_mean']
            aggregated_df['max_fire_size'] = aggregated_df['acres_max']
            aggregated_df['total_damage_incidents'] = aggregated_df['damage_count_sum']
            aggregated_df['avg_damage_severity'] = aggregated_df['damage_severity_score_mean']
            
            self.logger.info(f"Spatial aggregations completed: {len(aggregated_df)} aggregated records")
            return aggregated_df
            
        except Exception as e:
            self.logger.error(f"Failed to create spatial aggregations: {str(e)}")
            raise
    
    def perform_spatial_analysis(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Perform comprehensive spatial analysis"""
        try:
            self.logger.info("Performing comprehensive spatial analysis")
            
            analysis_results = {
                'timestamp': datetime.now().isoformat(),
                'total_records': len(df),
                'spatial_coverage': {},
                'temporal_analysis': {},
                'damage_analysis': {},
                'hotspot_analysis': {}
            }
            
            # Spatial coverage analysis
            if 'latitude' in df.columns and 'longitude' in df.columns:
                analysis_results['spatial_coverage'] = {
                    'min_latitude': df['latitude'].min(),
                    'max_latitude': df['latitude'].max(),
                    'min_longitude': df['longitude'].min(),
                    'max_longitude': df['longitude'].max(),
                    'centroid_latitude': df['latitude'].mean(),
                    'centroid_longitude': df['longitude'].mean()
                }
            
            # Temporal analysis
            if 'fire_year' in df.columns:
                year_counts = df['fire_year'].value_counts().sort_index()
                analysis_results['temporal_analysis'] = {
                    'year_range': [df['fire_year'].min(), df['fire_year'].max()],
                    'total_years': len(year_counts),
                    'peak_year': year_counts.idxmax(),
                    'peak_year_count': year_counts.max(),
                    'yearly_trend': year_counts.to_dict()
                }
            
            # Damage analysis
            if 'damage_count' in df.columns:
                damage_stats = df['damage_count'].describe()
                analysis_results['damage_analysis'] = {
                    'total_damage_incidents': df['damage_count'].sum(),
                    'fires_with_damage': (df['damage_count'] > 0).sum(),
                    'damage_rate': (df['damage_count'] > 0).mean(),
                    'avg_damage_per_fire': df['damage_count'].mean(),
                    'max_damage_per_fire': df['damage_count'].max(),
                    'damage_statistics': damage_stats.to_dict()
                }
            
            # Hotspot analysis using H3
            if 'h3_index' in df.columns:
                h3_counts = df['h3_index'].value_counts()
                analysis_results['hotspot_analysis'] = {
                    'total_h3_cells': len(h3_counts),
                    'most_active_cell': h3_counts.index[0] if len(h3_counts) > 0 else None,
                    'most_active_cell_count': h3_counts.iloc[0] if len(h3_counts) > 0 else 0,
                    'high_activity_threshold': h3_counts.quantile(0.95),
                    'high_activity_cells': len(h3_counts[h3_counts >= h3_counts.quantile(0.95)])
                }
            
            self.logger.info("Spatial analysis completed successfully")
            return analysis_results
            
        except Exception as e:
            self.logger.error(f"Failed to perform spatial analysis: {str(e)}")
            raise
    
    # Mock implementations of Mosaic functions
    def _mock_st_point(self, x: float, y: float) -> str:
        """Mock ST_Point function"""
        return f"POINT({x} {y})"
    
    def _mock_st_polygon(self, coordinates: List[List[float]]) -> str:
        """Mock ST_Polygon function"""
        coord_str = ", ".join([f"{coord[0]} {coord[1]}" for coord in coordinates])
        return f"POLYGON(({coord_str}))"
    
    def _mock_st_intersects(self, geom1: str, geom2: str) -> bool:
        """Mock ST_Intersects function"""
        # Simplified intersection check
        return True  # In real implementation, would use proper geometry library
    
    def _mock_st_within(self, geom1: str, geom2: str) -> bool:
        """Mock ST_Within function"""
        return False  # Simplified implementation
    
    def _mock_st_buffer(self, geom: str, distance: float) -> str:
        """Mock ST_Buffer function"""
        return f"BUFFER({geom}, {distance})"
    
    def _mock_st_area(self, geom: str) -> float:
        """Mock ST_Area function"""
        return 100.0  # Simplified implementation
    
    def _mock_st_distance(self, geom1: str, geom2: str) -> float:
        """Mock ST_Distance function"""
        return 1000.0  # Simplified implementation
    
    # Mock implementations of H3 functions
    def _mock_h3_longlatash3(self, longitude: float, latitude: float, resolution: int) -> str:
        """Mock h3_longlatash3 function"""
        # Generate a mock H3 index based on coordinates
        import hashlib
        coord_str = f"{longitude:.6f},{latitude:.6f},{resolution}"
        hash_val = hashlib.md5(coord_str.encode()).hexdigest()[:12]
        return f"8{hash_val}"
    
    def _mock_h3_h3tostring(self, h3_index: str) -> str:
        """Mock h3_h3tostring function"""
        return h3_index
    
    def _mock_h3_h3togeo(self, h3_index: str) -> Tuple[float, float]:
        """Mock h3_h3togeo function"""
        # Return mock coordinates
        return (37.7749, -122.4194)
    
    def _mock_h3_h3togeoboundary(self, h3_index: str) -> List[Tuple[float, float]]:
        """Mock h3_h3togeoboundary function"""
        # Return mock boundary coordinates
        return [(37.7749, -122.4194), (37.7750, -122.4194), 
                (37.7750, -122.4193), (37.7749, -122.4193)]

class GeospatialAnalytics:
    """Advanced geospatial analytics for wildfire data"""
    
    def __init__(self, processor: GeospatialProcessor):
        self.processor = processor
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def analyze_fire_trends(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze fire trends over time and space"""
        try:
            self.logger.info("Analyzing fire trends")
            
            trends = {
                'temporal_trends': {},
                'spatial_trends': {},
                'severity_trends': {},
                'damage_trends': {}
            }
            
            # Temporal trends
            if 'fire_year' in df.columns and 'acres' in df.columns:
                yearly_stats = df.groupby('fire_year')['acres'].agg(['sum', 'mean', 'count']).reset_index()
                trends['temporal_trends'] = {
                    'total_acres_by_year': yearly_stats.set_index('fire_year')['sum'].to_dict(),
                    'avg_fire_size_by_year': yearly_stats.set_index('fire_year')['mean'].to_dict(),
                    'fire_count_by_year': yearly_stats.set_index('fire_year')['count'].to_dict()
                }
            
            # Spatial trends
            if 'county' in df.columns and 'acres' in df.columns:
                county_stats = df.groupby('county')['acres'].agg(['sum', 'mean', 'count']).reset_index()
                trends['spatial_trends'] = {
                    'total_acres_by_county': county_stats.set_index('county')['sum'].to_dict(),
                    'avg_fire_size_by_county': county_stats.set_index('county')['mean'].to_dict(),
                    'fire_count_by_county': county_stats.set_index('county')['count'].to_dict()
                }
            
            # Severity trends
            if 'fire_cause' in df.columns and 'acres' in df.columns:
                cause_stats = df.groupby('fire_cause')['acres'].agg(['sum', 'mean', 'count']).reset_index()
                trends['severity_trends'] = {
                    'total_acres_by_cause': cause_stats.set_index('fire_cause')['sum'].to_dict(),
                    'avg_fire_size_by_cause': cause_stats.set_index('fire_cause')['mean'].to_dict(),
                    'fire_count_by_cause': cause_stats.set_index('fire_cause')['count'].to_dict()
                }
            
            # Damage trends
            if 'damage_count' in df.columns and 'fire_year' in df.columns:
                damage_trends = df.groupby('fire_year')['damage_count'].agg(['sum', 'mean']).reset_index()
                trends['damage_trends'] = {
                    'total_damage_by_year': damage_trends.set_index('fire_year')['sum'].to_dict(),
                    'avg_damage_per_fire_by_year': damage_trends.set_index('fire_year')['mean'].to_dict()
                }
            
            return trends
            
        except Exception as e:
            self.logger.error(f"Failed to analyze fire trends: {str(e)}")
            raise
    
    def identify_hotspots(self, df: pd.DataFrame, threshold_percentile: float = 95) -> pd.DataFrame:
        """Identify fire hotspots based on H3 indices"""
        try:
            self.logger.info(f"Identifying hotspots using {threshold_percentile}th percentile threshold")
            
            if 'h3_index' not in df.columns:
                self.logger.warning("No H3 indices found, adding them first")
                df = self.processor._add_h3_indices(df)
            
            # Count fires per H3 cell
            h3_counts = df['h3_index'].value_counts().reset_index()
            h3_counts.columns = ['h3_index', 'fire_count']
            
            # Calculate threshold
            threshold = h3_counts['fire_count'].quantile(threshold_percentile / 100)
            
            # Identify hotspots
            hotspots = h3_counts[h3_counts['fire_count'] >= threshold].copy()
            hotspots['hotspot_type'] = 'high_activity'
            hotspots['threshold'] = threshold
            
            # Add additional metrics for hotspots
            hotspot_details = []
            for _, hotspot in hotspots.iterrows():
                h3_cell_data = df[df['h3_index'] == hotspot['h3_index']]
                
                detail = {
                    'h3_index': hotspot['h3_index'],
                    'fire_count': hotspot['fire_count'],
                    'total_acres': h3_cell_data['acres'].sum() if 'acres' in h3_cell_data.columns else 0,
                    'avg_fire_size': h3_cell_data['acres'].mean() if 'acres' in h3_cell_data.columns else 0,
                    'damage_count': h3_cell_data['damage_count'].sum() if 'damage_count' in h3_cell_data.columns else 0,
                    'fire_years': list(h3_cell_data['fire_year'].unique()) if 'fire_year' in h3_cell_data.columns else [],
                    'counties': list(h3_cell_data['county'].unique()) if 'county' in h3_cell_data.columns else []
                }
                hotspot_details.append(detail)
            
            hotspots_df = pd.DataFrame(hotspot_details)
            
            self.logger.info(f"Identified {len(hotspots_df)} hotspots")
            return hotspots_df
            
        except Exception as e:
            self.logger.error(f"Failed to identify hotspots: {str(e)}")
            raise

# Example usage
if __name__ == "__main__":
    # Create sample data
    fire_data = pd.DataFrame({
        'fire_name': ['Test Fire 1', 'Test Fire 2', 'Test Fire 3'],
        'fire_year': [2024, 2023, 2024],
        'acres': [100.5, 250.0, 75.0],
        'latitude': [37.7749, 34.0522, 36.7783],
        'longitude': [-122.4194, -118.2437, -119.4179],
        'county': ['San Francisco', 'Los Angeles', 'Fresno'],
        'fire_cause': ['Human', 'Lightning', 'Human']
    })
    
    damage_data = pd.DataFrame({
        'damage_level': ['MINOR', 'MAJOR', 'MODERATE'],
        'inspection_date': ['2024-01-15', '2024-01-16', '2024-01-17'],
        'latitude': [37.7750, 34.0523, 36.7784],
        'longitude': [-122.4195, -118.2438, -119.4180],
        'structure_type': ['Residential', 'Commercial', 'Residential']
    })
    
    # Initialize geospatial processor
    config = GeospatialConfig(h3_resolution=8)
    processor = GeospatialProcessor(config)
    
    # Enrich fire data with damage data
    print("Enriching fire perimeters with damage data...")
    enriched_data = processor.enrich_fire_perimeters(fire_data, damage_data)
    print(f"Enriched data shape: {enriched_data.shape}")
    print(f"Columns: {list(enriched_data.columns)}")
    
    # Create spatial aggregations
    print("\nCreating spatial aggregations...")
    aggregated_data = processor.create_spatial_aggregations(enriched_data)
    print(f"Aggregated data shape: {aggregated_data.shape}")
    print(f"Columns: {list(aggregated_data.columns)}")
    
    # Perform spatial analysis
    print("\nPerforming spatial analysis...")
    analysis_results = processor.perform_spatial_analysis(enriched_data)
    print(f"Analysis results keys: {list(analysis_results.keys())}")
    
    # Identify hotspots
    print("\nIdentifying hotspots...")
    analytics = GeospatialAnalytics(processor)
    hotspots = analytics.identify_hotspots(enriched_data)
    print(f"Hotspots identified: {len(hotspots)}")
    
    # Analyze trends
    print("\nAnalyzing fire trends...")
    trends = analytics.analyze_fire_trends(enriched_data)
    print(f"Trend analysis keys: {list(trends.keys())}")
    
    print("\nGeospatial processing completed successfully!")
