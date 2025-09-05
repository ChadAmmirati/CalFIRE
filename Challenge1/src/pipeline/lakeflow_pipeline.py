"""
CalFIRE Production Data Ingestion Pipeline - Lakeflow Declarative Pipeline
Production-ready implementation leveraging the latest Databricks Lakeflow features
with real CalFIRE data sources and comprehensive monitoring
"""

from databricks.lakeflow import Pipeline, Source, Target, Transform
from databricks.lakeflow.sources import AutoLoader, API, Streaming
from databricks.lakeflow.targets import DeltaTable
from databricks.lakeflow.transforms import Clean, Validate, Enrich
from databricks.lakeflow.monitoring import Metrics, Alerts
import pyspark.sql.functions as F
from pyspark.sql.types import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pipeline Configuration
pipeline = Pipeline(
    name="calfire_wildfire_data_pipeline",
    description="Production-ready CalFIRE wildfire data ingestion pipeline with real data sources",
    version="2.0.0",
    environment="production",
    owner="CalFIRE Data Team",
    contact="data-team@calfire.gov"
)

# =============================================================================
# BRONZE LAYER - RAW DATA INGESTION
# =============================================================================

# 1. Real CalFIRE Fire Perimeters Data (Historical)
fire_perimeters_source = AutoLoader(
    name="california_fire_perimeters",
    source_path="abfss://raw-data@your-storage-account.dfs.core.windows.net/fire-perimeters/",
    file_format="json",  # GeoJSON format from CalFIRE
    schema_evolution=True,
    schema_hints={
        "properties": StructType([
            StructField("OBJECTID", IntegerType(), True),
            StructField("FIRE_NAME", StringType(), True),
            StructField("FIRE_YEAR", IntegerType(), True),
            StructField("ACRES", DoubleType(), True),
            StructField("FIRE_CAUSE", StringType(), True),
            StructField("FIRE_CAUSE_DESCR", StringType(), True),
            StructField("CONT_DATE", TimestampType(), True),
            StructField("ALARM_DATE", TimestampType(), True),
            StructField("UNIT_ID", StringType(), True),
            StructField("UNIT_TYPE", StringType(), True),
            StructField("COUNTY", StringType(), True),
            StructField("LATITUDE", DoubleType(), True),
            StructField("LONGITUDE", DoubleType(), True),
            StructField("GEOMETRY", StringType(), True)  # GeoJSON geometry
        ])
    },
    cloud_files_options={
        "multiline": "true",
        "allowBackslashEscapingAnyCharacter": "true",
        "allowUnquotedFieldNames": "true",
        "recursiveFileLookup": "true"
    },
    checkpoint_location="abfss://raw-data@your-storage-account.dfs.core.windows.net/checkpoints/fire-perimeters/"
)

fire_perimeters_target = DeltaTable(
    name="bronze_fire_perimeters",
    table_name="calfire.bronze.fire_perimeters",
    partition_columns=["fire_year"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "true"
    }
)

# 2. API Data Ingestion (Damage Inspection Data)
api_source = API(
    name="damage_inspection_api",
    endpoint="https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/CALFIRE_Damage_Inspection/FeatureServer/0/query",
    method="GET",
    parameters={
        "where": "1=1",
        "outFields": "*",
        "f": "json",
        "resultRecordCount": 1000
    },
    schedule="0 */6 * * *",  # Every 6 hours
    retry_policy={
        "max_retries": 3,
        "backoff_factor": 2
    }
)

api_target = DeltaTable(
    name="bronze_damage_inspection",
    table_name="calfire.bronze.damage_inspection",
    partition_columns=["inspection_date"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true"
    }
)

# 3. Streaming Data Ingestion (Simulated IoT/Real-time Alerts)
streaming_source = Streaming(
    name="fire_alerts_stream",
    source_type="kafka",
    kafka_options={
        "kafka.bootstrap.servers": "kafka-cluster:9092",
        "subscribe": "fire-alerts",
        "startingOffsets": "latest"
    },
    schema=StructType([
        StructField("alert_id", StringType(), True),
        StructField("fire_id", StringType(), True),
        StructField("alert_type", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("description", StringType(), True)
    ])
)

streaming_target = DeltaTable(
    name="bronze_fire_alerts",
    table_name="calfire.bronze.fire_alerts",
    partition_columns=["alert_date"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true"
    }
)

# =============================================================================
# SILVER LAYER - DATA CLEANING AND VALIDATION
# =============================================================================

# Fire Perimeters Cleaning and Validation
fire_perimeters_clean = Clean(
    name="clean_fire_perimeters",
    source_table="calfire.bronze.fire_perimeters",
    transformations=[
        # Standardize fire names
        F.upper(F.col("FIRE_NAME")).alias("fire_name"),
        # Convert acres to standardized format
        F.when(F.col("ACRES").isNull(), 0).otherwise(F.col("ACRES")).alias("acres"),
        # Standardize fire cause
        F.when(F.col("FIRE_CAUSE_DESCR").isNull(), "UNKNOWN")
         .otherwise(F.upper(F.col("FIRE_CAUSE_DESCR"))).alias("fire_cause"),
        # Add data quality flags
        F.when(F.col("LATITUDE").between(32.5, 42.0) & 
               F.col("LONGITUDE").between(-124.5, -114.0), "VALID")
         .otherwise("INVALID").alias("geo_validation"),
        # Add processing timestamp
        F.current_timestamp().alias("processed_at")
    ],
    data_quality_rules=[
        {
            "name": "valid_coordinates",
            "rule": "LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL",
            "action": "quarantine"
        },
        {
            "name": "valid_fire_year",
            "rule": "FIRE_YEAR BETWEEN 1950 AND 2025",
            "action": "quarantine"
        },
        {
            "name": "valid_acres",
            "rule": "ACRES >= 0",
            "action": "quarantine"
        }
    ]
)

fire_perimeters_validate = Validate(
    name="validate_fire_perimeters",
    source_table="calfire.silver.fire_perimeters_clean",
    validation_rules=[
        {
            "name": "required_fields",
            "rule": "fire_name IS NOT NULL AND fire_year IS NOT NULL",
            "severity": "error"
        },
        {
            "name": "coordinate_bounds",
            "rule": "latitude BETWEEN 32.5 AND 42.0 AND longitude BETWEEN -124.5 AND -114.0",
            "severity": "warning"
        }
    ]
)

# Damage Inspection Cleaning
damage_inspection_clean = Clean(
    name="clean_damage_inspection",
    source_table="calfire.bronze.damage_inspection",
    transformations=[
        F.upper(F.col("DAMAGE_LEVEL")).alias("damage_level"),
        F.col("INSPECTION_DATE").cast("timestamp").alias("inspection_date"),
        F.col("LATITUDE").cast("double").alias("latitude"),
        F.col("LONGITUDE").cast("double").alias("longitude"),
        F.current_timestamp().alias("processed_at")
    ]
)

# =============================================================================
# GOLD LAYER - ENRICHED ANALYTICS TABLES
# =============================================================================

# Geospatial Enrichment using Mosaic/H3
fire_perimeters_enriched = Enrich(
    name="enrich_fire_perimeters",
    source_table="calfire.silver.fire_perimeters_validate",
    enrichment_sources=[
        {
            "table": "calfire.silver.damage_inspection_clean",
            "join_type": "spatial",
            "join_condition": "ST_DWithin(ST_Point(longitude, latitude), ST_Point(LONGITUDE, LATITUDE), 1000)"
        }
    ],
    transformations=[
        # Add H3 index for spatial queries
        F.expr("h3_longlatash3(longitude, latitude, 8)").alias("h3_index"),
        # Calculate fire duration
        (F.col("CONT_DATE").cast("long") - F.col("ALARM_DATE").cast("long")) / 86400
        .alias("duration_days"),
        # Add damage assessment
        F.when(F.col("damage_level").isNotNull(), F.col("damage_level"))
         .otherwise("UNKNOWN").alias("damage_assessment")
    ]
)

# Aggregated Analytics Table
fire_analytics = Transform(
    name="create_fire_analytics",
    source_table="calfire.gold.fire_perimeters_enriched",
    transformations=[
        F.col("fire_year").alias("year"),
        F.col("county").alias("county"),
        F.col("fire_cause").alias("cause"),
        F.sum("acres").over(
            F.Window.partitionBy("fire_year", "county")
        ).alias("total_acres_by_county_year"),
        F.count("*").over(
            F.Window.partitionBy("fire_year", "county")
        ).alias("fire_count_by_county_year"),
        F.avg("duration_days").over(
            F.Window.partitionBy("fire_year")
        ).alias("avg_duration_days"),
        F.current_timestamp().alias("last_updated")
    ]
)

# =============================================================================
# MONITORING AND METRICS
# =============================================================================

# Pipeline Metrics
pipeline_metrics = Metrics(
    name="pipeline_metrics",
    metrics=[
        {
            "name": "records_processed",
            "type": "counter",
            "description": "Total records processed by pipeline"
        },
        {
            "name": "processing_latency",
            "type": "histogram",
            "description": "End-to-end processing latency"
        },
        {
            "name": "data_quality_score",
            "type": "gauge",
            "description": "Overall data quality score (0-100)"
        },
        {
            "name": "error_rate",
            "type": "gauge",
            "description": "Percentage of records with errors"
        }
    ]
)

# Alerting Configuration
pipeline_alerts = Alerts(
    name="pipeline_alerts",
    alerts=[
        {
            "name": "high_error_rate",
            "condition": "error_rate > 5",
            "severity": "critical",
            "notification": "email:admin@calfire.gov"
        },
        {
            "name": "processing_delay",
            "condition": "processing_latency > 300",
            "severity": "warning",
            "notification": "slack:#data-alerts"
        },
        {
            "name": "data_quality_degradation",
            "condition": "data_quality_score < 80",
            "severity": "warning",
            "notification": "email:data-team@calfire.gov"
        }
    ]
)

# =============================================================================
# PIPELINE DEFINITION
# =============================================================================

# Define the complete pipeline
pipeline.add_source(batch_source)
pipeline.add_source(api_source)
pipeline.add_source(streaming_source)

pipeline.add_target(batch_target)
pipeline.add_target(api_target)
pipeline.add_target(streaming_target)

pipeline.add_transform(fire_perimeters_clean)
pipeline.add_transform(fire_perimeters_validate)
pipeline.add_transform(damage_inspection_clean)
pipeline.add_transform(fire_perimeters_enriched)
pipeline.add_transform(fire_analytics)

pipeline.add_monitoring(pipeline_metrics)
pipeline.add_monitoring(pipeline_alerts)

# Pipeline execution configuration
pipeline.configure(
    cluster_config={
        "node_type_id": "i3.xlarge",
        "num_workers": 2,
        "spark_conf": {
            "spark.databricks.delta.optimizeWrite.enabled": "true",
            "spark.databricks.delta.autoCompact.enabled": "true",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true"
        }
    },
    schedule="0 0 * * *",  # Daily at midnight
    retry_policy={
        "max_retries": 3,
        "retry_delay": 300
    }
)

if __name__ == "__main__":
    # Deploy the pipeline
    pipeline.deploy()
    print("CalFIRE Data Ingestion Pipeline deployed successfully!")
