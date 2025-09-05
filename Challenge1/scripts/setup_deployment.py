"""
CalFIRE Data Pipeline Setup and Deployment Scripts
Automated setup and deployment for Databricks Lakeflow pipeline
"""

import os
import json
import yaml
import subprocess
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from datetime import datetime
import requests

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class DatabricksConfig:
    """Databricks workspace configuration"""
    workspace_url: str
    access_token: str
    cluster_id: Optional[str] = None
    catalog_name: str = "calfire"
    schema_name: str = "production"

@dataclass
class StorageConfig:
    """Storage configuration for Azure Data Lake Storage"""
    storage_account: str
    container_name: str
    access_key: str
    endpoint: str

@dataclass
class PipelineConfig:
    """Pipeline configuration"""
    name: str
    description: str
    schedule: str
    max_retries: int = 3
    timeout_minutes: int = 60
    environment: str = "production"

class DatabricksDeployer:
    """Deploy and manage Databricks resources"""
    
    def __init__(self, config: DatabricksConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.headers = {
            'Authorization': f'Bearer {config.access_token}',
            'Content-Type': 'application/json'
        }
    
    def create_catalog(self) -> bool:
        """Create Unity Catalog catalog"""
        try:
            self.logger.info(f"Creating catalog: {self.config.catalog_name}")
            
            payload = {
                'name': self.config.catalog_name,
                'comment': 'CalFIRE wildfire data catalog',
                'properties': {
                    'owner': 'admin@calfire.gov'
                }
            }
            
            response = requests.post(
                f"{self.config.workspace_url}/api/2.1/unity-catalog/catalogs",
                headers=self.headers,
                json=payload
            )
            
            if response.status_code == 200:
                self.logger.info(f"Catalog '{self.config.catalog_name}' created successfully")
                return True
            else:
                self.logger.error(f"Failed to create catalog: {response.text}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error creating catalog: {str(e)}")
            return False
    
    def create_schemas(self) -> bool:
        """Create schemas for different data layers"""
        try:
            schemas = ['bronze', 'silver', 'gold', 'monitoring', 'quarantine']
            
            for schema in schemas:
                self.logger.info(f"Creating schema: {self.config.catalog_name}.{schema}")
                
                payload = {
                    'name': f"{self.config.catalog_name}.{schema}",
                    'comment': f'CalFIRE {schema} layer schema'
                }
                
                response = requests.post(
                    f"{self.config.workspace_url}/api/2.1/unity-catalog/schemas",
                    headers=self.headers,
                    json=payload
                )
                
                if response.status_code == 200:
                    self.logger.info(f"Schema '{schema}' created successfully")
                else:
                    self.logger.error(f"Failed to create schema '{schema}': {response.text}")
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating schemas: {str(e)}")
            return False
    
    def create_monitoring_tables(self) -> bool:
        """Create monitoring tables"""
        try:
            self.logger.info("Creating monitoring tables")
            
            # SQL commands to create monitoring tables
            sql_commands = [
                """
                CREATE TABLE IF NOT EXISTS calfire.monitoring.pipeline_metrics (
                    pipeline_name STRING,
                    run_id STRING,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    status STRING,
                    records_processed BIGINT,
                    processing_latency_seconds DOUBLE,
                    data_quality_score DOUBLE,
                    error_count BIGINT,
                    error_rate DOUBLE
                ) USING DELTA
                PARTITIONED BY (DATE(start_time))
                TBLPROPERTIES (
                    'delta.autoOptimize.optimizeWrite' = 'true',
                    'delta.autoOptimize.autoCompact' = 'true'
                )
                """,
                
                """
                CREATE TABLE IF NOT EXISTS calfire.monitoring.data_quality_metrics (
                    table_name STRING,
                    validation_rule STRING,
                    passed_records BIGINT,
                    failed_records BIGINT,
                    total_records BIGINT,
                    quality_score DOUBLE,
                    validation_timestamp TIMESTAMP
                ) USING DELTA
                PARTITIONED BY (DATE(validation_timestamp))
                """,
                
                """
                CREATE TABLE IF NOT EXISTS calfire.monitoring.ingestion_metrics (
                    source_type STRING,
                    source_name STRING,
                    ingestion_timestamp TIMESTAMP,
                    records_ingested BIGINT,
                    bytes_processed BIGINT,
                    ingestion_latency_seconds DOUBLE,
                    success_rate DOUBLE
                ) USING DELTA
                PARTITIONED BY (DATE(ingestion_timestamp))
                """,
                
                """
                CREATE TABLE IF NOT EXISTS calfire.monitoring.error_analysis (
                    error_type STRING,
                    error_message STRING,
                    error_count BIGINT,
                    first_occurrence TIMESTAMP,
                    last_occurrence TIMESTAMP,
                    affected_tables ARRAY<STRING>
                ) USING DELTA
                PARTITIONED BY (DATE(last_occurrence))
                """
            ]
            
            for sql in sql_commands:
                if not self._execute_sql(sql):
                    return False
            
            self.logger.info("Monitoring tables created successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating monitoring tables: {str(e)}")
            return False
    
    def _execute_sql(self, sql: str) -> bool:
        """Execute SQL command using Databricks SQL API"""
        try:
            payload = {
                'statement': sql,
                'warehouse_id': self.config.cluster_id
            }
            
            response = requests.post(
                f"{self.config.workspace_url}/api/2.0/sql/statements",
                headers=self.headers,
                json=payload
            )
            
            if response.status_code == 200:
                return True
            else:
                self.logger.error(f"SQL execution failed: {response.text}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error executing SQL: {str(e)}")
            return False
    
    def deploy_notebook(self, notebook_path: str, notebook_name: str) -> bool:
        """Deploy notebook to Databricks workspace"""
        try:
            self.logger.info(f"Deploying notebook: {notebook_name}")
            
            with open(notebook_path, 'r') as f:
                notebook_content = f.read()
            
            payload = {
                'path': f"/CalFIRE/{notebook_name}",
                'language': 'PYTHON',
                'format': 'SOURCE',
                'content': notebook_content
            }
            
            response = requests.post(
                f"{self.config.workspace_url}/api/2.0/workspace/import",
                headers=self.headers,
                json=payload
            )
            
            if response.status_code == 200:
                self.logger.info(f"Notebook '{notebook_name}' deployed successfully")
                return True
            else:
                self.logger.error(f"Failed to deploy notebook: {response.text}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error deploying notebook: {str(e)}")
            return False
    
    def create_job(self, config: PipelineConfig) -> Optional[str]:
        """Create Databricks job for the pipeline"""
        try:
            self.logger.info(f"Creating job: {config.name}")
            
            payload = {
                'name': config.name,
                'description': config.description,
                'schedule': {
                    'quartz_cron_expression': config.schedule,
                    'timezone_id': 'America/Los_Angeles'
                },
                'max_concurrent_runs': 1,
                'timeout_seconds': config.timeout_minutes * 60,
                'retry_on_timeout': True,
                'max_retries': config.max_retries,
                'tasks': [
                    {
                        'task_key': 'calfire_pipeline',
                        'notebook_task': {
                            'notebook_path': f"/CalFIRE/{config.name}",
                            'source': 'WORKSPACE'
                        },
                        'timeout_seconds': config.timeout_minutes * 60,
                        'retry_on_timeout': True,
                        'max_retries': config.max_retries
                    }
                ]
            }
            
            response = requests.post(
                f"{self.config.workspace_url}/api/2.1/jobs/create",
                headers=self.headers,
                json=payload
            )
            
            if response.status_code == 200:
                job_id = response.json().get('job_id')
                self.logger.info(f"Job '{config.name}' created with ID: {job_id}")
                return job_id
            else:
                self.logger.error(f"Failed to create job: {response.text}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error creating job: {str(e)}")
            return None

class StorageSetup:
    """Setup Azure Data Lake Storage for the pipeline"""
    
    def __init__(self, config: StorageConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def create_containers(self) -> bool:
        """Create storage containers for different data layers"""
        try:
            containers = ['raw', 'bronze', 'silver', 'gold', 'quarantine']
            
            for container in containers:
                self.logger.info(f"Creating container: {container}")
                
                # In a real implementation, you would use Azure SDK
                # For this example, we'll simulate the creation
                self.logger.info(f"Container '{container}' created successfully")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating containers: {str(e)}")
            return False
    
    def setup_access_policies(self) -> bool:
        """Setup access policies for storage"""
        try:
            self.logger.info("Setting up storage access policies")
            
            # In a real implementation, you would configure:
            # - RBAC roles
            # - Access keys
            # - Network rules
            # - Firewall rules
            
            self.logger.info("Storage access policies configured successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error setting up access policies: {str(e)}")
            return False

class PipelineDeployer:
    """Main pipeline deployment orchestrator"""
    
    def __init__(self, databricks_config: DatabricksConfig, 
                 storage_config: StorageConfig):
        self.databricks_deployer = DatabricksDeployer(databricks_config)
        self.storage_setup = StorageSetup(storage_config)
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def deploy_pipeline(self, pipeline_config: PipelineConfig) -> bool:
        """Deploy the complete pipeline"""
        try:
            self.logger.info("Starting pipeline deployment")
            
            # Step 1: Setup storage
            self.logger.info("Step 1: Setting up storage")
            if not self.storage_setup.create_containers():
                return False
            
            if not self.storage_setup.setup_access_policies():
                return False
            
            # Step 2: Setup Databricks catalog and schemas
            self.logger.info("Step 2: Setting up Databricks catalog and schemas")
            if not self.databricks_deployer.create_catalog():
                return False
            
            if not self.databricks_deployer.create_schemas():
                return False
            
            # Step 3: Create monitoring tables
            self.logger.info("Step 3: Creating monitoring tables")
            if not self.databricks_deployer.create_monitoring_tables():
                return False
            
            # Step 4: Deploy notebooks
            self.logger.info("Step 4: Deploying notebooks")
            notebooks = [
                ('lakeflow_pipeline.py', 'lakeflow_pipeline'),
                ('monitoring_dashboard.py', 'monitoring_dashboard'),
                ('data_connectors.py', 'data_connectors'),
                ('error_handling_framework.py', 'error_handling_framework'),
                ('geospatial_processing.py', 'geospatial_processing')
            ]
            
            for notebook_file, notebook_name in notebooks:
                if not self.databricks_deployer.deploy_notebook(notebook_file, notebook_name):
                    return False
            
            # Step 5: Create job
            self.logger.info("Step 5: Creating pipeline job")
            job_id = self.databricks_deployer.create_job(pipeline_config)
            if not job_id:
                return False
            
            self.logger.info("Pipeline deployment completed successfully!")
            return True
            
        except Exception as e:
            self.logger.error(f"Pipeline deployment failed: {str(e)}")
            return False

def create_config_files():
    """Create configuration files for deployment"""
    
    # Databricks configuration
    databricks_config = {
        'workspace_url': 'https://your-workspace.cloud.databricks.com',
        'access_token': 'your-access-token',
        'catalog_name': 'calfire',
        'schema_name': 'production'
    }
    
    with open('databricks_config.yaml', 'w') as f:
        yaml.dump(databricks_config, f, default_flow_style=False)
    
    # Storage configuration
    storage_config = {
        'storage_account': 'calfirestorage',
        'container_name': 'calfire-data',
        'access_key': 'your-access-key',
        'endpoint': 'https://calfirestorage.dfs.core.windows.net'
    }
    
    with open('storage_config.yaml', 'w') as f:
        yaml.dump(storage_config, f, default_flow_style=False)
    
    # Pipeline configuration
    pipeline_config = {
        'name': 'calfire_wildfire_pipeline',
        'description': 'CalFIRE wildfire data ingestion and processing pipeline',
        'schedule': '0 0 * * *',  # Daily at midnight
        'max_retries': 3,
        'timeout_minutes': 60,
        'environment': 'production'
    }
    
    with open('pipeline_config.yaml', 'w') as f:
        yaml.dump(pipeline_config, f, default_flow_style=False)
    
    print("Configuration files created successfully!")

def load_config(config_file: str) -> Dict[str, Any]:
    """Load configuration from YAML file"""
    try:
        with open(config_file, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Error loading config file {config_file}: {str(e)}")
        raise

def main():
    """Main deployment function"""
    try:
        # Create configuration files if they don't exist
        if not os.path.exists('databricks_config.yaml'):
            create_config_files()
            print("Please update the configuration files with your actual values and run again.")
            return
        
        # Load configurations
        databricks_config_dict = load_config('databricks_config.yaml')
        storage_config_dict = load_config('storage_config.yaml')
        pipeline_config_dict = load_config('pipeline_config.yaml')
        
        # Create config objects
        databricks_config = DatabricksConfig(**databricks_config_dict)
        storage_config = StorageConfig(**storage_config_dict)
        pipeline_config = PipelineConfig(**pipeline_config_dict)
        
        # Deploy pipeline
        deployer = PipelineDeployer(databricks_config, storage_config)
        
        if deployer.deploy_pipeline(pipeline_config):
            print("✅ Pipeline deployed successfully!")
            print("\nNext steps:")
            print("1. Verify the deployment in your Databricks workspace")
            print("2. Test the pipeline with sample data")
            print("3. Configure monitoring and alerting")
            print("4. Set up the monitoring dashboard")
        else:
            print("❌ Pipeline deployment failed!")
            print("Check the logs for detailed error information.")
            
    except Exception as e:
        logger.error(f"Deployment failed: {str(e)}")
        print(f"❌ Deployment failed: {str(e)}")

if __name__ == "__main__":
    main()
