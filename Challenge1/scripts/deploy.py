#!/usr/bin/env python3
"""
CalFIRE Pipeline Deployment Script
Deploys the organized pipeline structure to Databricks
"""

import os
import sys
import yaml
import json
from pathlib import Path

# Add src directory to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

def load_config(config_file: str) -> dict:
    """Load configuration from YAML file"""
    config_path = Path(__file__).parent.parent / "config" / config_file
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def deploy_pipeline():
    """Deploy the complete pipeline"""
    print("🔥 CalFIRE Pipeline Deployment")
    print("=" * 50)
    
    try:
        # Load configurations
        print("📋 Loading configurations...")
        databricks_config = load_config("databricks_config.yaml")
        storage_config = load_config("storage_config.yaml")
        pipeline_config = load_config("pipeline_config.yaml")
        
        print("✅ Configurations loaded successfully")
        
        # Import deployment modules
        print("📦 Importing deployment modules...")
        from setup_deployment import PipelineDeployer, DatabricksConfig, StorageConfig, PipelineConfig
        
        # Create config objects
        db_config = DatabricksConfig(
            workspace_url=databricks_config['databricks']['workspace_url'],
            access_token=databricks_config['databricks']['access_token'],
            catalog_name=databricks_config['databricks']['catalog_name'],
            schema_name=databricks_config['databricks']['schema_name']
        )
        
        storage_cfg = StorageConfig(
            storage_account=storage_config['storage']['account_name'],
            container_name=storage_config['storage']['container_name'],
            access_key=storage_config['storage']['access_key'],
            endpoint=storage_config['storage']['endpoint']
        )
        
        pipeline_cfg = PipelineConfig(
            name=pipeline_config['pipeline']['name'],
            description=pipeline_config['pipeline']['description'],
            schedule=pipeline_config['schedule']['batch_processing'],
            max_retries=pipeline_config['performance']['max_retries'],
            timeout_minutes=pipeline_config['performance']['timeout_minutes'],
            environment=pipeline_config['pipeline']['environment']
        )
        
        print("✅ Configuration objects created")
        
        # Deploy pipeline
        print("🚀 Starting pipeline deployment...")
        deployer = PipelineDeployer(db_config, storage_cfg)
        
        if deployer.deploy_pipeline(pipeline_cfg):
            print("✅ Pipeline deployed successfully!")
            print("\n📊 Deployment Summary:")
            print(f"  - Pipeline Name: {pipeline_cfg.name}")
            print(f"  - Environment: {pipeline_cfg.environment}")
            print(f"  - Schedule: {pipeline_cfg.schedule}")
            print(f"  - Catalog: {db_config.catalog_name}")
            print(f"  - Storage: {storage_cfg.storage_account}")
            
            print("\n🎯 Next Steps:")
            print("  1. Verify deployment in Databricks workspace")
            print("  2. Test pipeline with sample data")
            print("  3. Configure monitoring and alerting")
            print("  4. Set up the monitoring dashboard")
            
            return True
        else:
            print("❌ Pipeline deployment failed!")
            return False
            
    except Exception as e:
        print(f"❌ Deployment error: {str(e)}")
        return False

def validate_deployment():
    """Validate the deployment"""
    print("\n🧪 Validating deployment...")
    
    try:
        # Import validation module
        sys.path.append(str(Path(__file__).parent.parent / "src" / "validation"))
        from pipeline_validation import PipelineValidator
        
        validator = PipelineValidator()
        results = validator.validate_all_components()
        
        if results['overall_status'] == 'PASSED':
            print("✅ Deployment validation PASSED!")
            return True
        else:
            print("❌ Deployment validation FAILED!")
            return False
            
    except Exception as e:
        print(f"❌ Validation error: {str(e)}")
        return False

def main():
    """Main deployment function"""
    print("Starting CalFIRE Pipeline Deployment...")
    
    # Check if we're in the right directory
    if not Path("config").exists():
        print("❌ Error: Please run this script from the Challenge1 root directory")
        print("   Expected structure: CalFIRE/Challenge1/")
        return 1
    
    # Deploy pipeline
    if deploy_pipeline():
        # Validate deployment
        if validate_deployment():
            print("\n🎉 Deployment completed successfully!")
            print("   The CalFIRE pipeline is ready for use.")
            return 0
        else:
            print("\n⚠️  Deployment completed but validation failed.")
            print("   Please check the logs and fix any issues.")
            return 1
    else:
        print("\n❌ Deployment failed!")
        return 2

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
