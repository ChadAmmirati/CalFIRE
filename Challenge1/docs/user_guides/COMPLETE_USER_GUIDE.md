# CalFIRE Data Pipeline - Complete User Guide

## 🎯 Welcome to the CalFIRE Data Ingestion Pipeline

This comprehensive guide will help you understand, use, modify, and troubleshoot the CalFIRE Data Ingestion Pipeline repository. Whether you're a developer, data engineer, system administrator, or evaluator, this guide provides everything you need to work with this repository effectively.

## 📋 Table of Contents

1. [Repository Overview](#repository-overview)
2. [Getting Started](#getting-started)
3. [Understanding the Code](#understanding-the-code)
4. [Configuration and Setup](#configuration-and-setup)
5. [Running the Pipeline](#running-the-pipeline)
6. [Modifying the Code](#modifying-the-code)
7. [Troubleshooting](#troubleshooting)
8. [Advanced Usage](#advanced-usage)
9. [Development Guidelines](#development-guidelines)
10. [Support and Resources](#support-and-resources)

---

## Repository Overview

### What is This Repository?

The CalFIRE Data Ingestion Pipeline is a comprehensive solution for ingesting, processing, and analyzing wildfire data using the latest Databricks technologies. It was built for the CalFIRE Data Sources and Ingestion Mechanisms challenge.

### Key Capabilities

- **Multi-Modal Data Ingestion**: Batch files, real-time APIs, and streaming data
- **Advanced Geospatial Processing**: Spatial joins, hotspot analysis, and trend detection
- **Comprehensive Monitoring**: Real-time dashboards and alerting
- **Robust Error Handling**: Fault tolerance and data quality assurance
- **Production Ready**: Automated deployment and configuration management

### Repository Structure

```
CalFIRE/Challenge1/
├── 📁 src/                    # Source code
│   ├── 📁 pipeline/           # Main pipeline components
│   │   └── lakeflow_pipeline.py  # Lakeflow Declarative Pipeline
│   ├── 📁 connectors/         # Data source connectors
│   │   └── data_connectors.py    # Source adapters and connectors
│   ├── 📁 processing/         # Data processing modules
│   │   ├── geospatial_processing.py  # Geospatial operations
│   │   └── error_handling_framework.py  # Error handling & validation
│   ├── 📁 monitoring/         # Monitoring and dashboards
│   │   └── monitoring_dashboard.py  # Streamlit monitoring dashboard
│   └── 📁 validation/         # Testing and validation
│       └── pipeline_validation.py  # Comprehensive validation script
├── 📁 config/                 # Configuration files
│   ├── databricks_config.yaml     # Databricks workspace config
│   ├── storage_config.yaml        # Azure storage config
│   ├── pipeline_config.yaml       # Pipeline configuration
│   └── requirements.txt           # Python dependencies
├── 📁 docs/                   # Documentation
│   ├── 📁 architecture/       # Architecture documentation
│   └── 📁 user_guides/        # User documentation
├── 📁 scripts/                # Deployment and utility scripts
│   ├── deploy.py              # Main deployment script
│   ├── run_tests.py           # Test runner script
│   └── sample_data_generator.py  # Sample data generation
├── 📁 data/                   # Data files (sample and output)
│   ├── 📁 sample/             # Sample data for testing
│   └── 📁 output/             # Output files
├── 📁 tests/                  # Test files
│   ├── 📁 unit/               # Unit tests
│   └── 📁 integration/        # Integration tests
├── PRODUCTION_DEPLOYMENT_GUIDE.md  # Production deployment guide
├── README.md                  # Main project README
└── Makefile                   # Easy command execution
```

---

## Getting Started

### Prerequisites

Before you begin, ensure you have:

1. **Python 3.8+** installed
2. **Git** for version control
3. **Databricks workspace** (for deployment)
4. **Azure Data Lake Storage** account (for data storage)
5. **Basic understanding** of Python and data engineering concepts

### Quick Start (5 Minutes)

1. **Clone and Navigate**
   ```bash
   git clone <repository-url>
   cd CalFIRE/Challenge1
   ```

2. **Configure Your Environment**
   ```bash
   # Edit configuration files with your credentials
   nano config/databricks_config.yaml
   nano config/storage_config.yaml
   ```

3. **Deploy to Production**
   ```bash
   # Run the deployment script
   python scripts/deploy.py
   ```
   This will:
   - Validate your configuration
   - Set up Azure storage containers
   - Create Databricks compute resources (serverless by default)
   - Deploy the Lakeflow Declarative Pipeline
   - Create monitoring dashboards
   - Run initial data load

4. **Verify Everything Works**
   ```bash
   make test
   ```

5. **View Results**
   ```bash
   # Check validation results
   cat data/output/validation_report.json
   
   # View sample data
   ls -la data/sample/
   
   # Access your deployed pipeline
   # You'll receive URLs to:
   # - Databricks workspace
   # - Pipeline workflows
   # - Monitoring dashboard
   ```

### What Just Happened?

The deployment process:
- ✅ Validated your configuration
- ✅ Set up Azure storage containers
- ✅ Created Databricks compute resources (serverless by default)
- ✅ Set up Unity Catalog with bronze/silver/gold schemas
- ✅ Deployed the Lakeflow Declarative Pipeline
- ✅ Created monitoring dashboards
- ✅ Ran initial data load with real CalFIRE data
- ✅ Validated the entire deployment

---

## Understanding the Code

### Core Components

#### 1. **Pipeline Engine** (`src/pipeline/lakeflow_pipeline.py`)
The main pipeline that orchestrates data flow through Bronze → Silver → Gold layers.

**Key Concepts**:
- **Bronze Layer**: Raw data ingestion with schema inference
- **Silver Layer**: Data cleaning, validation, and enrichment
- **Gold Layer**: Curated analytics tables for dashboards

**How to Read It**:
```python
# Look for these key sections:
pipeline = Pipeline(name="calfire_wildfire_data_pipeline")  # Pipeline definition
pipeline.add_source(batch_source)                          # Data sources
pipeline.add_transform(fire_perimeters_clean)              # Data transformations
pipeline.add_monitoring(pipeline_metrics)                  # Monitoring setup
```

#### 2. **Data Connectors** (`src/connectors/data_connectors.py`)
Handles different data sources (files, APIs, streaming).

**Key Concepts**:
- **Batch Connector**: Processes GeoJSON, CSV, KML files
- **API Connector**: Fetches data from ArcGIS REST APIs
- **Streaming Connector**: Handles real-time data streams

**How to Read It**:
```python
# Each connector follows this pattern:
class FirePerimetersBatchConnector(DataConnector):
    def connect(self) -> bool:      # Establish connection
    def extract(self) -> DataFrame: # Get data
    def validate(self) -> bool:     # Check data quality
```

#### 3. **Geospatial Processing** (`src/processing/geospatial_processing.py`)
Advanced spatial operations using Mosaic and H3 libraries.

**Key Concepts**:
- **H3 Indexing**: High-performance spatial indexing
- **Spatial Joins**: Efficient geospatial operations
- **Hotspot Analysis**: Identification of high-activity areas

**How to Read It**:
```python
# Main processing flow:
processor = GeospatialProcessor(config)
enriched_data = processor.enrich_fire_perimeters(fire_df, damage_df)
aggregated_data = processor.create_spatial_aggregations(enriched_data)
```

#### 4. **Error Handling** (`src/processing/error_handling_framework.py`)
Comprehensive error handling and data quality assurance.

**Key Concepts**:
- **Validation Rules**: Data quality constraints
- **Retry Logic**: Exponential backoff for failed operations
- **Quarantine**: Automatic isolation of problematic data

**How to Read It**:
```python
# Error handling flow:
validator = DataQualityValidator(rules)
results = validator.validate_dataframe(df, "source_name")
error_handler = ErrorHandler()
error_record = error_handler.handle_error(exception, context)
```

#### 5. **Monitoring Dashboard** (`src/monitoring/monitoring_dashboard.py`)
Real-time monitoring interface built with Streamlit.

**Key Concepts**:
- **Real-time Metrics**: Pipeline performance tracking
- **Data Quality Monitoring**: Quality score visualization
- **Error Analysis**: Detailed error tracking and alerting

### Code Organization Principles

1. **Separation of Concerns**: Each module has a specific responsibility
2. **Modularity**: Components can be used independently
3. **Extensibility**: Easy to add new data sources or processing steps
4. **Testability**: Comprehensive testing framework included

---

## Configuration and Setup

### Configuration Files

All configuration is centralized in the `config/` directory:

#### 1. **Databricks Configuration** (`config/databricks_config.yaml`)
```yaml
databricks:
  workspace_url: "https://your-workspace.cloud.databricks.com"
  access_token: "your-access-token"
  catalog_name: "calfire"
  schema_name: "production"
  
  # Serverless Configuration (Default)
  compute_type: "serverless"
  sql_warehouse_id: "auto-created"
  sql_warehouse_name: "calfire-serverless-warehouse"
```

**What to Change**:
- `workspace_url`: Your Databricks workspace URL
- `access_token`: Your Databricks personal access token
- `catalog_name`: Unity Catalog catalog name
- `schema_name`: Default schema for tables

#### 2. **Storage Configuration** (`config/storage_config.yaml`)
```yaml
storage:
  account_name: "your-storage-account"
  container_name: "calfire-data"
  access_key: "your-access-key"
  endpoint: "https://your-storage-account.dfs.core.windows.net"
  
  # Alternative: Use Managed Identity (Recommended)
  use_managed_identity: true
  managed_identity_client_id: "your-managed-identity-client-id"
```

**What to Change**:
- `account_name`: Your Azure storage account name
- `access_key`: Your storage account access key
- `container_name`: Container for data storage

#### 3. **Pipeline Configuration** (`config/pipeline_config.yaml`)
```yaml
pipeline:
  name: "calfire_wildfire_pipeline"
  description: "CalFIRE wildfire data ingestion pipeline"
  version: "2.0.0"
  environment: "production"
  
  # Scheduling
  schedule:
    batch_processing: "0 0 * * *"  # Daily at midnight
    api_processing: "0 */6 * * *"  # Every 6 hours
    
  # Performance
  performance:
    max_retries: 3
    timeout_minutes: 120
    batch_size: 10000
```

**What to Change**:
- `schedule`: Cron expressions for pipeline execution
- `max_retries`: Number of retry attempts for failed operations
- `timeout_minutes`: Maximum execution time
- `batch_size`: Number of records to process per batch

### Environment Setup

#### Option 1: Production Deployment (Recommended)
```bash
# 1. Install dependencies
pip install -r config/requirements.txt

# 2. Configure environment
# Edit config files with your credentials

# 3. Deploy to production
python scripts/deploy.py
```

#### Option 2: Development Setup
```bash
# 1. Install dependencies
pip install -r config/requirements.txt

# 2. Generate sample data
make generate-data

# 3. Run validation
make validate
```

### Security Considerations

**Never commit sensitive information**:
- Use environment variables for credentials
- Add `config/*.yaml` to `.gitignore` if they contain secrets
- Consider using Azure Key Vault for production deployments

**Environment Variables**:
```bash
export DATABRICKS_WORKSPACE_URL="https://your-workspace.cloud.databricks.com"
export DATABRICKS_ACCESS_TOKEN="your-access-token"
export AZURE_STORAGE_ACCOUNT="your-storage-account"
export AZURE_STORAGE_KEY="your-storage-key"
```

---

## Running the Pipeline

### Available Commands

The repository includes a Makefile for easy command execution:

```bash
make help          # Show all available commands
make install       # Install Python dependencies
make test          # Run all tests
make validate      # Run validation only
make deploy        # Deploy production pipeline
make generate-data # Generate sample data
make monitor       # Start monitoring dashboard
make clean         # Clean up files
```

### Running Tests

#### Run All Tests
```bash
make test
```

#### Run Specific Tests
```bash
# Structure validation only
python3 scripts/run_tests.py

# Pipeline validation only
make validate

# Generate sample data only
make generate-data
```

### Deploying the Pipeline

#### Deploy to Production
```bash
make deploy
```

This will:
1. Validate your configuration
2. Set up Azure storage containers
3. Create Databricks compute resources (serverless by default)
4. Set up Unity Catalog with bronze/silver/gold schemas
5. Deploy the Lakeflow Declarative Pipeline
6. Create monitoring dashboards
7. Run initial data load
8. Validate the deployment

#### Manual Deployment Steps
```bash
# 1. Configure your environment
# Edit config/databricks_config.yaml with your credentials
# Edit config/storage_config.yaml with your Azure details

# 2. Deploy using script
python3 scripts/deploy.py

# 3. Verify deployment
# Check your Databricks workspace for:
# - New catalog: calfire
# - Schemas: bronze, silver, gold, monitoring, quarantine
# - Lakeflow Declarative Pipeline: calfire_wildfire_data_pipeline
# - Serverless SQL Warehouse: calfire-serverless-warehouse
```

### Monitoring the Pipeline

#### Access Monitoring Dashboard
```bash
# Start Streamlit dashboard
make monitor
# or
streamlit run src/monitoring/monitoring_dashboard.py
```

#### View Pipeline Logs
- **Databricks Workspace**: Go to Jobs → Your Pipeline → View Logs
- **Local Logs**: Check console output during execution
- **Validation Report**: `data/output/validation_report.json`

---

## Modifying the Code

### Adding New Data Sources

#### 1. Create New Connector
```python
# In src/connectors/data_connectors.py
class NewDataSourceConnector(DataConnector):
    def __init__(self, config: DataSourceConfig):
        super().__init__(config)
    
    def connect(self) -> bool:
        # Implement connection logic
        pass
    
    def extract(self, **kwargs) -> pd.DataFrame:
        # Implement data extraction
        pass
    
    def validate(self, data: pd.DataFrame) -> bool:
        # Implement data validation
        pass
```

#### 2. Register Connector
```python
# In DataConnectorFactory
@staticmethod
def create_connector(config: DataSourceConfig) -> DataConnector:
    if config.source_type.lower() == 'new_source':
        return NewDataSourceConnector(config)
    # ... existing code
```

#### 3. Add to Pipeline
```python
# In src/pipeline/lakeflow_pipeline.py
new_source = NewDataSource(
    name="new_data_source",
    source_type="new_source",
    endpoint="your-endpoint"
)
pipeline.add_source(new_source)
```

### Adding New Data Processing Steps

#### 1. Create Processing Function
```python
# In src/processing/geospatial_processing.py
def new_processing_step(self, df: pd.DataFrame) -> pd.DataFrame:
    """Add your processing logic here"""
    # Example: Add new calculated field
    df['new_field'] = df['existing_field'] * 2
    return df
```

#### 2. Add to Pipeline
```python
# In src/pipeline/lakeflow_pipeline.py
new_transform = Transform(
    name="new_processing_step",
    source_table="calfire.silver.fire_perimeters",
    transformations=[
        # Your transformations here
    ]
)
pipeline.add_transform(new_transform)
```

### Modifying Validation Rules

#### 1. Add New Validation Rule
```python
# In src/processing/error_handling_framework.py
new_rule = ValidationRule(
    name="new_validation_rule",
    description="Validate new field",
    rule_expression="new_field > 0",
    severity=ErrorSeverity.MEDIUM,
    action=ErrorAction.QUARANTINE
)
```

#### 2. Add to Validator
```python
# In your processing code
validator = DataQualityValidator([new_rule])
results = validator.validate_dataframe(df, "source_name")
```

### Adding New Monitoring Metrics

#### 1. Add Metric to Dashboard
```python
# In src/monitoring/monitoring_dashboard.py
def get_new_metric(self, hours_back: int = 24) -> pd.DataFrame:
    """Retrieve new metric data"""
    query = """
    SELECT 
        metric_name,
        metric_value,
        timestamp
    FROM calfire.monitoring.new_metrics
    WHERE timestamp >= current_timestamp() - INTERVAL {hours_back} HOURS
    """
    # Implementation here
```

#### 2. Add to Dashboard UI
```python
# In the dashboard function
st.subheader("New Metric")
fig = px.line(new_metric_data, x='timestamp', y='metric_value')
st.plotly_chart(fig, use_container_width=True)
```

### Best Practices for Modifications

1. **Test Your Changes**
   ```bash
   make test  # Run full test suite
   ```

2. **Update Documentation**
   - Update relevant README files
   - Add comments to your code
   - Update configuration examples

3. **Follow Code Standards**
   - Use type hints
   - Add docstrings
   - Follow PEP 8 style guide
   - Add error handling

4. **Version Control**
   ```bash
   git add .
   git commit -m "Add new data source connector"
   git push
   ```

---

## 🔍 Troubleshooting

### Common Issues and Solutions

#### 1. **Import Errors**
**Problem**: `ModuleNotFoundError: No module named 'pandas'`

**Solution**:
```bash
# Install dependencies
pip install -r config/requirements.txt

# Or use the Makefile
make install
```

#### 2. **Configuration Errors**
**Problem**: `KeyError: 'workspace_url'`

**Solution**:
```bash
# Check configuration files exist
ls -la config/

# Verify YAML syntax
python3 -c "import yaml; yaml.safe_load(open('config/databricks_config.yaml'))"

# Update configuration with your values
```

#### 3. **Connection Errors**
**Problem**: `ConnectionError: Failed to connect to Databricks`

**Solution**:
```bash
# Verify workspace URL format
# Should be: https://your-workspace.cloud.databricks.com

# Check access token validity
# Generate new token in Databricks workspace

# Test connection
databricks configure --token
databricks workspace ls
```

#### 4. **Data Validation Failures**
**Problem**: `Data validation failed for X records`

**Solution**:
```bash
# Check validation report
cat data/output/validation_report.json

# Review data quality rules
# In src/processing/error_handling_framework.py

# Adjust validation thresholds if needed
# In config/pipeline_config.yaml
```

#### 5. **Performance Issues**
**Problem**: Pipeline runs slowly or times out

**Solution**:
```bash
# Check cluster configuration
# In config/databricks_config.yaml

# Adjust batch sizes
# In config/pipeline_config.yaml

# Monitor resource usage
# In Databricks workspace → Clusters
```

### Debug Mode

#### Enable Verbose Logging
```python
# In your code
import logging
logging.basicConfig(level=logging.DEBUG)
```

#### Run with Debug Output
```bash
# Add debug flags to scripts
python3 scripts/deploy.py --verbose
python3 scripts/run_tests.py --debug
```

### Getting Help

#### 1. **Check Logs**
```bash
# Local logs
tail -f logs/pipeline.log

# Databricks logs
# Go to workspace → Jobs → Your Pipeline → View Logs
```

#### 2. **Review Documentation**
- [Architecture Design](docs/architecture/architecture_design.md)
- [User Guide](docs/user_guides/README.md)
- [Configuration Guide](config/README.md)

#### 3. **Run Diagnostics**
```bash
# Check system status
make test

# Validate configuration
python3 -c "from src.validation.pipeline_validation import PipelineValidator; PipelineValidator().validate_all_components()"
```

#### 4. **Common Commands for Debugging**
```bash
# Check Python environment
python3 --version
pip list

# Check file permissions
ls -la config/
ls -la data/

# Check disk space
df -h

# Check network connectivity
ping your-workspace.cloud.databricks.com
```

---

## Advanced Usage

### Custom Data Sources

#### Adding Custom File Formats
```python
# Extend the batch connector
class CustomFileConnector(FirePerimetersBatchConnector):
    def _extract_custom_format(self) -> pd.DataFrame:
        """Handle custom file format"""
        # Your custom parsing logic
        pass
```

#### Adding Custom APIs
```python
# Extend the API connector
class CustomAPIConnector(ArcGISAPIConnector):
    def extract(self, **kwargs) -> pd.DataFrame:
        """Custom API extraction logic"""
        # Your custom API logic
        pass
```

### Performance Optimization

#### Tuning Pipeline Performance
```yaml
# In config/pipeline_config.yaml
performance:
  batch_size: 5000        # Increase for better throughput
  parallel_jobs: 8        # Increase for parallel processing
  timeout_minutes: 120    # Increase for large datasets
```

#### Optimizing Geospatial Operations
```python
# In src/processing/geospatial_processing.py
config = GeospatialConfig(
    h3_resolution=6,  # Lower resolution for faster processing
    buffer_distance_meters=500.0  # Smaller buffer for faster joins
)
```

### Custom Monitoring

#### Adding Custom Metrics
```python
# In src/monitoring/monitoring_dashboard.py
def get_custom_metrics(self) -> Dict[str, Any]:
    """Add your custom metrics"""
    return {
        'custom_metric_1': self.calculate_metric_1(),
        'custom_metric_2': self.calculate_metric_2()
    }
```

#### Custom Alerting
```python
# In src/processing/error_handling_framework.py
custom_alert = {
    "name": "custom_alert",
    "condition": "custom_metric > threshold",
    "severity": "warning",
    "notification": "slack:#custom-channel"
}
```

### Integration with External Systems

#### Webhook Integration
```python
# Add webhook notifications
import requests

def send_webhook_notification(message: str):
    webhook_url = "https://your-webhook-url"
    payload = {"text": message}
    requests.post(webhook_url, json=payload)
```

#### Database Integration
```python
# Add database connectivity
import sqlalchemy

def save_to_database(df: pd.DataFrame, table_name: str):
    engine = sqlalchemy.create_engine("your-database-url")
    df.to_sql(table_name, engine, if_exists='append')
```

---

## Development Guidelines

### Code Standards

#### Python Style Guide
```python
# Use type hints
def process_data(df: pd.DataFrame) -> pd.DataFrame:
    """Process data with proper documentation."""
    return df

# Follow PEP 8
class DataProcessor:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
    
    def process(self) -> None:
        """Process data according to configuration."""
        pass
```

#### Error Handling
```python
# Always handle exceptions
try:
    result = risky_operation()
except SpecificException as e:
    logger.error(f"Specific error occurred: {e}")
    # Handle specific case
except Exception as e:
    logger.error(f"Unexpected error: {e}")
    # Handle general case
```

#### Logging
```python
import logging

logger = logging.getLogger(__name__)

def your_function():
    logger.info("Starting operation")
    try:
        # Your code here
        logger.info("Operation completed successfully")
    except Exception as e:
        logger.error(f"Operation failed: {e}")
        raise
```

### Testing Guidelines

#### Unit Tests
```python
# In tests/unit/test_data_connectors.py
import unittest
from src.connectors.data_connectors import FirePerimetersBatchConnector

class TestDataConnectors(unittest.TestCase):
    def test_batch_connector_validation(self):
        """Test batch connector data validation."""
        connector = FirePerimetersBatchConnector(config)
        # Your test logic here
        self.assertTrue(connector.validate(test_data))
```

#### Integration Tests
```python
# In tests/integration/test_pipeline.py
def test_end_to_end_pipeline():
    """Test complete pipeline execution."""
    # Setup test data
    # Run pipeline
    # Verify results
    pass
```

### Documentation Standards

#### Code Documentation
```python
def complex_function(param1: str, param2: int) -> Dict[str, Any]:
    """
    Perform complex data processing operation.
    
    Args:
        param1: Description of parameter 1
        param2: Description of parameter 2
    
    Returns:
        Dictionary containing processed results
    
    Raises:
        ValueError: If parameters are invalid
        ConnectionError: If external service is unavailable
    
    Example:
        >>> result = complex_function("test", 42)
        >>> print(result['status'])
        'success'
    """
    pass
```

#### README Updates
When adding new features:
1. Update relevant README files
2. Add usage examples
3. Update configuration documentation
4. Add troubleshooting information

### Version Control Best Practices

#### Commit Messages
```bash
# Good commit messages
git commit -m "Add new data source connector for weather API"
git commit -m "Fix validation rule for coordinate bounds"
git commit -m "Update documentation for new features"

# Bad commit messages
git commit -m "fix"
git commit -m "updates"
git commit -m "stuff"
```

#### Branch Strategy
```bash
# Create feature branches
git checkout -b feature/new-data-source
git checkout -b bugfix/validation-error
git checkout -b docs/update-readme

# Merge back to main
git checkout main
git merge feature/new-data-source
```

---

## Support and Resources

### Getting Help

#### 1. **Documentation Resources**
- [Main README](README.md) - Project overview
- [User Guide](docs/user_guides/README.md) - Detailed instructions
- [Architecture Design](docs/architecture/architecture_design.md) - System design
- [Configuration Guide](config/README.md) - Setup instructions
- [Scripts Guide](scripts/README.md) - Deployment help

#### 2. **Validation and Testing**
```bash
# Run comprehensive tests
make test

# Check validation report
cat data/output/validation_report.json

# View sample data
ls -la data/sample/
```

#### 3. **Common Commands Reference**
```bash
# Setup and installation
make setup          # Complete setup
make install        # Install dependencies only
make clean          # Clean up files

# Testing and validation
make test           # Run all tests
make validate       # Run validation only
make generate-data  # Generate sample data

# Deployment
make deploy         # Deploy to Databricks

# Help
make help           # Show all commands
```

### Troubleshooting Checklist

#### Before Asking for Help
1. ✅ **Check Prerequisites**: Python 3.8+, required packages installed
2. ✅ **Verify Configuration**: All config files properly set up
3. ✅ **Run Tests**: `make test` passes without errors
4. ✅ **Check Logs**: Review error messages and logs
5. ✅ **Review Documentation**: Check relevant README files
6. ✅ **Try Clean Setup**: `make clean && make setup`

#### When Reporting Issues
Include the following information:
- **Error Message**: Complete error text
- **Steps to Reproduce**: What you did before the error
- **Environment**: Python version, OS, Databricks version
- **Configuration**: Relevant config file contents (remove secrets)
- **Logs**: Relevant log output

### Additional Resources

#### Databricks Documentation
- [Databricks Lakeflow](https://docs.databricks.com/en/lakeflow/index.html)
- [Unity Catalog](https://docs.databricks.com/en/unity-catalog/index.html)
- [Delta Lake](https://docs.databricks.com/en/delta/index.html)

#### Python Libraries
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [Streamlit Documentation](https://docs.streamlit.io/)
- [Plotly Documentation](https://plotly.com/python/)

#### Geospatial Libraries
- [H3 Documentation](https://h3geo.org/docs/)
- [GeoPandas Documentation](https://geopandas.org/)

---

## Conclusion

This CalFIRE Data Ingestion Pipeline repository provides a comprehensive, production-ready solution for wildfire data processing. With this guide, you should be able to:

- ✅ **Understand** the repository structure and components
- ✅ **Set up** the pipeline in your environment
- ✅ **Run** tests and validate functionality
- ✅ **Deploy** to Databricks workspace
- ✅ **Modify** code to meet your specific needs
- ✅ **Troubleshoot** common issues
- ✅ **Extend** functionality with new features

### Next Steps

1. **Start with Quick Setup**: Run `make setup` to get everything working
2. **Explore the Code**: Read through the source code to understand the implementation
3. **Run Tests**: Use `make test` to validate everything is working
4. **Customize Configuration**: Update config files for your environment
5. **Deploy**: Use `make deploy` to deploy to your Databricks workspace
6. **Monitor**: Set up monitoring and alerting for your pipeline

### Remember

- **This is a complete, working solution** - all components are functional and tested
- **Documentation is comprehensive** - check README files for specific topics
- **Support is built-in** - use validation and testing tools to diagnose issues
- **Extension is encouraged** - the modular design makes it easy to add new features

**Happy coding! 🚀**

---

*Built with ❤️ for CalFIRE using the latest Databricks technologies*
