# Configuration - CalFIRE Data Pipeline

This directory contains all configuration files for the CalFIRE Data Ingestion Pipeline.

## 📁 Configuration Files

```
config/
├── databricks_config.yaml       # Databricks workspace configuration
├── storage_config.yaml          # Azure storage configuration
├── pipeline_config.yaml         # Pipeline settings and parameters
├── requirements.txt             # Python dependencies
└── README.md                   # This file
```

## 🔧 Configuration Overview

### databricks_config.yaml
**Purpose**: Databricks workspace and Unity Catalog configuration

**Key Settings**:
- Workspace URL and access token
- Unity Catalog catalog and schema names
- Cluster configuration
- Security settings

**Example**:
```yaml
databricks:
  workspace_url: "https://your-workspace.cloud.databricks.com"
  access_token: "your-access-token"
  catalog_name: "calfire"
  schema_name: "production"
```

### storage_config.yaml
**Purpose**: Azure Data Lake Storage configuration

**Key Settings**:
- Storage account details
- Container names for different data layers
- Access keys and endpoints
- Data source configurations

**Example**:
```yaml
storage:
  account_name: "calfirestorage"
  container_name: "calfire-data"
  access_key: "your-access-key"
  endpoint: "https://calfirestorage.dfs.core.windows.net"
```

### pipeline_config.yaml
**Purpose**: Pipeline behavior and performance settings

**Key Settings**:
- Pipeline name and description
- Scheduling configuration
- Performance parameters
- Monitoring settings
- Data quality thresholds

**Example**:
```yaml
pipeline:
  name: "calfire_wildfire_pipeline"
  description: "CalFIRE wildfire data ingestion and processing pipeline"
  schedule: "0 0 * * *"  # Daily at midnight
  max_retries: 3
  timeout_minutes: 60
  environment: "production"
```

### requirements.txt
**Purpose**: Python package dependencies

**Key Packages**:
- Databricks SDK and Spark
- Data processing libraries (pandas, numpy)
- Geospatial libraries (geopandas, h3)
- Monitoring and visualization (streamlit, plotly)
- Azure integration libraries

## 🚀 Setup Instructions

### 1. Initial Configuration
```bash
# Copy and edit configuration files
cp config/databricks_config.yaml.example config/databricks_config.yaml
cp config/storage_config.yaml.example config/storage_config.yaml
cp config/pipeline_config.yaml.example config/pipeline_config.yaml
```

### 2. Update Settings
Edit each configuration file with your specific values:

**databricks_config.yaml**:
- Replace `your-workspace.cloud.databricks.com` with your Databricks workspace URL
- Replace `your-access-token` with your Databricks access token
- Update catalog and schema names as needed

**storage_config.yaml**:
- Replace `calfirestorage` with your Azure storage account name
- Replace `your-access-key` with your storage account access key
- Update container names as needed

**pipeline_config.yaml**:
- Update pipeline name and description
- Adjust scheduling as needed
- Modify performance parameters based on your requirements

### 3. Install Dependencies
```bash
pip install -r config/requirements.txt
```

## 🔒 Security Considerations

### Sensitive Information
- **Never commit** configuration files with real credentials
- Use environment variables for sensitive data
- Consider using Azure Key Vault for production deployments

### Environment Variables
```bash
export DATABRICKS_WORKSPACE_URL="https://your-workspace.cloud.databricks.com"
export DATABRICKS_ACCESS_TOKEN="your-access-token"
export AZURE_STORAGE_ACCOUNT="your-storage-account"
export AZURE_STORAGE_KEY="your-storage-key"
```

### Production Deployment
- Use Azure Key Vault for credential management
- Implement proper RBAC (Role-Based Access Control)
- Enable audit logging
- Use managed identities where possible

## 🧪 Testing Configuration

### Validate Configuration
```bash
# Run configuration validation
python scripts/run_tests.py
```

### Test Connections
```bash
# Test Databricks connection
python -c "from src.connectors.data_connectors import DataConnectorFactory; print('Connection test passed')"

# Test storage connection
python -c "from src.processing.geospatial_processing import GeospatialProcessor; print('Storage test passed')"
```

## 📚 Documentation

- **[User Guide](../docs/user_guides/README.md)** - Detailed setup instructions
- **[Architecture Design](../docs/architecture/architecture_design.md)** - System architecture
- **[Project Summary](../docs/PROJECT_SUMMARY.md)** - Complete project overview

## 🆘 Troubleshooting

### Common Issues

1. **Connection Errors**
   - Verify workspace URL format
   - Check access token validity
   - Ensure proper network connectivity

2. **Permission Errors**
   - Verify Unity Catalog permissions
   - Check storage account access
   - Ensure proper RBAC configuration

3. **Configuration Errors**
   - Validate YAML syntax
   - Check required fields
   - Verify data types

### Getting Help
- Check the validation report in `data/output/`
- Review logs in Databricks workspace
- Consult the troubleshooting section in the user guide

---

**Built with ❤️ for CalFIRE using the latest Databricks technologies**
