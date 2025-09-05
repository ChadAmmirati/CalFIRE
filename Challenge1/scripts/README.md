# Scripts - CalFIRE Data Pipeline

This directory contains deployment and utility scripts for the CalFIRE Data Ingestion Pipeline.

## 📁 Scripts Overview

```
scripts/
├── deploy.py                    # Main deployment script (recommended)
├── run_tests.py                 # Test runner script
├── sample_data_generator.py     # Sample data generation
├── setup_deployment.py          # Original deployment script
└── README.md                   # This file
```

## 🚀 Scripts Description

### deploy.py
**Purpose**: Main deployment script for the organized pipeline structure

**Features**:
- Loads configuration from organized config files
- Deploys complete pipeline to Databricks
- Validates deployment
- Provides deployment summary

**Usage**:
```bash
python scripts/deploy.py
# or
make deploy
```

### run_tests.py
**Purpose**: Comprehensive test runner for the pipeline

**Features**:
- Checks project structure
- Generates sample data
- Runs pipeline validation
- Provides test summary

**Usage**:
```bash
python scripts/run_tests.py
# or
make test
```

### sample_data_generator.py
**Purpose**: Generates realistic sample data for testing

**Features**:
- Creates fire perimeter data (GeoJSON)
- Generates damage inspection data (CSV)
- Creates streaming alert data (CSV)
- Provides data summary

**Usage**:
```bash
python scripts/sample_data_generator.py
# or
make generate-data
```

### setup_deployment.py
**Purpose**: Original deployment script (legacy)

**Features**:
- Complete pipeline deployment
- Databricks workspace setup
- Unity Catalog configuration
- Monitoring setup

**Usage**:
```bash
python scripts/setup_deployment.py
```

## 🔧 Quick Commands

### Using Makefile (Recommended)
```bash
make help          # Show all available commands
make setup         # Complete setup (install + generate data + validate)
make test          # Run all tests
make deploy        # Deploy pipeline
make generate-data # Generate sample data
make clean         # Clean up files
```

### Using Python Scripts Directly
```bash
# Run all tests
python scripts/run_tests.py

# Deploy pipeline
python scripts/deploy.py

# Generate sample data
python scripts/sample_data_generator.py
```

## 📊 Script Workflow

### 1. Setup Workflow
```bash
make setup
```
This runs:
1. `pip install -r config/requirements.txt`
2. `python scripts/sample_data_generator.py`
3. `python src/validation/pipeline_validation.py`

### 2. Testing Workflow
```bash
make test
```
This runs:
1. Structure validation
2. Sample data generation
3. Pipeline validation
4. Test summary

### 3. Deployment Workflow
```bash
make deploy
```
This runs:
1. Configuration loading
2. Pipeline deployment
3. Deployment validation
4. Deployment summary

## 🧪 Testing and Validation

### Test Structure
- **Structure Check**: Validates organized directory structure
- **Sample Data**: Generates realistic test data
- **Pipeline Validation**: Comprehensive component testing
- **Integration Tests**: End-to-end pipeline testing

### Validation Results
All scripts provide detailed output:
- ✅ Success indicators
- ❌ Error messages
- 📊 Summary statistics
- 🔍 Detailed logs

## 🔧 Configuration

### Script Configuration
Scripts automatically load configuration from:
- `config/databricks_config.yaml`
- `config/storage_config.yaml`
- `config/pipeline_config.yaml`

### Environment Variables
Scripts support environment variables:
```bash
export DATABRICKS_WORKSPACE_URL="https://your-workspace.cloud.databricks.com"
export DATABRICKS_ACCESS_TOKEN="your-access-token"
export AZURE_STORAGE_ACCOUNT="your-storage-account"
export AZURE_STORAGE_KEY="your-storage-key"
```

## 📚 Documentation

- **[User Guide](../docs/user_guides/README.md)** - Detailed setup instructions
- **[Configuration Guide](../config/README.md)** - Configuration file documentation
- **[Source Code Guide](../src/README.md)** - Source code organization
- **[Project Summary](../docs/PROJECT_SUMMARY.md)** - Complete project overview

## 🆘 Troubleshooting

### Common Issues

1. **Import Errors**
   - Ensure you're running from the Challenge1 root directory
   - Check that all dependencies are installed
   - Verify Python path configuration

2. **Configuration Errors**
   - Verify configuration files exist in `config/` directory
   - Check YAML syntax and required fields
   - Ensure credentials are properly set

3. **Deployment Errors**
   - Verify Databricks workspace access
   - Check Unity Catalog permissions
   - Ensure storage account configuration

### Debug Mode
Run scripts with verbose output:
```bash
python scripts/deploy.py --verbose
python scripts/run_tests.py --debug
```

### Logs
Check logs in:
- Databricks workspace (deployment logs)
- `data/output/validation_report.json` (validation results)
- Console output (script execution logs)

---

**Built with ❤️ for CalFIRE using the latest Databricks technologies**
