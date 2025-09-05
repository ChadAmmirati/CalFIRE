# CalFIRE Pipeline - Quick Reference Guide

## 🚀 Essential Commands

### Setup and Installation
```bash
# Complete setup (recommended for first time)
make setup

# Install dependencies only
make install

# Generate sample data
make generate-data
```

### Testing and Validation
```bash
# Run all tests
make test

# Run validation only
make validate

# Check test results
cat data/output/validation_report.json
```

### Deployment
```bash
# Deploy to Databricks
make deploy

# Clean up files
make clean

# Show all available commands
make help
```

## 📁 Key File Locations

### Configuration Files
```
config/databricks_config.yaml    # Databricks workspace settings
config/storage_config.yaml       # Azure storage settings
config/pipeline_config.yaml      # Pipeline behavior settings
config/requirements.txt          # Python dependencies
```

### Source Code
```
src/pipeline/lakeflow_pipeline.py           # Main pipeline
src/connectors/data_connectors.py           # Data source connectors
src/processing/geospatial_processing.py     # Spatial operations
src/processing/error_handling_framework.py  # Error handling
src/monitoring/monitoring_dashboard.py      # Monitoring dashboard
src/validation/pipeline_validation.py       # Validation tests
```

### Data Files
```
data/sample/                     # Sample data for testing
data/output/validation_report.json  # Test results
```

### Documentation
```
README.md                        # Main project overview
COMPLETE_USER_GUIDE.md          # Comprehensive user guide
docs/user_guides/README.md       # Detailed setup instructions
docs/architecture/architecture_design.md  # System architecture
```

## ⚙️ Configuration Quick Setup

### 1. Databricks Configuration
Edit `config/databricks_config.yaml`:
```yaml
databricks:
  workspace_url: "https://your-workspace.cloud.databricks.com"
  access_token: "your-access-token"
  catalog_name: "calfire"
```

### 2. Storage Configuration
Edit `config/storage_config.yaml`:
```yaml
storage:
  account_name: "your-storage-account"
  access_key: "your-access-key"
  endpoint: "https://your-storage.dfs.core.windows.net"
```

### 3. Pipeline Configuration
Edit `config/pipeline_config.yaml`:
```yaml
pipeline:
  name: "calfire_wildfire_pipeline"
  schedule: "0 0 * * *"  # Daily at midnight
  max_retries: 3
```

## 🔧 Common Modifications

### Adding New Data Source
1. Create connector in `src/connectors/data_connectors.py`
2. Add to `DataConnectorFactory`
3. Update pipeline in `src/pipeline/lakeflow_pipeline.py`

### Adding New Validation Rule
1. Define rule in `src/processing/error_handling_framework.py`
2. Add to validator in your processing code
3. Test with `make validate`

### Adding New Monitoring Metric
1. Add metric function in `src/monitoring/monitoring_dashboard.py`
2. Add to dashboard UI
3. Test with `make test`

## 🐛 Troubleshooting

### Common Issues

#### Import Errors
```bash
# Solution: Install dependencies
make install
```

#### Configuration Errors
```bash
# Solution: Check config files
ls -la config/
python3 -c "import yaml; yaml.safe_load(open('config/databricks_config.yaml'))"
```

#### Connection Errors
```bash
# Solution: Verify credentials
databricks configure --token
databricks workspace ls
```

#### Validation Failures
```bash
# Solution: Check validation report
cat data/output/validation_report.json
```

### Debug Commands
```bash
# Run with verbose output
python3 scripts/deploy.py --verbose

# Check Python environment
python3 --version
pip list

# Check file permissions
ls -la config/
ls -la data/
```

## 📊 Understanding Output

### Validation Report Structure
```json
{
  "overall_status": "PASSED",
  "component_results": {
    "data_connectors": {"status": "PASSED"},
    "error_handling": {"status": "PASSED"},
    "geospatial_processing": {"status": "PASSED"},
    "data_quality": {"status": "PASSED"}
  },
  "requirements_compliance": 100.0,
  "performance_score": 100.0
}
```

### Sample Data Structure
```
data/sample/
├── fire_perimeters_sample.geojson    # Fire perimeter data
├── damage_inspection_sample.csv      # Damage inspection data
├── fire_alerts_sample.csv           # Fire alert data
└── sample_data_summary.json         # Data summary
```

## 🎯 Success Indicators

### ✅ Everything Working
- `make test` returns exit code 0
- All components show "PASSED" status
- Validation report shows 100% compliance
- Sample data generated successfully

### ❌ Issues Detected
- `make test` returns non-zero exit code
- Components show "FAILED" status
- Validation report shows errors
- Missing or corrupted sample data

## 📞 Getting Help

### 1. Check Documentation
- [Complete User Guide](COMPLETE_USER_GUIDE.md)
- [User Guide](docs/user_guides/README.md)
- [Architecture Design](docs/architecture/architecture_design.md)

### 2. Run Diagnostics
```bash
make test                    # Full test suite
make validate               # Validation only
cat data/output/validation_report.json  # Check results
```

### 3. Common Solutions
```bash
# Clean and reinstall
make clean
make setup

# Check configuration
ls -la config/
cat config/databricks_config.yaml

# Verify environment
python3 --version
pip list | grep pandas
```

---

**Quick Reference for CalFIRE Data Pipeline - Keep this handy! 🚀**
