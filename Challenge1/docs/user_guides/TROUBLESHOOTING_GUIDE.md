# CalFIRE Pipeline - Troubleshooting Guide

## 🚨 Common Issues and Solutions

This guide provides step-by-step solutions for the most common issues you might encounter when working with the CalFIRE Data Pipeline.

---

## 🔧 Installation and Setup Issues

### Issue: `ModuleNotFoundError: No module named 'pandas'`

**Symptoms**:
```
ModuleNotFoundError: No module named 'pandas'
```

**Solution**:
```bash
# Install dependencies
pip install -r config/requirements.txt

# Or use Makefile
make install

# Verify installation
python3 -c "import pandas; print('Pandas installed successfully')"
```

**Prevention**: Always run `make install` after cloning the repository.

---

### Issue: `python: command not found`

**Symptoms**:
```
make: python: No such file or directory
```

**Solution**:
```bash
# Use python3 instead
python3 scripts/run_tests.py

# Or create alias
alias python=python3

# Verify Python installation
python3 --version
```

**Prevention**: Ensure Python 3.8+ is installed and accessible as `python3`.

---

### Issue: Permission denied when installing packages

**Symptoms**:
```
PermissionError: [Errno 13] Permission denied
```

**Solution**:
```bash
# Install for user only
pip install --user -r config/requirements.txt

# Or use virtual environment
python3 -m venv venv
source venv/bin/activate
pip install -r config/requirements.txt
```

---

## ⚙️ Configuration Issues

### Issue: `KeyError: 'workspace_url'`

**Symptoms**:
```
KeyError: 'workspace_url'
```

**Solution**:
```bash
# Check if config file exists
ls -la config/databricks_config.yaml

# Verify YAML syntax
python3 -c "import yaml; yaml.safe_load(open('config/databricks_config.yaml'))"

# Check file content
cat config/databricks_config.yaml
```

**Expected Format**:
```yaml
databricks:
  workspace_url: "https://your-workspace.cloud.databricks.com"
  access_token: "your-access-token"
  catalog_name: "calfire"
```

---

### Issue: Invalid YAML syntax

**Symptoms**:
```
yaml.scanner.ScannerError: while scanning for the next token
```

**Solution**:
```bash
# Validate YAML syntax
python3 -c "import yaml; yaml.safe_load(open('config/databricks_config.yaml'))"

# Common YAML issues:
# - Missing quotes around values with special characters
# - Incorrect indentation (use spaces, not tabs)
# - Missing colons after keys
```

**Correct Format**:
```yaml
# ✅ Correct
databricks:
  workspace_url: "https://workspace.cloud.databricks.com"
  access_token: "dapi1234567890abcdef"

# ❌ Incorrect
databricks:
  workspace_url: https://workspace.cloud.databricks.com  # Missing quotes
  access_token: dapi1234567890abcdef                     # Missing quotes
```

---

## 🔌 Connection Issues

### Issue: `ConnectionError: Failed to connect to Databricks`

**Symptoms**:
```
ConnectionError: Failed to connect to Databricks workspace
```

**Solution**:
```bash
# 1. Verify workspace URL format
# Should be: https://your-workspace.cloud.databricks.com
# NOT: https://your-workspace.cloud.databricks.com/

# 2. Test connection
curl -H "Authorization: Bearer YOUR_TOKEN" \
     https://your-workspace.cloud.databricks.com/api/2.0/workspace/list

# 3. Check token validity
# Generate new token in Databricks workspace:
# User Settings → Developer → Access Tokens → Generate New Token
```

**Common URL Issues**:
- ❌ `https://workspace.cloud.databricks.com/` (trailing slash)
- ❌ `http://workspace.cloud.databricks.com` (http instead of https)
- ✅ `https://workspace.cloud.databricks.com` (correct format)

---

### Issue: `403 Forbidden` when accessing Databricks

**Symptoms**:
```
403 Forbidden: Insufficient permissions
```

**Solution**:
```bash
# 1. Check token permissions
# Token needs: Workspace access, Job creation, Cluster management

# 2. Verify workspace access
# Ensure you have access to the workspace

# 3. Check Unity Catalog permissions
# Ensure you can create catalogs and schemas
```

---

### Issue: `ConnectionError: Failed to connect to Azure Storage`

**Symptoms**:
```
ConnectionError: Failed to connect to Azure Storage
```

**Solution**:
```bash
# 1. Verify storage account name
# Check in Azure portal

# 2. Verify access key
# Regenerate key in Azure portal if needed

# 3. Test connection
python3 -c "
from azure.storage.blob import BlobServiceClient
client = BlobServiceClient(
    account_url='https://your-storage.dfs.core.windows.net',
    credential='your-access-key'
)
print('Connection successful')
"
```

---

## 📊 Data and Validation Issues

### Issue: `FileNotFoundError: sample data not found`

**Symptoms**:
```
FileNotFoundError: [Errno 2] No such file or directory: 'data/sample/fire_perimeters_sample.geojson'
```

**Solution**:
```bash
# Generate sample data
make generate-data

# Or manually
python3 scripts/sample_data_generator.py

# Verify files created
ls -la data/sample/
```

---

### Issue: Data validation failures

**Symptoms**:
```
❌ data_connectors: FAILED
```

**Solution**:
```bash
# 1. Check validation report
cat data/output/validation_report.json

# 2. Look for specific errors
grep -A 5 -B 5 "error" data/output/validation_report.json

# 3. Regenerate sample data
make clean
make generate-data
make validate
```

---

### Issue: Geospatial processing errors

**Symptoms**:
```
ERROR:GeospatialProcessor:Failed to perform spatial join
```

**Solution**:
```bash
# 1. Check data format
head -5 data/sample/fire_perimeters_sample.geojson

# 2. Verify coordinate data
python3 -c "
import pandas as pd
df = pd.read_csv('data/sample/damage_inspection_sample.csv')
print(df[['latitude', 'longitude']].head())
print(df[['latitude', 'longitude']].isnull().sum())
"

# 3. Check for valid coordinates
# Latitude should be between 32.5 and 42.0
# Longitude should be between -124.5 and -114.0
```

---

## 🚀 Deployment Issues

### Issue: `Job creation failed`

**Symptoms**:
```
Failed to create job: 400 Bad Request
```

**Solution**:
```bash
# 1. Check job configuration
# Verify job name is unique
# Check cluster configuration

# 2. Verify permissions
# Ensure you can create jobs in the workspace

# 3. Check notebook paths
# Ensure notebooks exist in the workspace
```

---

### Issue: `Notebook not found`

**Symptoms**:
```
Notebook not found: /CalFIRE/lakeflow_pipeline
```

**Solution**:
```bash
# 1. Check notebook deployment
# Verify notebooks were uploaded to workspace

# 2. Check workspace structure
databricks workspace ls /CalFIRE

# 3. Redeploy notebooks
python3 scripts/deploy.py
```

---

## 🧪 Testing Issues

### Issue: Tests fail with import errors

**Symptoms**:
```
ImportError: No module named 'data_connectors'
```

**Solution**:
```bash
# 1. Check Python path
python3 -c "import sys; print(sys.path)"

# 2. Run from correct directory
cd /path/to/CalFIRE/Challenge1
python3 scripts/run_tests.py

# 3. Check file structure
ls -la src/connectors/
```

---

### Issue: Validation tests timeout

**Symptoms**:
```
TimeoutError: Operation timed out
```

**Solution**:
```bash
# 1. Increase timeout in config
# Edit config/pipeline_config.yaml
timeout_minutes: 120

# 2. Check system resources
# Ensure sufficient memory and CPU

# 3. Run tests individually
python3 src/validation/pipeline_validation.py
```

---

## 🔍 Debugging Techniques

### Enable Verbose Logging

```python
# Add to your code
import logging
logging.basicConfig(level=logging.DEBUG)

# Or set environment variable
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
```

### Check System Status

```bash
# Check Python environment
python3 --version
pip list | grep -E "(pandas|numpy|streamlit)"

# Check disk space
df -h

# Check memory usage
free -h

# Check network connectivity
ping your-workspace.cloud.databricks.com
```

### Validate Configuration

```bash
# Test YAML syntax
python3 -c "
import yaml
for file in ['config/databricks_config.yaml', 'config/storage_config.yaml', 'config/pipeline_config.yaml']:
    try:
        with open(file) as f:
            yaml.safe_load(f)
        print(f'✅ {file} - Valid')
    except Exception as e:
        print(f'❌ {file} - Error: {e}')
"
```

### Test Individual Components

```bash
# Test data connectors
python3 -c "
import sys
sys.path.append('src/connectors')
from data_connectors import DataConnectorFactory, DataSourceConfig
print('✅ Data connectors import successful')
"

# Test geospatial processing
python3 -c "
import sys
sys.path.append('src/processing')
from geospatial_processing import GeospatialProcessor
print('✅ Geospatial processing import successful')
"
```

---

## 📞 Getting Additional Help

### Before Asking for Help

1. ✅ **Run the troubleshooting checklist**:
   ```bash
   make test                    # Run full test suite
   cat data/output/validation_report.json  # Check results
   ```

2. ✅ **Check the logs**:
   ```bash
   # Look for error messages in console output
   # Check Databricks workspace logs if deployed
   ```

3. ✅ **Verify your environment**:
   ```bash
   python3 --version           # Should be 3.8+
   pip list | grep pandas      # Should show pandas installed
   ls -la config/              # Should show config files
   ```

### When Reporting Issues

Include the following information:

1. **Error Message**: Complete error text
2. **Steps to Reproduce**: What you did before the error
3. **Environment**: 
   - Python version
   - Operating system
   - Databricks workspace version
4. **Configuration**: Relevant config file contents (remove secrets)
5. **Logs**: Relevant log output

### Example Issue Report

```
Issue: ConnectionError when deploying pipeline

Environment:
- Python 3.9.7
- macOS 12.0
- Databricks workspace: https://my-workspace.cloud.databricks.com

Steps to Reproduce:
1. Run `make setup`
2. Run `make deploy`
3. Get ConnectionError

Error Message:
ConnectionError: Failed to connect to Databricks workspace

Configuration (databricks_config.yaml):
databricks:
  workspace_url: "https://my-workspace.cloud.databricks.com"
  access_token: "dapi***"  # (redacted)
  catalog_name: "calfire"

Logs:
[Include relevant log output here]
```

---

## 🎯 Prevention Tips

### Best Practices

1. **Always run setup first**:
   ```bash
   make setup  # Before doing anything else
   ```

2. **Keep configuration files secure**:
   - Never commit secrets to version control
   - Use environment variables for sensitive data
   - Regularly rotate access tokens

3. **Test frequently**:
   ```bash
   make test  # Run after any changes
   ```

4. **Keep dependencies updated**:
   ```bash
   pip install --upgrade -r config/requirements.txt
   ```

5. **Monitor system resources**:
   - Ensure sufficient disk space
   - Monitor memory usage
   - Check network connectivity

---

**Remember: Most issues can be resolved by running `make setup` and `make test`! 🚀**
