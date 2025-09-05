# CalFIRE Data Ingestion Pipeline - User Guide

## Overview

This user guide provides detailed instructions for setting up, configuring, and using the CalFIRE Data Ingestion Pipeline. The pipeline is built using the latest Databricks Lakeflow Declarative Pipelines and provides comprehensive data ingestion capabilities for wildfire monitoring and emergency response.

## 🏗️ Architecture

### High-Level System Architecture

The solution implements a modern medallion architecture (Bronze-Silver-Gold) with advanced monitoring and error handling:

```
Data Sources → Ingestion Layer → Lakeflow Pipeline → Storage Layer → Analytics & Monitoring
     ↓              ↓                    ↓               ↓                    ↓
• GeoJSON      • Auto Loader        • Bronze Layer   • Delta Lake        • SQL Dashboards
• CSV          • API Connectors     • Silver Layer   • Unity Catalog     • Monitoring
• KML          • Streaming          • Gold Layer     • Partitioning      • Alerting
• ArcGIS API   • Lakeflow Connect   • Validation     • ACID Compliance   • Metrics
```

### Key Components

1. **Lakeflow Declarative Pipeline** - Latest Databricks feature for declarative, observable data pipelines
2. **Multi-Modal Ingestion** - Batch, real-time, and streaming data sources
3. **Advanced Geospatial Processing** - Mosaic and H3 libraries for spatial operations
4. **Comprehensive Monitoring** - Real-time dashboards and alerting
5. **Robust Error Handling** - Fault tolerance and data quality assurance

## 🚀 Features

### Data Ingestion Capabilities
- **Batch Processing**: Historical fire perimeter data (GeoJSON, CSV, KML)
- **Real-time APIs**: ArcGIS REST API for damage inspection data
- **Streaming Data**: Simulated IoT/real-time fire alerts
- **Schema Evolution**: Automatic schema inference and evolution
- **Incremental Processing**: Efficient handling of new and updated data

### Data Quality & Validation
- **Schema Validation**: Automatic schema inference and validation
- **Data Quality Rules**: Built-in constraints and validation rules
- **Error Handling**: Comprehensive error tracking and quarantine
- **Retry Logic**: Exponential backoff with jitter
- **Fault Tolerance**: Graceful degradation and recovery

### Geospatial Processing
- **H3 Indexing**: High-performance spatial indexing
- **Spatial Joins**: Efficient geospatial operations
- **Hotspot Analysis**: Identification of high-activity areas
- **Trend Analysis**: Temporal and spatial trend analysis
- **Damage Assessment**: Integration with damage inspection data

### Monitoring & Observability
- **Real-time Dashboards**: Comprehensive monitoring interface
- **Latency Metrics**: End-to-end processing latency tracking
- **Fidelity Checks**: Data quality score monitoring
- **Error Analysis**: Detailed error tracking and analysis
- **Alerting**: Configurable alerts for critical issues

## 📁 Project Structure

The project is organized into logical modules for easy navigation and maintenance:

```
CalFIRE/Challenge1/
├── 📁 src/                    # Source code
│   ├── 📁 pipeline/           # Main pipeline components
│   ├── 📁 connectors/         # Data source connectors
│   ├── 📁 processing/         # Data processing modules
│   ├── 📁 monitoring/         # Monitoring and dashboards
│   └── 📁 validation/         # Testing and validation
├── 📁 config/                 # Configuration files
├── 📁 docs/                   # Documentation
├── 📁 scripts/                # Deployment and utility scripts
├── 📁 data/                   # Data files (sample and output)
└── 📁 tests/                  # Test files
```

For detailed structure information, see [DIRECTORY_STRUCTURE.md](../../DIRECTORY_STRUCTURE.md).

## 🛠️ Setup and Installation

### Prerequisites

1. **Databricks Workspace** with Unity Catalog enabled
2. **Azure Data Lake Storage** account
3. **Python 3.8+** with required packages
4. **Databricks CLI** configured

### Installation Steps

1. **Navigate to the project directory**
   ```bash
   cd CalFIRE/Challenge1
   ```

2. **Quick Setup (Recommended)**
   ```bash
   make setup
   ```
   This command will:
   - Install dependencies
   - Generate sample data
   - Run validation tests

3. **Manual Setup (Alternative)**
   ```bash
   # Install dependencies
   pip install -r config/requirements.txt
   
   # Configure your environment
   # Edit config/databricks_config.yaml with your workspace details
   # Edit config/storage_config.yaml with your storage account details
   # Edit config/pipeline_config.yaml with your pipeline preferences
   
   # Generate sample data
   make generate-data
   
   # Run validation
   make validate
   
   # Deploy the pipeline
   make deploy
   ```

### Configuration Files

All configuration files are located in the `config/` directory:

#### config/databricks_config.yaml
```yaml
databricks:
  workspace_url: "https://your-workspace.cloud.databricks.com"
  access_token: "your-access-token"
  catalog_name: "calfire"
  schema_name: "production"
```

#### config/storage_config.yaml
```yaml
storage:
  account_name: "calfirestorage"
  container_name: "calfire-data"
  access_key: "your-access-key"
  endpoint: "https://calfirestorage.dfs.core.windows.net"
```

#### config/pipeline_config.yaml
```yaml
pipeline:
  name: "calfire_wildfire_pipeline"
  description: "CalFIRE wildfire data ingestion and processing pipeline"
  schedule: "0 0 * * *"  # Daily at midnight
  max_retries: 3
  timeout_minutes: 60
  environment: "production"
```

## 🔧 Usage

### Running the Pipeline

1. **Deploy Pipeline**
   ```bash
   make deploy
   # or
   python scripts/deploy.py
   ```

2. **Run Tests**
   ```bash
   make test
   # or
   python scripts/run_tests.py
   ```

3. **Scheduled Execution**
   The pipeline runs automatically based on the schedule defined in `config/pipeline_config.yaml`

4. **Monitoring**
   Access the monitoring dashboard at the configured Streamlit URL

### Available Commands

The project includes a Makefile for easy command execution:

```bash
make help          # Show all available commands
make install       # Install dependencies
make test          # Run all tests
make validate      # Run validation only
make deploy        # Deploy pipeline
make generate-data # Generate sample data
make clean         # Clean up files
make setup         # Complete setup (install + generate data + validate)
```

### Data Sources

#### Batch Data (Fire Perimeters)
- **Format**: GeoJSON, CSV, KML
- **Location**: Azure Data Lake Storage
- **Processing**: Auto Loader with schema inference
- **Frequency**: Daily

#### API Data (Damage Inspection)
- **Source**: ArcGIS REST API
- **Endpoint**: CalFIRE Damage Inspection Service
- **Processing**: Scheduled API calls
- **Frequency**: Every 6 hours

#### Streaming Data (Fire Alerts)
- **Source**: Kafka/Event Hub
- **Processing**: Structured Streaming
- **Frequency**: Real-time

### Monitoring Dashboard

The monitoring dashboard provides:

- **Pipeline Overview**: Success rates, processing latency, data quality scores
- **Performance Metrics**: Throughput, latency trends, resource utilization
- **Data Quality**: Validation results, error rates, quality trends
- **Ingestion Metrics**: Source-specific metrics and success rates
- **Alerts**: Error analysis and critical issue notifications

## 📊 Performance Characteristics

### Expected Performance
- **Batch Processing**: < 5 minutes for typical file sizes
- **Real-time API**: < 30 seconds for API data ingestion
- **Streaming**: < 10 seconds end-to-end latency
- **Throughput**: TB-scale data processing capability
- **Availability**: 99.9% uptime with built-in redundancy

### Scalability
- **Auto-scaling**: Lakeflow pipelines automatically scale based on data volume
- **Concurrency**: Supports multiple concurrent pipelines
- **Storage**: Petabyte-scale Delta Lake storage
- **Processing**: Distributed Spark processing

## 🔍 Data Quality & Validation

### Validation Rules
1. **Coordinate Validation**: Ensure coordinates are within California bounds
2. **Year Validation**: Validate fire years are reasonable (1950-2025)
3. **Acres Validation**: Ensure acres values are non-negative
4. **Required Fields**: Validate required fields are not null
5. **Damage Level Validation**: Validate damage level values

### Error Handling
- **Retry Logic**: Exponential backoff with jitter
- **Quarantine**: Automatic quarantine of problematic data
- **Alerting**: Configurable alerts for critical errors
- **Recovery**: Automatic retry and recovery mechanisms

## 🌍 Geospatial Features

### H3 Spatial Indexing
- **Resolution**: Configurable H3 resolution (default: 8)
- **Indexing**: Automatic H3 index generation for spatial queries
- **Joins**: Efficient spatial joins using H3 indices

### Spatial Analysis
- **Hotspot Detection**: Identification of high-activity areas
- **Trend Analysis**: Temporal and spatial trend analysis
- **Damage Assessment**: Integration with damage inspection data
- **Coverage Analysis**: Spatial coverage and distribution analysis

## 📈 Monitoring & Alerting

### Metrics Tracked
- **Pipeline Metrics**: Success rates, processing latency, error rates
- **Data Quality**: Quality scores, validation results, error counts
- **Ingestion Metrics**: Source-specific metrics, throughput, success rates
- **Error Analysis**: Error types, frequencies, affected records

### Alerting Configuration
- **High Error Rate**: Alert when error rate > 5%
- **Processing Delay**: Alert when latency > 5 minutes
- **Data Quality Degradation**: Alert when quality score < 80%
- **Critical Errors**: Immediate alerts for critical system errors

## 🧪 Testing

### Test Data
The solution includes sample data for testing:
- **Fire Perimeters**: Sample GeoJSON data with various fire types
- **Damage Data**: Sample damage inspection records
- **Streaming Data**: Simulated real-time fire alerts

### Test Scenarios
1. **Batch Processing**: Test with various file formats and sizes
2. **API Integration**: Test with real ArcGIS API endpoints
3. **Streaming**: Test with simulated streaming data
4. **Error Handling**: Test error scenarios and recovery
5. **Geospatial**: Test spatial operations and analysis

## 🔒 Security & Governance

### Unity Catalog Integration
- **Centralized Governance**: Unified data governance across the platform
- **Access Control**: Fine-grained access controls and permissions
- **Data Lineage**: Complete data lineage tracking
- **Audit Logging**: Comprehensive audit trails

### Security Features
- **Encryption**: Data encryption at rest and in transit
- **Authentication**: Azure Active Directory integration
- **Authorization**: Role-based access control
- **Network Security**: VNet integration and firewall rules

## 📚 Documentation

### Technical Documentation
- **API Reference**: Complete API documentation
- **Configuration Guide**: Detailed configuration options
- **Troubleshooting**: Common issues and solutions
- **Performance Tuning**: Optimization recommendations

### User Guides
- **Getting Started**: Quick start guide
- **Dashboard Usage**: Monitoring dashboard guide
- **Data Sources**: Supported data sources and formats
- **Best Practices**: Recommended practices and patterns

## 🤝 Contributing

### Development Setup
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

### Code Standards
- **Python**: PEP 8 compliance
- **Documentation**: Comprehensive docstrings
- **Testing**: Unit tests for all components
- **Logging**: Structured logging throughout


## 🆘 Support

### Getting Help
- **Documentation**: Check the comprehensive documentation
- **Issues**: Report issues via GitHub issues
- **Discussions**: Use GitHub discussions for questions
- **Email**: Contact the development team

### Troubleshooting

#### Common Issues
1. **Connection Errors**: Verify Databricks configuration
2. **Permission Errors**: Check Unity Catalog permissions
3. **Storage Errors**: Verify Azure storage configuration
4. **Performance Issues**: Check cluster configuration and data partitioning

#### Logs and Debugging
- **Pipeline Logs**: Available in Databricks workspace
- **Monitoring Logs**: Available in monitoring dashboard
- **Error Logs**: Detailed error information in quarantine tables

## 🎯 Challenge Requirements Compliance

This solution addresses all CalFIRE challenge requirements:

### ✅ Core Technical Deliverables
- **Architectural Blueprint**: Comprehensive system architecture with data flow
- **Data Ingestion Prototype**: Multi-modal ingestion with source adapters
- **Latency & Fidelity Dashboard**: Real-time monitoring with metrics visualization

### ✅ Reliability & Scalability Assets
- **Error Handling Framework**: Comprehensive error handling and validation
- **Data Quality Assurance**: Built-in data quality modules and protocols
- **Fault Tolerance**: Retry mechanisms, deduplication, and recovery

### ✅ Documentation & Knowledge Share
- **Technical Documentation**: Complete setup instructions and API references
- **User Guide**: Step-by-step deployment and testing guide
- **Sample Outputs**: Screenshots and sample data included

## 🏆 Key Differentiators

1. **Latest Databricks Features**: Leverages Lakeflow Declarative Pipelines and Unity Catalog
2. **Advanced Geospatial Processing**: Mosaic and H3 libraries for high-performance spatial operations
3. **Comprehensive Monitoring**: Real-time dashboards with detailed metrics and alerting
4. **Robust Error Handling**: Multi-layered error handling with quarantine and recovery
5. **Scalable Architecture**: Auto-scaling pipelines with petabyte-scale storage capability
6. **Production Ready**: Complete deployment automation and configuration management

---

**Built with ❤️ for CalFIRE using the latest Databricks technologies**
