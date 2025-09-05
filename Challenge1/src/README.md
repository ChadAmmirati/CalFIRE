# Source Code - CalFIRE Data Pipeline

This directory contains the source code for the CalFIRE Data Ingestion Pipeline, organized into logical modules for maintainability and scalability.

## 📁 Directory Structure

```
src/
├── 📁 pipeline/                 # Main pipeline components
│   └── lakeflow_pipeline.py     # Lakeflow Declarative Pipeline
├── 📁 connectors/               # Data source connectors
│   └── data_connectors.py       # Source adapters and connectors
├── 📁 processing/               # Data processing modules
│   ├── geospatial_processing.py # Geospatial operations (Mosaic/H3)
│   └── error_handling_framework.py # Error handling & validation
├── 📁 monitoring/               # Monitoring and dashboards
│   └── monitoring_dashboard.py  # Streamlit monitoring dashboard
└── 📁 validation/               # Testing and validation
    └── pipeline_validation.py   # Comprehensive validation script
```

## 🔧 Components Overview

### Pipeline (`pipeline/`)
- **lakeflow_pipeline.py**: Main Lakeflow Declarative Pipeline implementation
  - Bronze, Silver, Gold layer processing
  - Multi-modal data ingestion (batch, real-time, streaming)
  - Schema evolution and validation
  - Performance optimization

### Connectors (`connectors/`)
- **data_connectors.py**: Data source adapters and connectors
  - Batch file connectors (GeoJSON, CSV, KML)
  - API connectors (ArcGIS REST API)
  - Streaming connectors (Kafka/Event Hub)
  - Connection validation and error handling

### Processing (`processing/`)
- **geospatial_processing.py**: Advanced geospatial operations
  - Mosaic and H3 library integration
  - Spatial joins and enrichment
  - Hotspot analysis and trend detection
  - California-specific coordinate validation

- **error_handling_framework.py**: Comprehensive error handling
  - Data quality validation rules
  - Retry logic with exponential backoff
  - Quarantine and recovery mechanisms
  - Error tracking and alerting

### Monitoring (`monitoring/`)
- **monitoring_dashboard.py**: Real-time monitoring interface
  - Streamlit-based dashboard
  - Performance metrics visualization
  - Data quality monitoring
  - Error analysis and alerting

### Validation (`validation/`)
- **pipeline_validation.py**: Comprehensive testing framework
  - Component validation
  - Integration testing
  - Performance benchmarking
  - Requirements compliance checking

## 🚀 Usage

### Running Individual Components

```bash
# Run pipeline validation
python src/validation/pipeline_validation.py

# Generate sample data
python scripts/sample_data_generator.py

# Deploy pipeline
python scripts/deploy.py
```

### Importing Modules

```python
# Import pipeline components
from src.pipeline.lakeflow_pipeline import pipeline
from src.connectors.data_connectors import DataConnectorFactory
from src.processing.geospatial_processing import GeospatialProcessor
from src.monitoring.monitoring_dashboard import CalFIREMonitoringDashboard
```

## 🧪 Testing

Each component includes comprehensive testing:
- Unit tests for individual functions
- Integration tests for component interactions
- Validation tests for data quality
- Performance tests for scalability

Run all tests:
```bash
make test
```

## 📚 Documentation

- **[Architecture Design](../docs/architecture/architecture_design.md)** - System architecture
- **[User Guide](../docs/user_guides/README.md)** - Setup and usage instructions
- **[Project Summary](../docs/PROJECT_SUMMARY.md)** - Complete project overview

## 🔧 Development

### Code Standards
- Python PEP 8 compliance
- Comprehensive docstrings
- Type hints where applicable
- Error handling and logging

### Adding New Components
1. Create new module in appropriate directory
2. Add comprehensive tests
3. Update documentation
4. Add to validation framework

---

**Built with ❤️ for CalFIRE using the latest Databricks technologies**
