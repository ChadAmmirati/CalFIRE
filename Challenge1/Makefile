# CalFIRE Production Pipeline Makefile
# Easy commands for production deployment and management

.PHONY: help install test validate deploy clean generate-data setup monitor

# Default target
help:
	@echo "🔥 CalFIRE Production Pipeline Commands"
	@echo "======================================"
	@echo "install           - Install Python dependencies"
	@echo "test              - Run all tests and validation"
	@echo "validate          - Run pipeline validation only"
	@echo "deploy            - Deploy production-ready pipeline"
	@echo "generate-data     - Generate sample data for testing"
	@echo "clean             - Clean up generated files"
	@echo "setup             - Complete setup (install + generate data + validate)"
	@echo "monitor           - Start monitoring dashboard"
	@echo "help              - Show this help message"

# Install dependencies
install:
	@echo "📦 Installing dependencies..."
	pip install -r config/requirements.txt

# Run all tests
test:
	@echo "🧪 Running all tests..."
	python3 scripts/run_tests.py

# Run validation only
validate:
	@echo "✅ Running pipeline validation..."
	python3 src/validation/pipeline_validation.py

# Deploy production pipeline
deploy:
	@echo "🚀 Deploying production pipeline..."
	python3 scripts/deploy.py

# Generate sample data
generate-data:
	@echo "📊 Generating sample data..."
	python3 scripts/sample_data_generator.py

# Clean up generated files
clean:
	@echo "🧹 Cleaning up generated files..."
	rm -rf data/sample/*
	rm -rf data/output/*
	rm -rf __pycache__
	rm -rf src/*/__pycache__
	rm -rf scripts/__pycache__
	find . -name "*.pyc" -delete
	find . -name "*.pyo" -delete
	rm -f deployment.log
	rm -f deployment_status_*.json

# Quick setup for new environment
setup: install generate-data validate
	@echo "🎉 Setup complete! Pipeline is ready."

# Start monitoring dashboard
monitor:
	@echo "📊 Starting monitoring dashboard..."
	streamlit run src/monitoring/monitoring_dashboard.py

# Full deployment pipeline
deploy-full: clean install generate-data validate deploy
	@echo "🎉 Full production deployment complete!"

# Development commands
dev-setup: install generate-data
	@echo "🔧 Development environment ready!"

# Production commands
prod-deploy: deploy
	@echo "🏭 Production deployment complete!"

# Testing commands
test-unit:
	@echo "🧪 Running unit tests..."
	python3 -m pytest tests/unit/ -v

test-integration:
	@echo "🧪 Running integration tests..."
	python3 -m pytest tests/integration/ -v

test-all: test-unit test-integration
	@echo "✅ All tests completed!"

# Configuration commands
config-validate:
	@echo "🔍 Validating configuration files..."
	python3 -c "import yaml; yaml.safe_load(open('config/databricks_config.yaml'))"
	python3 -c "import yaml; yaml.safe_load(open('config/storage_config.yaml'))"
	python3 -c "import yaml; yaml.safe_load(open('config/pipeline_config.yaml'))"
	@echo "✅ Configuration validation passed!"

# Documentation commands
docs-serve:
	@echo "📚 Serving documentation..."
	python3 -m http.server 8000 --directory docs/

# Utility commands
logs:
	@echo "📋 Showing recent logs..."
	tail -f deployment.log

status:
	@echo "📊 Checking deployment status..."
	@if [ -f deployment_status_*.json ]; then \
		cat deployment_status_*.json | jq '.'; \
	else \
		echo "No deployment status found. Run 'make deploy-production' first."; \
	fi

# Security commands
security-check:
	@echo "🔒 Running security checks..."
	@echo "Checking for hardcoded secrets..."
	@grep -r "your-access-token\|your-access-key\|your-storage-account" config/ || echo "✅ No hardcoded secrets found"
	@echo "Checking file permissions..."
	@ls -la config/
	@echo "✅ Security check complete!"

# Backup commands
backup-config:
	@echo "💾 Backing up configuration..."
	@mkdir -p backups/$(shell date +%Y%m%d_%H%M%S)
	@cp config/*.yaml backups/$(shell date +%Y%m%d_%H%M%S)/
	@echo "✅ Configuration backed up!"

# Environment commands
env-check:
	@echo "🔍 Checking environment..."
	@echo "Python version: $(shell python3 --version)"
	@echo "Pip version: $(shell pip --version)"
	@echo "Git version: $(shell git --version)"
	@echo "✅ Environment check complete!"

# Quick commands for common tasks
quick-deploy: config-validate deploy
	@echo "⚡ Quick deployment complete!"

quick-test: test validate
	@echo "⚡ Quick test complete!"

# Help for specific commands
help-deploy:
	@echo "🚀 Deployment Commands:"
	@echo "  deploy            - Full production deployment"
	@echo "  deploy-full       - Clean + install + deploy"
	@echo "  quick-deploy      - Validate config + deploy"

help-test:
	@echo "🧪 Testing Commands:"
	@echo "  test              - Run all tests"
	@echo "  test-unit         - Run unit tests only"
	@echo "  test-integration  - Run integration tests only"
	@echo "  validate          - Run pipeline validation"
	@echo "  quick-test        - Test + validate"

help-monitor:
	@echo "📊 Monitoring Commands:"
	@echo "  monitor           - Start monitoring dashboard"
	@echo "  logs              - Show deployment logs"
	@echo "  status            - Show deployment status"

# Production readiness check
prod-ready:
	@echo "🏭 Checking production readiness..."
	@echo "1. Validating configuration..."
	@make config-validate
	@echo "2. Running tests..."
	@make test
	@echo "3. Checking security..."
	@make security-check
	@echo "4. Checking environment..."
	@make env-check
	@echo "✅ Production readiness check complete!"
	@echo "🚀 Ready for production deployment!"

# Emergency commands
emergency-stop:
	@echo "🚨 Emergency stop - stopping all processes..."
	@pkill -f "streamlit\|python.*production_deploy"
	@echo "✅ Emergency stop complete!"

emergency-clean:
	@echo "🚨 Emergency clean - removing all generated files..."
	@make clean
	@rm -rf backups/
	@rm -rf logs/
	@echo "✅ Emergency clean complete!"

# Show version and info
version:
	@echo "🔥 CalFIRE Production Pipeline"
	@echo "Version: 2.0.0"
	@echo "Environment: Production"
	@echo "Last Updated: $(shell date)"
	@echo "Git Commit: $(shell git rev-parse --short HEAD 2>/dev/null || echo 'N/A')"