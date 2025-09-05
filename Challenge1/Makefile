# CalFIRE Pipeline Makefile
# Easy commands for pipeline management

.PHONY: help install test validate deploy clean generate-data

# Default target
help:
	@echo "🔥 CalFIRE Pipeline Commands"
	@echo "=========================="
	@echo "install     - Install Python dependencies"
	@echo "test        - Run all tests and validation"
	@echo "validate    - Run pipeline validation only"
	@echo "deploy      - Deploy pipeline to Databricks"
	@echo "generate-data - Generate sample data for testing"
	@echo "clean       - Clean up generated files"
	@echo "help        - Show this help message"

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

# Deploy pipeline
deploy:
	@echo "🚀 Deploying pipeline..."
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

# Quick setup for new environment
setup: install generate-data validate
	@echo "🎉 Setup complete! Pipeline is ready."

# Full deployment pipeline
deploy-full: clean install generate-data validate deploy
	@echo "🎉 Full deployment complete!"
