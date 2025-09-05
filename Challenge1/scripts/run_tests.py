#!/usr/bin/env python3
"""
CalFIRE Pipeline Test Runner
Runs all tests and validation for the organized pipeline structure
"""

import os
import sys
import subprocess
from pathlib import Path

def run_validation():
    """Run the main pipeline validation"""
    print("🧪 Running Pipeline Validation...")
    
    try:
        # Run the validation script
        validation_script = Path(__file__).parent.parent / "src" / "validation" / "pipeline_validation.py"
        result = subprocess.run([sys.executable, str(validation_script)], 
                              capture_output=True, text=True)
        
        print(result.stdout)
        if result.stderr:
            print("Errors:", result.stderr)
        
        return result.returncode == 0
        
    except Exception as e:
        print(f"❌ Validation failed: {str(e)}")
        return False

def generate_sample_data():
    """Generate sample data for testing"""
    print("📊 Generating Sample Data...")
    
    try:
        # Run the sample data generator
        generator_script = Path(__file__).parent / "sample_data_generator.py"
        result = subprocess.run([sys.executable, str(generator_script)], 
                              capture_output=True, text=True)
        
        print(result.stdout)
        if result.stderr:
            print("Errors:", result.stderr)
        
        return result.returncode == 0
        
    except Exception as e:
        print(f"❌ Sample data generation failed: {str(e)}")
        return False

def check_structure():
    """Check if the organized structure is correct"""
    print("📁 Checking Project Structure...")
    
    required_dirs = [
        "src/pipeline",
        "src/connectors", 
        "src/processing",
        "src/monitoring",
        "src/validation",
        "config",
        "docs/architecture",
        "docs/user_guides",
        "scripts",
        "data/sample",
        "data/output"
    ]
    
    required_files = [
        "src/pipeline/lakeflow_pipeline.py",
        "src/connectors/data_connectors.py",
        "src/processing/geospatial_processing.py",
        "src/processing/error_handling_framework.py",
        "src/monitoring/monitoring_dashboard.py",
        "src/validation/pipeline_validation.py",
        "config/requirements.txt",
        "config/databricks_config.yaml",
        "config/storage_config.yaml",
        "config/pipeline_config.yaml",
        "docs/architecture/architecture_design.md",
        "docs/user_guides/README.md",
        "docs/PROJECT_SUMMARY.md",
        "scripts/setup_deployment.py",
        "scripts/sample_data_generator.py"
    ]
    
    missing_dirs = []
    missing_files = []
    
    for dir_path in required_dirs:
        if not Path(dir_path).exists():
            missing_dirs.append(dir_path)
    
    for file_path in required_files:
        if not Path(file_path).exists():
            missing_files.append(file_path)
    
    if missing_dirs:
        print(f"❌ Missing directories: {missing_dirs}")
        return False
    
    if missing_files:
        print(f"❌ Missing files: {missing_files}")
        return False
    
    print("✅ Project structure is correct")
    return True

def run_all_tests():
    """Run all tests and validations"""
    print("🔥 CalFIRE Pipeline Test Suite")
    print("=" * 50)
    
    tests_passed = 0
    total_tests = 3
    
    # Test 1: Check structure
    if check_structure():
        tests_passed += 1
        print("✅ Structure check PASSED")
    else:
        print("❌ Structure check FAILED")
    
    # Test 2: Generate sample data
    if generate_sample_data():
        tests_passed += 1
        print("✅ Sample data generation PASSED")
    else:
        print("❌ Sample data generation FAILED")
    
    # Test 3: Run validation
    if run_validation():
        tests_passed += 1
        print("✅ Pipeline validation PASSED")
    else:
        print("❌ Pipeline validation FAILED")
    
    # Summary
    print(f"\n📊 Test Results: {tests_passed}/{total_tests} tests passed")
    
    if tests_passed == total_tests:
        print("🎉 All tests PASSED! Pipeline is ready for deployment.")
        return True
    else:
        print("⚠️  Some tests FAILED. Please fix issues before deployment.")
        return False

def main():
    """Main test runner function"""
    # Check if we're in the right directory
    if not Path("config").exists():
        print("❌ Error: Please run this script from the Challenge1 root directory")
        print("   Expected structure: CalFIRE/Challenge1/")
        return 1
    
    if run_all_tests():
        return 0
    else:
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
