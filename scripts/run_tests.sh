#!/bin/bash

# Test runner script for the DynamoDB to S3 event processor
# This script runs comprehensive tests with different configurations

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if we're in the right directory
if [ ! -f "requirements.txt" ] || [ ! -d "src" ] || [ ! -d "tests" ]; then
    print_error "Please run this script from the project root directory"
    exit 1
fi

# Check if virtual environment exists
if [ ! -d "venv" ] && [ ! -d ".venv" ] && [ -z "$VIRTUAL_ENV" ]; then
    print_warning "No virtual environment detected. Consider creating one with:"
    echo "  python -m venv venv && source venv/bin/activate"
fi

print_status "Installing/updating dependencies..."
pip install -r requirements.txt > /dev/null 2>&1

print_status "Running code quality checks..."

# Run pylint
print_status "Running pylint..."
pylint src/ --score=yes --reports=yes || print_warning "Pylint found issues"

# Run black for code formatting check
print_status "Checking code formatting with black..."
black --check src/ tests/ || {
    print_warning "Code formatting issues found. Run 'black src/ tests/' to fix."
}

print_status "Running test suites..."

# Run unit tests
print_status "Running unit tests..."
pytest tests/test_config.py tests/test_data_processor.py -v --tb=short -m "not slow"

if [ $? -eq 0 ]; then
    print_success "Unit tests passed"
else
    print_error "Unit tests failed"
    exit 1
fi

# Run integration tests
print_status "Running integration tests..."
pytest tests/test_integration.py -v --tb=short -m "not slow"

if [ $? -eq 0 ]; then
    print_success "Integration tests passed"
else
    print_error "Integration tests failed"
    exit 1
fi

# Run DynamoDB and S3 tests
print_status "Running AWS service tests..."
pytest tests/test_dynamodb_reader.py tests/test_s3_writer.py -v --tb=short -m "not slow"

if [ $? -eq 0 ]; then
    print_success "AWS service tests passed"
else
    print_error "AWS service tests failed"
    exit 1
fi

# Run performance tests (optional)
if [ "$1" = "--performance" ] || [ "$1" = "--full" ]; then
    print_status "Running performance tests (this may take several minutes)..."
    pytest tests/test_performance.py -v --tb=short -s

    if [ $? -eq 0 ]; then
        print_success "Performance tests passed"
    else
        print_warning "Performance tests had issues"
    fi
fi

# Run all tests with coverage
if [ "$1" = "--coverage" ] || [ "$1" = "--full" ]; then
    print_status "Running all tests with coverage analysis..."
    pytest tests/ --cov=src --cov-report=html --cov-report=term -m "not slow"

    if [ $? -eq 0 ]; then
        print_success "Coverage analysis complete. Check htmlcov/index.html for detailed report."
    else
        print_error "Coverage analysis failed"
        exit 1
    fi
fi

# Run specific test categories based on markers
if [ "$1" = "--unit" ]; then
    print_status "Running only unit tests..."
    pytest -m "unit" -v
elif [ "$1" = "--integration" ]; then
    print_status "Running only integration tests..."
    pytest -m "integration" -v
elif [ "$1" = "--performance" ]; then
    print_status "Running only performance tests..."
    pytest -m "performance" -v -s
fi

print_success "All requested tests completed successfully!"

# Print usage information
if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --unit          Run only unit tests"
    echo "  --integration   Run only integration tests"
    echo "  --performance   Run performance tests (slow)"
    echo "  --coverage      Run tests with coverage analysis"
    echo "  --full          Run all tests including performance and coverage"
    echo "  --help, -h      Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                    # Run standard test suite"
    echo "  $0 --full            # Run all tests with coverage"
    echo "  $0 --unit            # Run only fast unit tests"
    echo "  $0 --performance     # Run only performance tests"
    echo ""
fi