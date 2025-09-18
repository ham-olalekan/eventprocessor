# Makefile for DynamoDB to S3 Event Processor
# Provides convenient commands for development, testing, and deployment

.PHONY: help install test test-unit test-integration test-performance test-all clean lint format check coverage setup run deploy

# Default target
help:
	@echo "🚀 DynamoDB to S3 Event Processor"
	@echo ""
	@echo "Available commands:"
	@echo "  setup          Set up development environment"
	@echo "  install        Install dependencies"
	@echo "  test           Run standard test suite"
	@echo "  test-unit      Run unit tests only"
	@echo "  test-integration  Run integration tests only"
	@echo "  test-performance  Run performance tests"
	@echo "  test-all       Run all tests with coverage"
	@echo "  lint           Run code quality checks"
	@echo "  format         Format code with black"
	@echo "  check          Run all quality checks"
	@echo "  coverage       Generate coverage report"
	@echo "  clean          Clean up generated files"
	@echo "  run            Run processor locally"
	@echo "  package        Create deployment package"
	@echo ""

# Set up development environment
setup:
	@echo "🔧 Setting up development environment..."
	./scripts/setup_dev_environment.sh

# Install dependencies
install:
	@echo "📦 Installing dependencies..."
	pip install -r requirements.txt

# Run standard test suite
test:
	@echo "🧪 Running standard test suite..."
	pytest tests/test_config.py tests/test_data_processor.py tests/test_dynamodb_reader.py tests/test_s3_writer.py -v --tb=short

# Run unit tests only
test-unit:
	@echo "🔬 Running unit tests..."
	pytest -m "unit or not integration" -v --tb=short

# Run integration tests only
test-integration:
	@echo "🔗 Running integration tests..."
	pytest tests/test_integration.py -v --tb=short

# Run performance tests
test-performance:
	@echo "⚡ Running performance tests..."
	pytest tests/test_performance.py -v --tb=short -s

# Run all tests with coverage
test-all:
	@echo "🎯 Running comprehensive test suite..."
	pytest tests/ --cov=src --cov-report=html --cov-report=term --cov-report=xml -v

# Run code quality checks
lint:
	@echo "🔍 Running code quality checks..."
	@echo "Running pylint..."
	pylint src/ --score=yes || echo "Pylint completed with warnings"
	@echo "Running flake8..."
	flake8 src/ tests/ --max-line-length=100 --ignore=E203,W503 || echo "Flake8 completed with warnings"

# Format code
format:
	@echo "✨ Formatting code..."
	black src/ tests/
	@echo "Code formatting complete"

# Check code formatting without changing files
check-format:
	@echo "🎨 Checking code formatting..."
	black --check src/ tests/

# Run all quality checks
check: check-format lint test
	@echo "✅ All quality checks completed"

# Generate coverage report
coverage:
	@echo "📊 Generating coverage report..."
	pytest tests/ --cov=src --cov-report=html --cov-report=term-missing
	@echo "Coverage report generated in htmlcov/"

# Clean up generated files
clean:
	@echo "🧹 Cleaning up..."
	rm -rf __pycache__
	rm -rf .pytest_cache
	rm -rf htmlcov
	rm -rf .coverage
	rm -rf coverage.xml
	rm -rf dist
	rm -rf build
	rm -rf *.egg-info
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	@echo "Cleanup complete"

# Run processor locally
run:
	@echo "🏃 Running processor locally..."
	python scripts/run_local.py

# Create deployment package
package:
	@echo "📦 Creating deployment package..."
	mkdir -p dist
	cp -r src/* dist/
	cp requirements.txt dist/
	cd dist && zip -r ../event-processor-deployment.zip .
	@echo "Deployment package created: event-processor-deployment.zip"

# Install development dependencies
install-dev:
	@echo "🛠️ Installing development dependencies..."
	pip install -r requirements.txt
	pip install pytest-xdist pytest-benchmark pytest-mock coverage[toml] flake8

# Create virtual environment
venv:
	@echo "🐍 Creating virtual environment..."
	python3 -m venv venv
	@echo "Activate with: source venv/bin/activate"

# Quick development test (fastest tests only)
test-quick:
	@echo "⚡ Running quick tests..."
	pytest tests/test_config.py tests/test_data_processor.py -x -v

# Test with different Python versions (if available)
test-python-versions:
	@echo "🐍 Testing with different Python versions..."
	@for version in python3.8 python3.9 python3.10 python3.11; do \
		if command -v $$version >/dev/null 2>&1; then \
			echo "Testing with $$version..."; \
			$$version -m pytest tests/test_config.py -v; \
		fi; \
	done

# Security check
security:
	@echo "🔒 Running security checks..."
	pip install safety bandit
	safety check
	bandit -r src/ -f json -o security-report.json || echo "Bandit completed with warnings"

# Performance benchmark
benchmark:
	@echo "🏁 Running performance benchmarks..."
	pytest tests/test_performance.py::TestPerformance::test_data_processor_performance -v -s --benchmark-only

# Docker build (if Dockerfile exists)
docker-build:
	@if [ -f Dockerfile ]; then \
		echo "🐳 Building Docker image..."; \
		docker build -t event-processor .; \
	else \
		echo "❌ Dockerfile not found"; \
	fi

# Docker run (if image exists)
docker-run:
	@echo "🐳 Running Docker container..."
	docker run --rm -it event-processor

# Show project statistics
stats:
	@echo "📈 Project Statistics:"
	@echo "Lines of code:"
	@find src/ -name "*.py" -exec wc -l {} + | tail -1
	@echo "Test files:"
	@find tests/ -name "test_*.py" | wc -l
	@echo "Total files:"
	@find . -name "*.py" | wc -l