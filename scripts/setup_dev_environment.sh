#!/bin/bash

# Development environment setup script
# This script sets up a complete development environment for the event processor

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

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

print_status "Setting up development environment for DynamoDB to S3 Event Processor..."

# Check Python version
python_version=$(python3 --version 2>&1 | cut -d' ' -f2 | cut -d'.' -f1,2)
required_version="3.8"

if [ "$(printf '%s\n' "$required_version" "$python_version" | sort -V | head -n1)" != "$required_version" ]; then
    print_error "Python $required_version or higher is required. Found: $python_version"
    exit 1
fi

print_success "Python version check passed: $python_version"

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    print_status "Creating virtual environment..."
    python3 -m venv venv
    print_success "Virtual environment created"
else
    print_status "Virtual environment already exists"
fi

# Activate virtual environment
print_status "Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
print_status "Upgrading pip..."
pip install --upgrade pip > /dev/null 2>&1

# Install dependencies
print_status "Installing Python dependencies..."
pip install -r requirements.txt

# Install development dependencies
print_status "Installing additional development tools..."
pip install pytest-xdist pytest-benchmark pytest-mock coverage[toml]

# Create necessary directories
print_status "Creating necessary directories..."
mkdir -p logs
mkdir -p data
mkdir -p htmlcov

# Copy example environment file
if [ ! -f ".env" ]; then
    print_status "Creating .env file from example..."
    cp .env.example .env
    print_warning "Please update .env file with your AWS credentials and configuration"
else
    print_status ".env file already exists"
fi

# Set up git hooks (if .git exists)
if [ -d ".git" ]; then
    print_status "Setting up git pre-commit hooks..."
    cat > .git/hooks/pre-commit << 'EOF'
#!/bin/bash
# Pre-commit hook to run tests and code quality checks

echo "Running pre-commit checks..."

# Run black formatting check
echo "Checking code formatting..."
if ! black --check src/ tests/ > /dev/null 2>&1; then
    echo "❌ Code formatting issues found. Run 'black src/ tests/' to fix."
    exit 1
fi

# Run quick tests
echo "Running quick tests..."
if ! pytest tests/test_config.py tests/test_data_processor.py -x -q > /dev/null 2>&1; then
    echo "❌ Quick tests failed. Please fix issues before committing."
    exit 1
fi

echo "✅ Pre-commit checks passed"
EOF
    chmod +x .git/hooks/pre-commit
    print_success "Git pre-commit hooks installed"
fi

# Create a sample configuration for testing
print_status "Creating sample test configuration..."
cat > config/test_config.yaml << 'EOF'
# Test configuration for development and testing

dynamodb:
  table_name: test-events-dev
  region: us-east-1
  parallel_segments: 4
  read_throughput_percent: 0.3
  scan_batch_size: 500

s3:
  bucket_prefix: dev-client-events
  region: us-east-1
  output_format: json
  server_side_encryption: AES256

processing:
  time_window_hours: 1
  batch_size: 5000
  max_retries: 3
  retry_delay: 1

logging:
  level: DEBUG
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

performance:
  parallel_uploads: true
  max_concurrent_uploads: 5
EOF

# Create development scripts
print_status "Creating development helper scripts..."

# Create quick test script
cat > scripts/quick_test.sh << 'EOF'
#!/bin/bash
# Quick test script for development

echo "Running quick development tests..."
pytest tests/test_config.py tests/test_data_processor.py -v --tb=short
EOF
chmod +x scripts/quick_test.sh

# Create format code script
cat > scripts/format_code.sh << 'EOF'
#!/bin/bash
# Code formatting script

echo "Formatting code with black..."
black src/ tests/
echo "Code formatting complete."
EOF
chmod +x scripts/format_code.sh

# Create local run script
cat > scripts/run_local.py << 'EOF'
#!/usr/bin/env python3
"""
Local development runner for testing the event processor.
This script simulates the Lambda environment locally.
"""

import sys
import os
import json
from datetime import datetime

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from main import process_events
from config import Config

def main():
    """Run the processor locally with development configuration."""
    print("🚀 Running DynamoDB to S3 Event Processor locally...")
    print(f"📅 Started at: {datetime.now().isoformat()}")
    print("-" * 50)

    try:
        # Load development configuration
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'test_config.yaml')
        config = Config(config_path)

        # Process events
        results = process_events(config)

        # Print results
        print("\n📊 Processing Results:")
        print(json.dumps(results, indent=2, default=str))

        if results['success']:
            print("\n✅ Processing completed successfully!")
        else:
            print("\n❌ Processing failed!")
            return 1

    except Exception as e:
        print(f"\n💥 Error: {str(e)}")
        return 1

    return 0

if __name__ == "__main__":
    sys.exit(main())
EOF
chmod +x scripts/run_local.py

# Run initial tests to verify setup
print_status "Running initial tests to verify setup..."
if pytest tests/test_config.py -v --tb=short > /dev/null 2>&1; then
    print_success "Initial tests passed - setup verified!"
else
    print_warning "Some tests failed - please check configuration"
fi

# Print setup summary
echo ""
echo "🎉 Development environment setup complete!"
echo ""
echo "📋 Next steps:"
echo "  1. Update .env file with your AWS credentials"
echo "  2. Run tests: ./scripts/run_tests.sh"
echo "  3. Run quick tests: ./scripts/quick_test.sh"
echo "  4. Format code: ./scripts/format_code.sh"
echo "  5. Run locally: python scripts/run_local.py"
echo ""
echo "📁 Key files created:"
echo "  - venv/                    # Virtual environment"
echo "  - .env                     # Environment variables"
echo "  - config/test_config.yaml  # Test configuration"
echo "  - scripts/                 # Development scripts"
echo ""
echo "🔧 Development commands:"
echo "  source venv/bin/activate   # Activate environment"
echo "  pytest tests/              # Run all tests"
echo "  black src/ tests/          # Format code"
echo "  pylint src/                # Code quality check"
echo ""

print_success "Ready for development! 🚀"