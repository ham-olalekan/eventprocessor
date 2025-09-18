#!/bin/bash

# One-command deployment script for DynamoDB to S3 Event Processor
# Usage: ./scripts/deploy.sh [environment] [region]

set -e

# Configuration
PROJECT_NAME="eaglesense-event-processor"
ENVIRONMENT=${1:-prod}
AWS_REGION=${2:-us-east-1}
STACK_NAME="${PROJECT_NAME}-${ENVIRONMENT}"

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

# Check requirements
check_requirements() {
    print_status "Checking requirements..."

    # Check AWS CLI
    if ! command -v aws &> /dev/null; then
        print_error "AWS CLI is not installed. Please install it first."
        exit 1
    fi

    # Check AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        print_error "AWS credentials not configured. Run 'aws configure' first."
        exit 1
    fi

    # Check if we're in the right directory
    if [ ! -f "src/main.py" ] || [ ! -f "cloudformation/infrastructure.yaml" ]; then
        print_error "Please run this script from the project root directory"
        exit 1
    fi

    print_success "Requirements check passed"
}

# Package Lambda function
package_lambda() {
    print_status "Packaging Lambda function..."

    # Create temporary directory
    TEMP_DIR=$(mktemp -d)
    PACKAGE_DIR="$TEMP_DIR/package"
    mkdir -p "$PACKAGE_DIR"

    # Copy source code
    cp -r src/* "$PACKAGE_DIR/"

    # Install dependencies in package directory
    python3 -m pip install -r requirements.txt -t "$PACKAGE_DIR/" --quiet

    # Create deployment package
    cd "$PACKAGE_DIR"
    zip -r "$TEMP_DIR/lambda-package.zip" . --quiet
    cd - > /dev/null

    LAMBDA_PACKAGE="$TEMP_DIR/lambda-package.zip"
    print_success "Lambda package created: $(du -h $LAMBDA_PACKAGE | cut -f1)"
}

# Deploy CloudFormation stack
deploy_infrastructure() {
    print_status "Deploying CloudFormation stack: $STACK_NAME..."

    # Check if stack exists
    if aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$AWS_REGION" &> /dev/null; then
        print_status "Stack exists, updating..."
        OPERATION="update-stack"
    else
        print_status "Creating new stack..."
        OPERATION="create-stack"
    fi

    # Deploy stack
    aws cloudformation "$OPERATION" \
        --stack-name "$STACK_NAME" \
        --template-body file://cloudformation/infrastructure.yaml \
        --parameters \
            ParameterKey=ProjectName,ParameterValue="$PROJECT_NAME" \
            ParameterKey=Environment,ParameterValue="$ENVIRONMENT" \
        --capabilities CAPABILITY_NAMED_IAM \
        --region "$AWS_REGION"

    print_status "Waiting for CloudFormation deployment to complete..."
    aws cloudformation wait stack-"${OPERATION//-*/}"-complete \
        --stack-name "$STACK_NAME" \
        --region "$AWS_REGION"

    if [ $? -eq 0 ]; then
        print_success "CloudFormation deployment completed"
    else
        print_error "CloudFormation deployment failed"
        exit 1
    fi
}

# Update Lambda function code
update_lambda_code() {
    print_status "Updating Lambda function code..."

    # Get function name from CloudFormation outputs
    FUNCTION_NAME=$(aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "$AWS_REGION" \
        --query 'Stacks[0].Outputs[?OutputKey==`LambdaFunctionName`].OutputValue' \
        --output text)

    if [ -z "$FUNCTION_NAME" ]; then
        print_error "Could not get Lambda function name from stack outputs"
        exit 1
    fi

    # Update function code
    aws lambda update-function-code \
        --function-name "$FUNCTION_NAME" \
        --zip-file fileb://"$LAMBDA_PACKAGE" \
        --region "$AWS_REGION" > /dev/null

    print_success "Lambda function code updated: $FUNCTION_NAME"
}

# Populate sample data
populate_sample_data() {
    print_status "Populating DynamoDB with sample data..."

    # Get table name from CloudFormation outputs
    TABLE_NAME=$(aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "$AWS_REGION" \
        --query 'Stacks[0].Outputs[?OutputKey==`DynamoDBTableName`].OutputValue' \
        --output text)

    if [ -z "$TABLE_NAME" ]; then
        print_error "Could not get DynamoDB table name from stack outputs"
        exit 1
    fi

    # Create sample data script
    cat > /tmp/populate_data.py << 'EOF'
import boto3
import json
from datetime import datetime, timedelta, timezone
import random

def populate_sample_data(table_name, region):
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(table_name)

    current_time = datetime.now(timezone.utc)
    events = []

    # Generate 1000 sample events across 10 clients
    for i in range(1000):
        client_id = f'client-{(i % 10) + 1:03d}'
        event_time = current_time - timedelta(minutes=random.randint(1, 59))

        events.append({
            'eventId': f'evt-{i:08d}',
            'clientId': client_id,
            'time': event_time.isoformat(),
            'params': [f'param1-{i}', f'param2-{i}'],
            'data': f'sample-data-{i}'
        })

    # Batch write events
    with table.batch_writer() as batch:
        for event in events:
            batch.put_item(Item=event)

    print(f"Successfully populated {len(events)} sample events")

if __name__ == "__main__":
    import sys
    populate_sample_data(sys.argv[1], sys.argv[2])
EOF

    # Run sample data population
    python3 /tmp/populate_data.py "$TABLE_NAME" "$AWS_REGION"
    rm /tmp/populate_data.py

    print_success "Sample data populated in table: $TABLE_NAME"
}

# Test the deployment
test_deployment() {
    print_status "Testing the deployment..."

    # Get function name
    FUNCTION_NAME=$(aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "$AWS_REGION" \
        --query 'Stacks[0].Outputs[?OutputKey==`LambdaFunctionName`].OutputValue' \
        --output text)

    # Invoke Lambda function
    print_status "Invoking Lambda function..."
    RESULT=$(aws lambda invoke \
        --function-name "$FUNCTION_NAME" \
        --region "$AWS_REGION" \
        --payload '{}' \
        /tmp/lambda-output.json)

    if [ $? -eq 0 ]; then
        print_success "Lambda function invoked successfully"

        # Show function output
        if [ -f /tmp/lambda-output.json ]; then
            print_status "Function output:"
            python3 -m json.tool /tmp/lambda-output.json | head -20
            rm /tmp/lambda-output.json
        fi
    else
        print_warning "Lambda function test failed"
    fi
}

# Display deployment summary
show_summary() {
    print_status "Deployment Summary:"
    echo ""
    echo "🎉 Deployment completed successfully!"
    echo ""
    echo "📋 Resources created:"

    # Get outputs from CloudFormation
    OUTPUTS=$(aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "$AWS_REGION" \
        --query 'Stacks[0].Outputs' \
        --output table)

    echo "$OUTPUTS"
    echo ""
    echo "⏰ Next steps:"
    echo "  1. The Lambda function will run automatically every hour"
    echo "  2. Check CloudWatch logs for execution details"
    echo "  3. Monitor S3 buckets for processed event files"
    echo "  4. View CloudWatch alarms for system health"
    echo ""
    echo "📊 Monitoring:"
    echo "  - CloudWatch Logs: /aws/lambda/${PROJECT_NAME}-${ENVIRONMENT}-event-processor"
    echo "  - CloudWatch Alarms: Check for errors and performance issues"
    echo ""
    echo "🔧 Management commands:"
    echo "  - Update code: aws lambda update-function-code --function-name [function-name] --zip-file fileb://package.zip"
    echo "  - Manual run: aws lambda invoke --function-name [function-name] output.json"
    echo "  - View logs: aws logs tail /aws/lambda/[function-name] --follow"
    echo ""
}

# Cleanup function
cleanup() {
    if [ -n "$TEMP_DIR" ] && [ -d "$TEMP_DIR" ]; then
        rm -rf "$TEMP_DIR"
    fi
}

# Set up cleanup trap
trap cleanup EXIT

# Main execution
main() {
    echo "🚀 DynamoDB to S3 Event Processor Deployment"
    echo "============================================="
    echo "Environment: $ENVIRONMENT"
    echo "Region: $AWS_REGION"
    echo "Stack: $STACK_NAME"
    echo ""

    check_requirements
    package_lambda
    deploy_infrastructure
    update_lambda_code
    populate_sample_data
    test_deployment
    show_summary

    print_success "Deployment completed! 🎉"
}

# Run main function
main "$@"