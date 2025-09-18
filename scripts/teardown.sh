#!/bin/bash

# Teardown script for DynamoDB to S3 Event Processor
# Usage: ./scripts/teardown.sh [environment] [region]

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

# Check if stack exists
check_stack_exists() {
    if aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$AWS_REGION" &> /dev/null; then
        return 0
    else
        return 1
    fi
}

# Empty S3 buckets before deletion
empty_s3_buckets() {
    print_status "Emptying S3 buckets..."

    # Get bucket prefix from CloudFormation outputs
    BUCKET_PREFIX=$(aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "$AWS_REGION" \
        --query 'Stacks[0].Outputs[?OutputKey==`S3BucketPrefix`].OutputValue' \
        --output text 2>/dev/null || echo "")

    if [ -n "$BUCKET_PREFIX" ]; then
        # List and empty all project buckets
        aws s3api list-buckets --query "Buckets[?starts_with(Name, '$BUCKET_PREFIX')].Name" --output text | \
        while read bucket; do
            if [ -n "$bucket" ]; then
                print_status "Emptying bucket: $bucket"
                aws s3 rm s3://"$bucket" --recursive || true
            fi
        done
    fi

    print_success "S3 buckets emptied"
}

# Delete CloudFormation stack
delete_stack() {
    print_status "Deleting CloudFormation stack: $STACK_NAME..."

    aws cloudformation delete-stack \
        --stack-name "$STACK_NAME" \
        --region "$AWS_REGION"

    print_status "Waiting for stack deletion to complete..."
    aws cloudformation wait stack-delete-complete \
        --stack-name "$STACK_NAME" \
        --region "$AWS_REGION"

    if [ $? -eq 0 ]; then
        print_success "CloudFormation stack deleted successfully"
    else
        print_warning "Stack deletion may have failed or timed out"
    fi
}

# Main execution
main() {
    echo "🗑️  DynamoDB to S3 Event Processor Teardown"
    echo "============================================"
    echo "Environment: $ENVIRONMENT"
    echo "Region: $AWS_REGION"
    echo "Stack: $STACK_NAME"
    echo ""

    # Check if stack exists
    if ! check_stack_exists; then
        print_warning "Stack $STACK_NAME does not exist in region $AWS_REGION"
        exit 0
    fi

    # Confirm deletion
    read -p "Are you sure you want to delete the stack '$STACK_NAME'? (yes/no): " confirm
    if [ "$confirm" != "yes" ]; then
        print_status "Teardown cancelled"
        exit 0
    fi

    empty_s3_buckets
    delete_stack

    print_success "Teardown completed! 🎉"
    echo ""
    echo "🧹 All resources have been removed:"
    echo "  - DynamoDB table and all data"
    echo "  - S3 buckets and all files"
    echo "  - Lambda function"
    echo "  - IAM roles and policies"
    echo "  - CloudWatch logs and alarms"
    echo "  - EventBridge rules"
}

# Run main function
main "$@"