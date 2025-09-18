#!/bin/bash

# Monitoring script for DynamoDB to S3 Event Processor
# Usage: ./scripts/monitor.sh [environment] [region]

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

# Get CloudFormation outputs
get_outputs() {
    FUNCTION_NAME=$(aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "$AWS_REGION" \
        --query 'Stacks[0].Outputs[?OutputKey==`LambdaFunctionName`].OutputValue' \
        --output text 2>/dev/null || echo "")

    TABLE_NAME=$(aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "$AWS_REGION" \
        --query 'Stacks[0].Outputs[?OutputKey==`DynamoDBTableName`].OutputValue' \
        --output text 2>/dev/null || echo "")

    BUCKET_PREFIX=$(aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "$AWS_REGION" \
        --query 'Stacks[0].Outputs[?OutputKey==`S3BucketPrefix`].OutputValue' \
        --output text 2>/dev/null || echo "")
}

# Show function status
show_function_status() {
    print_status "Lambda Function Status:"

    if [ -n "$FUNCTION_NAME" ]; then
        aws lambda get-function \
            --function-name "$FUNCTION_NAME" \
            --region "$AWS_REGION" \
            --query '{
                FunctionName: Configuration.FunctionName,
                Runtime: Configuration.Runtime,
                State: Configuration.State,
                LastModified: Configuration.LastModified,
                MemorySize: Configuration.MemorySize,
                Timeout: Configuration.Timeout
            }' \
            --output table
    else
        print_error "Function not found"
    fi
    echo ""
}

# Show recent executions
show_recent_executions() {
    print_status "Recent Lambda Executions (last 24 hours):"

    if [ -n "$FUNCTION_NAME" ]; then
        END_TIME=$(date -u +"%Y-%m-%dT%H:%M:%S.000Z")
        START_TIME=$(date -u -d '24 hours ago' +"%Y-%m-%dT%H:%M:%S.000Z")

        aws logs filter-log-events \
            --log-group-name "/aws/lambda/$FUNCTION_NAME" \
            --region "$AWS_REGION" \
            --start-time $(date -d "$START_TIME" +%s)000 \
            --end-time $(date -d "$END_TIME" +%s)000 \
            --filter-pattern "Processing Summary" \
            --query 'events[*].[eventTimestamp,message]' \
            --output table 2>/dev/null || print_warning "No recent executions found"
    fi
    echo ""
}

# Show DynamoDB metrics
show_dynamodb_metrics() {
    print_status "DynamoDB Table Status:"

    if [ -n "$TABLE_NAME" ]; then
        aws dynamodb describe-table \
            --table-name "$TABLE_NAME" \
            --region "$AWS_REGION" \
            --query '{
                TableName: Table.TableName,
                TableStatus: Table.TableStatus,
                ItemCount: Table.ItemCount,
                TableSizeBytes: Table.TableSizeBytes,
                BillingMode: Table.BillingModeSummary.BillingMode
            }' \
            --output table
    else
        print_error "Table not found"
    fi
    echo ""
}

# Show S3 bucket status
show_s3_status() {
    print_status "S3 Buckets Status:"

    if [ -n "$BUCKET_PREFIX" ]; then
        echo "Bucket Prefix: $BUCKET_PREFIX"
        echo ""

        aws s3api list-buckets \
            --query "Buckets[?starts_with(Name, '$BUCKET_PREFIX')].{Name:Name,Created:CreationDate}" \
            --output table

        echo ""
        print_status "Recent Files (last 10 per bucket):"

        aws s3api list-buckets \
            --query "Buckets[?starts_with(Name, '$BUCKET_PREFIX')].Name" \
            --output text | while read bucket; do
            if [ -n "$bucket" ]; then
                echo "📁 $bucket:"
                aws s3 ls s3://"$bucket" --recursive | tail -10 | while read line; do
                    echo "   $line"
                done
                echo ""
            fi
        done
    else
        print_error "Bucket prefix not found"
    fi
}

# Show CloudWatch alarms
show_alarms() {
    print_status "CloudWatch Alarms:"

    aws cloudwatch describe-alarms \
        --alarm-names \
            "${PROJECT_NAME}-${ENVIRONMENT}-lambda-errors" \
            "${PROJECT_NAME}-${ENVIRONMENT}-lambda-duration" \
            "${PROJECT_NAME}-${ENVIRONMENT}-dynamodb-throttles" \
        --region "$AWS_REGION" \
        --query 'MetricAlarms[*].{
            Name: AlarmName,
            State: StateValue,
            Reason: StateReason,
            Updated: StateUpdatedTimestamp
        }' \
        --output table 2>/dev/null || print_warning "No alarms found"
    echo ""
}

# Trigger manual execution
trigger_execution() {
    print_status "Triggering manual execution..."

    if [ -n "$FUNCTION_NAME" ]; then
        RESULT=$(aws lambda invoke \
            --function-name "$FUNCTION_NAME" \
            --region "$AWS_REGION" \
            --payload '{}' \
            /tmp/lambda-output.json)

        if [ $? -eq 0 ]; then
            print_success "Function invoked successfully"
            echo "Execution result:"
            python3 -m json.tool /tmp/lambda-output.json
            rm -f /tmp/lambda-output.json
        else
            print_error "Function invocation failed"
        fi
    else
        print_error "Function not found"
    fi
    echo ""
}

# Show live logs
show_live_logs() {
    print_status "Showing live logs (Ctrl+C to stop)..."

    if [ -n "$FUNCTION_NAME" ]; then
        aws logs tail "/aws/lambda/$FUNCTION_NAME" \
            --region "$AWS_REGION" \
            --follow
    else
        print_error "Function not found"
    fi
}

# Show help
show_help() {
    echo "DynamoDB to S3 Event Processor - Monitoring Script"
    echo ""
    echo "Usage: $0 [environment] [region] [command]"
    echo ""
    echo "Commands:"
    echo "  status     - Show overall system status (default)"
    echo "  logs       - Show live logs"
    echo "  run        - Trigger manual execution"
    echo "  help       - Show this help"
    echo ""
    echo "Examples:"
    echo "  $0                    # Show status for prod environment"
    echo "  $0 dev                # Show status for dev environment"
    echo "  $0 prod us-west-2     # Show status for prod in us-west-2"
    echo "  $0 prod us-east-1 logs # Show live logs"
    echo ""
}

# Main execution
main() {
    COMMAND=${3:-status}

    case "$COMMAND" in
        "help"|"-h"|"--help")
            show_help
            exit 0
            ;;
        "logs")
            get_outputs
            show_live_logs
            ;;
        "run")
            get_outputs
            trigger_execution
            ;;
        "status"|*)
            echo "📊 DynamoDB to S3 Event Processor - Monitoring Dashboard"
            echo "========================================================"
            echo "Environment: $ENVIRONMENT"
            echo "Region: $AWS_REGION"
            echo "Stack: $STACK_NAME"
            echo ""

            get_outputs

            if [ -z "$FUNCTION_NAME" ]; then
                print_error "Stack $STACK_NAME not found or not deployed"
                exit 1
            fi

            show_function_status
            show_recent_executions
            show_dynamodb_metrics
            show_s3_status
            show_alarms

            echo "🔧 Management Commands:"
            echo "  Monitor logs: $0 $ENVIRONMENT $AWS_REGION logs"
            echo "  Manual run:   $0 $ENVIRONMENT $AWS_REGION run"
            echo ""
            ;;
    esac
}

# Run main function
main "$@"