#!/usr/bin/env python3
"""
Load Testing Script for DynamoDB to S3 Event Processor

This script:
1. Populates DynamoDB with 500K events across 10 clients
2. Triggers the Lambda function
3. Measures processing performance
4. Generates performance reports
"""

import boto3
import json
import time
import random
import argparse
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any
import sys

# Color codes for output
GREEN = '\033[0;32m'
RED = '\033[0;31m'
YELLOW = '\033[1;33m'
BLUE = '\033[0;34m'
NC = '\033[0m'  # No Color

def print_status(message: str):
    print(f"{BLUE}[INFO]{NC} {message}")

def print_success(message: str):
    print(f"{GREEN}[SUCCESS]{NC} {message}")

def print_warning(message: str):
    print(f"{YELLOW}[WARNING]{NC} {message}")

def print_error(message: str):
    print(f"{RED}[ERROR]{NC} {message}")

class LoadTester:
    def __init__(self, table_name: str, function_name: str, region: str = 'us-east-1'):
        self.table_name = table_name
        self.function_name = function_name
        self.region = region
        self.dynamodb = boto3.resource('dynamodb', region_name=region)
        self.lambda_client = boto3.client('lambda', region_name=region)
        self.table = self.dynamodb.Table(table_name)

    def generate_events_batch(self, batch_size: int, offset: int) -> List[Dict[str, Any]]:
        """Generate a batch of events."""
        events = []
        current_time = datetime.now(timezone.utc)

        for i in range(batch_size):
            event_id = offset + i
            client_id = f'client-{(event_id % 10) + 1:03d}'

            # Distribute events across the past hour for realistic testing
            event_time = current_time - timedelta(minutes=random.randint(1, 59))

            events.append({
                'eventId': f'evt-{event_id:08d}',
                'clientId': client_id,
                'time': event_time.isoformat(),
                'params': [f'param1-{event_id}', f'param2-{event_id}', f'param3-{event_id}'],
                'data': f'load-test-data-{event_id}',
                'metadata': {
                    'source': 'load-test',
                    'version': '1.0',
                    'priority': random.choice(['low', 'medium', 'high']),
                    'category': random.choice(['transaction', 'user_action', 'system_event'])
                },
                'payload_size': len(f'load-test-data-{event_id}') * 10  # Simulate larger payloads
            })

        return events

    def write_batch_to_dynamodb(self, events: List[Dict[str, Any]], batch_id: int) -> Dict[str, Any]:
        """Write a batch of events to DynamoDB."""
        start_time = time.time()

        try:
            with self.table.batch_writer() as batch:
                for event in events:
                    batch.put_item(Item=event)

            duration = time.time() - start_time
            return {
                'batch_id': batch_id,
                'success': True,
                'events_count': len(events),
                'duration': duration,
                'events_per_second': len(events) / duration if duration > 0 else 0
            }

        except Exception as e:
            duration = time.time() - start_time
            return {
                'batch_id': batch_id,
                'success': False,
                'events_count': len(events),
                'duration': duration,
                'error': str(e)
            }

    def populate_events(self, total_events: int = 500000, batch_size: int = 25, max_workers: int = 20) -> Dict[str, Any]:
        """Populate DynamoDB with test events using parallel writes."""
        print_status(f"Starting population of {total_events:,} events...")
        print_status(f"Batch size: {batch_size}, Max workers: {max_workers}")

        start_time = time.time()
        total_batches = (total_events + batch_size - 1) // batch_size
        results = []
        successful_batches = 0
        failed_batches = 0
        total_events_written = 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all batch write tasks
            future_to_batch = {}

            for batch_id in range(total_batches):
                offset = batch_id * batch_size
                actual_batch_size = min(batch_size, total_events - offset)

                events = self.generate_events_batch(actual_batch_size, offset)
                future = executor.submit(self.write_batch_to_dynamodb, events, batch_id)
                future_to_batch[future] = batch_id

            # Process completed batches
            for future in as_completed(future_to_batch):
                result = future.result()
                results.append(result)

                if result['success']:
                    successful_batches += 1
                    total_events_written += result['events_count']

                    # Progress reporting every 100 batches
                    if successful_batches % 100 == 0:
                        progress = (successful_batches / total_batches) * 100
                        elapsed = time.time() - start_time
                        events_so_far = successful_batches * batch_size
                        eps = events_so_far / elapsed if elapsed > 0 else 0
                        print_status(f"Progress: {progress:.1f}% ({events_so_far:,} events, {eps:.0f} events/sec)")
                else:
                    failed_batches += 1
                    print_error(f"Batch {result['batch_id']} failed: {result.get('error', 'Unknown error')}")

        total_duration = time.time() - start_time

        # Calculate statistics
        successful_results = [r for r in results if r['success']]
        if successful_results:
            avg_batch_duration = sum(r['duration'] for r in successful_results) / len(successful_results)
            avg_events_per_second = sum(r['events_per_second'] for r in successful_results) / len(successful_results)
        else:
            avg_batch_duration = 0
            avg_events_per_second = 0

        stats = {
            'total_events_requested': total_events,
            'total_events_written': total_events_written,
            'successful_batches': successful_batches,
            'failed_batches': failed_batches,
            'total_duration_seconds': total_duration,
            'overall_events_per_second': total_events_written / total_duration if total_duration > 0 else 0,
            'average_batch_duration': avg_batch_duration,
            'average_batch_events_per_second': avg_events_per_second
        }

        print_success(f"Population completed!")
        print_status(f"Events written: {total_events_written:,} / {total_events:,}")
        print_status(f"Total duration: {total_duration:.2f} seconds")
        print_status(f"Overall throughput: {stats['overall_events_per_second']:.0f} events/second")

        return stats

    def trigger_lambda_processing(self) -> Dict[str, Any]:
        """Trigger the Lambda function and measure processing performance."""
        print_status("Triggering Lambda function for processing...")

        start_time = time.time()

        try:
            response = self.lambda_client.invoke(
                FunctionName=self.function_name,
                Payload=json.dumps({'load_test': True})
            )

            duration = time.time() - start_time

            # Parse response
            payload = json.loads(response['Payload'].read().decode('utf-8'))

            result = {
                'success': response['StatusCode'] == 200,
                'duration_seconds': duration,
                'status_code': response['StatusCode'],
                'function_error': response.get('FunctionError'),
                'lambda_response': payload
            }

            if result['success'] and 'body' in payload:
                body = json.loads(payload['body'])
                result['processing_stats'] = body

                print_success(f"Lambda processing completed in {duration:.2f} seconds")
                print_status(f"Events processed: {body.get('events_processed', 'N/A')}")
                print_status(f"Clients processed: {body.get('clients_processed', 'N/A')}")

                # Calculate processing throughput
                if 'events_processed' in body and 'processing_time_seconds' in body:
                    processing_eps = body['events_processed'] / body['processing_time_seconds']
                    print_status(f"Processing throughput: {processing_eps:.0f} events/second")

                upload_stats = body.get('upload_stats', {})
                print_status(f"S3 uploads: {upload_stats.get('successful_uploads', 0)} successful, {upload_stats.get('failed_uploads', 0)} failed")
                print_status(f"Data uploaded: {upload_stats.get('total_size_bytes', 0):,} bytes")
            else:
                print_error(f"Lambda processing failed: {result}")

            return result

        except Exception as e:
            duration = time.time() - start_time
            print_error(f"Failed to trigger Lambda: {e}")
            return {
                'success': False,
                'duration_seconds': duration,
                'error': str(e)
            }

    def run_load_test(self, total_events: int = 500000) -> Dict[str, Any]:
        """Run a comprehensive load test."""
        print_status("🚀 Starting Comprehensive Load Test")
        print_status("=" * 50)

        test_start_time = time.time()

        # Step 1: Populate events
        print_status(f"Step 1: Populating {total_events:,} events...")
        population_stats = self.populate_events(total_events)

        # Step 2: Wait a moment for DynamoDB to process
        print_status("Step 2: Waiting for DynamoDB to process writes...")
        time.sleep(5)

        # Step 3: Trigger Lambda processing
        print_status("Step 3: Triggering Lambda processing...")
        processing_stats = self.trigger_lambda_processing()

        total_test_duration = time.time() - test_start_time

        # Compile results
        results = {
            'test_configuration': {
                'total_events': total_events,
                'target_clients': 10,
                'table_name': self.table_name,
                'function_name': self.function_name,
                'region': self.region
            },
            'population_stats': population_stats,
            'processing_stats': processing_stats,
            'total_test_duration_seconds': total_test_duration,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }

        # Generate summary report
        self.generate_summary_report(results)

        return results

    def generate_summary_report(self, results: Dict[str, Any]):
        """Generate a comprehensive summary report."""
        print_status("\n" + "=" * 60)
        print_status("📊 LOAD TEST SUMMARY REPORT")
        print_status("=" * 60)

        # Test Configuration
        config = results['test_configuration']
        print_status(f"📋 Test Configuration:")
        print_status(f"   Events: {config['total_events']:,}")
        print_status(f"   Clients: {config['target_clients']}")
        print_status(f"   Table: {config['table_name']}")
        print_status(f"   Function: {config['function_name']}")

        # Population Performance
        pop_stats = results['population_stats']
        print_status(f"\n📈 Population Performance:")
        print_status(f"   Events Written: {pop_stats['total_events_written']:,} / {pop_stats['total_events_requested']:,}")
        print_status(f"   Duration: {pop_stats['total_duration_seconds']:.2f} seconds")
        print_status(f"   Throughput: {pop_stats['overall_events_per_second']:.0f} events/second")
        success_rate = (pop_stats['successful_batches']/(pop_stats['successful_batches']+pop_stats['failed_batches'])*100) if (pop_stats['successful_batches']+pop_stats['failed_batches']) > 0 else 0
        print_status(f"   Success Rate: {success_rate:.1f}%")

        # Processing Performance
        if results['processing_stats']['success']:
            proc_stats = results['processing_stats']['processing_stats']
            print_status(f"\n⚡ Lambda Processing Performance:")
            print_status(f"   Events Processed: {proc_stats.get('events_processed', 'N/A'):,}")
            print_status(f"   Processing Time: {proc_stats.get('processing_time_seconds', 'N/A')} seconds")

            # Calculate events per second for processing
            if 'events_processed' in proc_stats and 'processing_time_seconds' in proc_stats:
                processing_eps = proc_stats['events_processed'] / proc_stats['processing_time_seconds']
                print_status(f"   Processing Rate: {processing_eps:,.0f} events/second")

            upload_stats = proc_stats.get('upload_stats', {})
            print_status(f"   Upload Success: {upload_stats.get('successful_uploads', 0)}/10 clients")
            print_status(f"   Data Uploaded: {upload_stats.get('total_size_bytes', 0):,} bytes")
        else:
            print_error(f"\n❌ Lambda Processing Failed:")
            print_error(f"   Error: {results['processing_stats'].get('error', 'Unknown error')}")

        # Overall Test Results
        print_status(f"\n🏁 Overall Test Results:")
        print_status(f"   Total Test Duration: {results['total_test_duration_seconds']:.2f} seconds")
        print_status(f"   Test Timestamp: {results['timestamp']}")

        print_status("=" * 60)


def main():
    parser = argparse.ArgumentParser(description='Load test the DynamoDB to S3 Event Processor')
    parser.add_argument('--events', type=int, default=500000, help='Number of events to generate (default: 500000)')
    parser.add_argument('--table', type=str, help='DynamoDB table name')
    parser.add_argument('--function', type=str, help='Lambda function name')
    parser.add_argument('--region', type=str, default='us-east-1', help='AWS region (default: us-east-1)')
    parser.add_argument('--environment', type=str, default='dev', help='Environment (default: dev)')

    args = parser.parse_args()

    # Auto-detect resource names if not provided
    if not args.table:
        args.table = f"eaglesense-event-processor-{args.environment}-events"

    if not args.function:
        args.function = f"eaglesense-event-processor-{args.environment}-event-processor"

    print_status(f"🧪 Load Testing Configuration:")
    print_status(f"   Events: {args.events:,}")
    print_status(f"   Table: {args.table}")
    print_status(f"   Function: {args.function}")
    print_status(f"   Region: {args.region}")
    print_status("")

    # Auto-confirm for load test execution
    if args.events > 100000:
        print_warning(f"Creating {args.events:,} events in DynamoDB...")
        print_status("Proceeding with load test...")

    # Run load test
    tester = LoadTester(args.table, args.function, args.region)

    try:
        results = tester.run_load_test(args.events)

        # Save results to file
        results_file = f"load_test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2)
        print_success(f"Results saved to: {results_file}")

    except KeyboardInterrupt:
        print_warning("\nLoad test interrupted by user.")
    except Exception as e:
        print_error(f"Load test failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()