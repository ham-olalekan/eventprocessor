#!/usr/bin/env python3
"""
Script to clear DynamoDB table for fresh load testing
"""

import boto3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

def clear_dynamodb_table(table_name: str, region: str = 'us-east-1'):
    """Clear all items from DynamoDB table using parallel delete operations."""

    print(f"🗑️  Clearing DynamoDB table: {table_name}")

    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(table_name)

    # Get total item count first
    response = table.scan(Select='COUNT')
    total_items = response['Count']
    print(f"📊 Total items to delete: {total_items:,}")

    if total_items == 0:
        print("✅ Table is already empty")
        return

    # Scan and delete in batches
    items_deleted = 0
    batch_size = 25  # DynamoDB batch_writer limit

    def delete_batch(items):
        """Delete a batch of items."""
        deleted_count = 0
        with table.batch_writer() as batch:
            for item in items:
                batch.delete_item(Key={'eventId': item['eventId']})
                deleted_count += 1
        return deleted_count

    # Scan table and collect items for deletion
    print("🔍 Scanning table for items to delete...")
    scan_kwargs = {'ProjectionExpression': 'eventId'}

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        items_batch = []

        while True:
            response = table.scan(**scan_kwargs)

            for item in response['Items']:
                items_batch.append(item)

                # When batch is full, submit for deletion
                if len(items_batch) >= batch_size:
                    future = executor.submit(delete_batch, items_batch.copy())
                    futures.append(future)
                    items_batch = []

            # Process any remaining items
            if items_batch:
                future = executor.submit(delete_batch, items_batch.copy())
                futures.append(future)
                items_batch = []

            # Check if there are more items to scan
            if 'LastEvaluatedKey' not in response:
                break
            scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']

        # Wait for all deletions to complete
        for future in as_completed(futures):
            items_deleted += future.result()
            if items_deleted % 1000 == 0:
                progress = (items_deleted / total_items) * 100
                print(f"🗑️  Progress: {progress:.1f}% ({items_deleted:,}/{total_items:,} deleted)")

    print(f"✅ Successfully deleted {items_deleted:,} items")

    # Verify table is empty
    time.sleep(2)
    response = table.scan(Select='COUNT')
    remaining_items = response['Count']
    print(f"📊 Remaining items: {remaining_items:,}")

if __name__ == "__main__":
    clear_dynamodb_table("eaglesense-event-processor-dev-events")