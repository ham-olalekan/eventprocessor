"""Pytest configuration and shared fixtures."""

import pytest
import boto3
from moto import mock_aws
from datetime import datetime, timedelta, timezone
import json
import os
import sys

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
def sample_events():
    """Generate sample events for testing."""
    base_time = datetime.now(timezone.utc) - timedelta(minutes=30)
    events = []

    # Generate events for 10 clients with varying amounts
    client_event_counts = {
        "client-001": 15000,
        "client-002": 12000,
        "client-003": 10000,
        "client-004": 8000,
        "client-005": 8000,
        "client-006": 7000,
        "client-007": 7000,
        "client-008": 7000,
        "client-009": 7000,
        "client-010": 19000,
    }

    event_id = 1
    for client_id, count in client_event_counts.items():
        # Distribute events over the past hour
        for i in range(count):
            event_time = base_time + timedelta(
                seconds=(i * 3600 / count)  # Spread evenly over the hour
            )
            events.append(
                {
                    "eventId": f"evt-{event_id:08d}",
                    "clientId": client_id,
                    "params": [f"param1-{i}", f"param2-{i}", f"param3-{i}"],
                    "time": event_time.isoformat(),
                    "data": f"sample-data-{i}",
                }
            )
            event_id += 1

    return events


@pytest.fixture
def sample_events_small():
    """Generate small set of sample events for unit testing."""
    base_time = datetime.now(timezone.utc) - timedelta(minutes=30)
    events = []

    for i in range(100):
        client_id = f"client-{(i % 3) + 1:03d}"
        event_time = base_time + timedelta(seconds=i * 10)
        events.append(
            {
                "eventId": f"evt-{i:08d}",
                "clientId": client_id,
                "params": [f"param1-{i}", f"param2-{i}"],
                "time": event_time.isoformat(),
                "data": f"test-data-{i}",
            }
        )

    return events


@pytest.fixture
def invalid_events():
    """Generate events with missing/invalid fields for testing validation."""
    base_time = datetime.now(timezone.utc) - timedelta(minutes=30)

    return [
        # Missing clientId
        {
            "eventId": "evt-invalid-001",
            "time": base_time.isoformat(),
            "params": ["param1"],
        },
        # Missing eventId
        {"clientId": "client-001", "time": base_time.isoformat(), "params": ["param1"]},
        # Missing time
        {"eventId": "evt-invalid-003", "clientId": "client-001", "params": ["param1"]},
        # None values
        {"eventId": None, "clientId": "client-001", "time": base_time.isoformat()},
        # Valid event for comparison
        {
            "eventId": "evt-valid-001",
            "clientId": "client-001",
            "time": base_time.isoformat(),
            "params": ["param1"],
        },
    ]


@pytest.fixture
def old_events():
    """Generate events older than 1 hour for filtering tests."""
    old_time = datetime.now(timezone.utc) - timedelta(hours=2)
    events = []

    for i in range(50):
        events.append(
            {
                "eventId": f"evt-old-{i:05d}",
                "clientId": f"client-{(i % 3) + 1:03d}",
                "params": ["old-param"],
                "time": old_time.isoformat(),
            }
        )

    return events


@pytest.fixture
def mock_dynamodb_table(aws_credentials):
    """Create a mocked DynamoDB table."""
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

        # Create table
        table = dynamodb.create_table(
            TableName="test-events",
            KeySchema=[{"AttributeName": "eventId", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "eventId", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )

        # Wait for table to be created
        table.meta.client.get_waiter("table_exists").wait(TableName="test-events")

        yield table


@pytest.fixture
def mock_s3_buckets(aws_credentials):
    """Create mocked S3 buckets for testing."""
    with mock_aws():
        s3_client = boto3.client("s3", region_name="us-east-1")

        # Create buckets for each client
        bucket_names = []
        for i in range(1, 11):
            bucket_name = f"test-client-events-client-{i:03d}"
            s3_client.create_bucket(Bucket=bucket_name)
            bucket_names.append(bucket_name)

        yield s3_client, bucket_names


@pytest.fixture
def mock_config(tmp_path):
    """Create a mock configuration file."""
    config_dir = tmp_path / "config"
    config_dir.mkdir()

    config_file = config_dir / "config.yaml"
    config_content = """
dynamodb:
  table_name: test-events
  region: us-east-1
  parallel_segments: 4
  read_throughput_percent: 0.5
  scan_batch_size: 100

s3:
  bucket_prefix: test-client-events
  region: us-east-1
  output_format: json
  server_side_encryption: AES256

processing:
  time_window_hours: 1
  batch_size: 1000
  max_retries: 3
  retry_delay: 1

logging:
  level: DEBUG
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

performance:
  parallel_uploads: true
  max_concurrent_uploads: 5
"""

    config_file.write_text(config_content)
    return str(config_file)


@pytest.fixture
def populated_dynamodb(mock_dynamodb_table, sample_events_small):
    """Populate DynamoDB table with sample events."""
    table = mock_dynamodb_table

    # Add events to table
    with table.batch_writer() as batch:
        for event in sample_events_small:
            batch.put_item(Item=event)

    return table


@pytest.fixture
def mixed_time_events():
    """Generate events with mixed timestamps for time filtering tests."""
    current_time = datetime.now(timezone.utc)
    events = []

    # Events from 2 hours ago
    for i in range(20):
        event_time = current_time - timedelta(hours=2, minutes=i)
        events.append(
            {
                "eventId": f"evt-old-{i:03d}",
                "clientId": "client-001",
                "params": ["old"],
                "time": event_time.isoformat(),
            }
        )

    # Events from 30 minutes ago (should be included)
    for i in range(30):
        event_time = current_time - timedelta(minutes=30 + i)
        events.append(
            {
                "eventId": f"evt-recent-{i:03d}",
                "clientId": "client-002",
                "params": ["recent"],
                "time": event_time.isoformat(),
            }
        )

    # Events from the future (edge case)
    for i in range(10):
        event_time = current_time + timedelta(minutes=10 + i)
        events.append(
            {
                "eventId": f"evt-future-{i:03d}",
                "clientId": "client-003",
                "params": ["future"],
                "time": event_time.isoformat(),
            }
        )

    return events
