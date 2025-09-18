"""Integration tests for the complete processing pipeline."""

import pytest
import boto3
import json
import time
from datetime import datetime, timedelta, timezone
from moto import mock_aws
from unittest.mock import patch
import os
import sys

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from main import process_events, lambda_handler
from config import Config
from dynamodb_reader import DynamoDBReader
from data_processor import DataProcessor
from s3_writer import S3Writer


class TestIntegration:
    """Integration tests for the complete pipeline."""

    @mock_aws
    def test_end_to_end_processing(self, aws_credentials, mock_config):
        """Test complete end-to-end processing pipeline."""
        # Setup DynamoDB
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamodb.create_table(
            TableName="test-events",
            KeySchema=[{"AttributeName": "eventId", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "eventId", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        table.meta.client.get_waiter("table_exists").wait(TableName="test-events")

        # Setup S3 buckets
        s3_client = boto3.client("s3", region_name="us-east-1")
        for i in range(1, 4):
            s3_client.create_bucket(Bucket=f"test-client-events-client-{i:03d}")

        # Add test data to DynamoDB
        current_time = datetime.now(timezone.utc)
        with table.batch_writer() as batch:
            # Recent events (within past hour)
            for i in range(30):
                client_id = f"client-{(i % 3) + 1:03d}"
                event_time = current_time - timedelta(minutes=30 + (i % 30))
                batch.put_item(
                    Item={
                        "eventId": f"evt-recent-{i:03d}",
                        "clientId": client_id,
                        "time": event_time.isoformat(),
                        "params": ["param1", "param2"],
                        "data": f"test-data-{i}",
                    }
                )

            # Old events (should be filtered out)
            for i in range(10):
                event_time = current_time - timedelta(hours=2)
                batch.put_item(
                    Item={
                        "eventId": f"evt-old-{i:03d}",
                        "clientId": "client-001",
                        "time": event_time.isoformat(),
                        "params": ["old-param"],
                    }
                )

        # Load config and process
        config = Config(mock_config)
        results = process_events(config)

        # Verify results
        assert results["success"] is True
        assert results["events_processed"] == 30  # Only recent events
        assert results["clients_processed"] == 3

        # Verify files were created in S3
        for i in range(1, 4):
            bucket_name = f"test-client-events-client-{i:03d}"
            response = s3_client.list_objects_v2(Bucket=bucket_name)
            assert "Contents" in response
            assert len(response["Contents"]) == 1

            # Verify file content
            obj = s3_client.get_object(
                Bucket=bucket_name, Key=response["Contents"][0]["Key"]
            )
            content = json.loads(obj["Body"].read().decode("utf-8"))
            assert len(content) == 10  # Each client should have 10 events

    @mock_aws
    def test_lambda_handler(self, aws_credentials, mock_config):
        """Test Lambda handler function."""
        # Setup DynamoDB
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamodb.create_table(
            TableName="test-events",
            KeySchema=[{"AttributeName": "eventId", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "eventId", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        table.meta.client.get_waiter("table_exists").wait(TableName="test-events")

        # Setup S3
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket="test-client-events-client-001")

        # Add minimal test data
        current_time = datetime.now(timezone.utc)
        table.put_item(
            Item={
                "eventId": "evt-001",
                "clientId": "client-001",
                "time": (current_time - timedelta(minutes=30)).isoformat(),
                "params": ["test"],
            }
        )

        # Mock config loading
        with patch("main.Config") as MockConfig:
            mock_config_instance = MockConfig.return_value
            mock_config_instance.dynamodb_table_name = "test-events"
            mock_config_instance.dynamodb_region = "us-east-1"
            mock_config_instance.s3_bucket_prefix = "test-client-events"
            mock_config_instance.s3_region = "us-east-1"
            mock_config_instance.output_format = "json"
            mock_config_instance.parallel_segments = 2
            mock_config_instance.time_window_hours = 1
            mock_config_instance.log_level = "INFO"
            mock_config_instance.get.return_value = 10

            # Call Lambda handler
            event = {"test": "event"}
            context = {}
            response = lambda_handler(event, context)

            assert response["statusCode"] == 200
            body = json.loads(response["body"])
            assert body["success"] is True
            assert body["events_processed"] == 1

    @mock_aws
    def test_processing_with_failures(self, aws_credentials, mock_config):
        """Test processing with some S3 upload failures."""
        # Setup DynamoDB
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamodb.create_table(
            TableName="test-events",
            KeySchema=[{"AttributeName": "eventId", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "eventId", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        table.meta.client.get_waiter("table_exists").wait(TableName="test-events")

        # Only create bucket for client-001, not client-002
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket="test-client-events-client-001")

        # Add events for both clients
        current_time = datetime.now(timezone.utc)
        with table.batch_writer() as batch:
            for i in range(10):
                client_id = f"client-{(i % 2) + 1:03d}"
                batch.put_item(
                    Item={
                        "eventId": f"evt-{i:03d}",
                        "clientId": client_id,
                        "time": (current_time - timedelta(minutes=30)).isoformat(),
                        "params": ["test"],
                    }
                )

        config = Config(mock_config)
        results = process_events(config)

        # Should be marked as failed due to missing bucket for client-002
        assert results["success"] is False
        assert "client-002" in str(results["errors"][0])
        assert results["upload_stats"]["successful_uploads"] == 1
        assert results["upload_stats"]["failed_uploads"] == 1

    @mock_aws
    def test_dynamodb_rate_limiting(self, aws_credentials, mock_config):
        """Test handling of DynamoDB rate limiting."""
        # Setup DynamoDB
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamodb.create_table(
            TableName="test-events",
            KeySchema=[{"AttributeName": "eventId", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "eventId", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        table.meta.client.get_waiter("table_exists").wait(TableName="test-events")

        config = Config(mock_config)

        # Mock DynamoDB reader to simulate rate limiting
        with patch("main.DynamoDBReader") as MockReader:
            mock_reader = MockReader.return_value
            mock_reader.estimate_scan_time.return_value = 1.0

            # Simulate rate limiting that eventually succeeds
            call_count = [0]

            def get_events_with_retry():
                call_count[0] += 1
                if call_count[0] <= 2:
                    from botocore.exceptions import ClientError

                    raise ClientError(
                        {"Error": {"Code": "ProvisionedThroughputExceededException"}},
                        "Scan",
                    )
                return [
                    {
                        "eventId": "evt-1",
                        "clientId": "client-001",
                        "time": datetime.now(timezone.utc).isoformat(),
                    }
                ]

            mock_reader.get_events_past_hour.side_effect = get_events_with_retry

            # Process should handle the rate limiting
            with patch("main.S3Writer"):
                with patch("time.sleep"):  # Speed up test
                    results = process_events(config)

            # Should eventually fail after retries
            assert results["success"] is False
            assert "DynamoDB" in str(results["errors"][0])

    @mock_aws
    def test_large_scale_processing(self, aws_credentials, mock_config):
        """Test processing with large number of events (100k)."""
        # Setup infrastructure
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamodb.create_table(
            TableName="test-events",
            KeySchema=[{"AttributeName": "eventId", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "eventId", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        table.meta.client.get_waiter("table_exists").wait(TableName="test-events")

        s3_client = boto3.client("s3", region_name="us-east-1")
        for i in range(1, 11):
            s3_client.create_bucket(Bucket=f"test-client-events-client-{i:03d}")

        # Generate 100k events across 10 clients
        current_time = datetime.now(timezone.utc)
        batch_size = 25  # DynamoDB batch write limit

        for batch_num in range(4000):  # 4000 * 25 = 100k
            with table.batch_writer() as batch:
                for i in range(batch_size):
                    event_id = batch_num * batch_size + i
                    client_id = f"client-{(event_id % 10) + 1:03d}"
                    event_time = current_time - timedelta(minutes=30 + (event_id % 30))

                    batch.put_item(
                        Item={
                            "eventId": f"evt-{event_id:08d}",
                            "clientId": client_id,
                            "time": event_time.isoformat(),
                            "params": [f"param-{event_id}"],
                            "data": f"data-{event_id}",
                        }
                    )

        # Process the large dataset
        config = Config(mock_config)
        start_time = time.time()
        results = process_events(config)
        processing_time = time.time() - start_time

        # Verify results
        assert results["success"] is True
        assert results["events_processed"] == 100000
        assert results["clients_processed"] == 10

        # Check reasonable processing time (should be under 5 minutes for 100k events)
        assert processing_time < 300

        # Verify each client got approximately 10k events
        for i in range(1, 11):
            bucket_name = f"test-client-events-client-{i:03d}"
            response = s3_client.list_objects_v2(Bucket=bucket_name)
            assert "Contents" in response

            obj = s3_client.get_object(
                Bucket=bucket_name, Key=response["Contents"][0]["Key"]
            )
            content = json.loads(obj["Body"].read().decode("utf-8"))
            assert 9000 <= len(content) <= 11000  # Allow some variance

    @mock_aws
    def test_csv_output_format(self, aws_credentials, tmp_path):
        """Test processing with CSV output format."""
        # Create config with CSV format
        config_file = tmp_path / "config.yaml"
        config_content = """
dynamodb:
  table_name: test-events
  region: us-east-1
  parallel_segments: 2

s3:
  bucket_prefix: test-client-events
  region: us-east-1
  output_format: csv

processing:
  time_window_hours: 1
"""
        config_file.write_text(config_content)

        # Setup infrastructure
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamodb.create_table(
            TableName="test-events",
            KeySchema=[{"AttributeName": "eventId", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "eventId", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        table.meta.client.get_waiter("table_exists").wait(TableName="test-events")

        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket="test-client-events-client-001")

        # Add test data
        current_time = datetime.now(timezone.utc)
        table.put_item(
            Item={
                "eventId": "evt-001",
                "clientId": "client-001",
                "time": (current_time - timedelta(minutes=30)).isoformat(),
                "params": ["p1", "p2"],
                "data": "test-data",
            }
        )

        # Process with CSV format
        config = Config(str(config_file))
        results = process_events(config)

        assert results["success"] is True

        # Verify CSV file was created
        response = s3_client.list_objects_v2(Bucket="test-client-events-client-001")
        assert response["Contents"][0]["Key"].endswith(".csv")

        # Verify CSV content
        import csv
        import io

        obj = s3_client.get_object(
            Bucket="test-client-events-client-001", Key=response["Contents"][0]["Key"]
        )
        content = obj["Body"].read().decode("utf-8")
        reader = csv.DictReader(io.StringIO(content))
        rows = list(reader)

        assert len(rows) == 1
        assert rows[0]["eventId"] == "evt-001"
        assert json.loads(rows[0]["params"]) == ["p1", "p2"]

    def test_concurrent_processing(self, aws_credentials, mock_config):
        """Test that parallel processing works correctly."""
        config = Config(mock_config)

        # Mock components to track parallel execution
        with patch("main.DynamoDBReader") as MockReader:
            with patch("main.DataProcessor") as MockProcessor:
                with patch("main.S3Writer") as MockWriter:
                    # Setup mocks
                    mock_reader = MockReader.return_value
                    mock_reader.estimate_scan_time.return_value = 1.0
                    mock_reader.get_events_past_hour.return_value = [
                        {
                            "eventId": f"evt-{i}",
                            "clientId": f"client-{i % 5}",
                            "time": datetime.now(timezone.utc).isoformat(),
                        }
                        for i in range(100)
                    ]

                    mock_processor = MockProcessor.return_value
                    grouped_events = {}
                    for i in range(5):
                        grouped_events[f"client-{i}"] = [
                            {"eventId": f"evt-{j}", "clientId": f"client-{i}"}
                            for j in range(i * 20, (i + 1) * 20)
                        ]
                    mock_processor.process_events.return_value = grouped_events
                    mock_processor.get_statistics.return_value = {"total_events": 100}

                    mock_writer = MockWriter.return_value
                    mock_writer.write_all_groups.return_value = {
                        f"client-{i}": True for i in range(5)
                    }
                    mock_writer.get_upload_statistics.return_value = {
                        "successful_uploads": 5,
                        "failed_uploads": 0,
                        "total_size_bytes": 10000,
                    }

                    # Process events
                    results = process_events(config)

                    # Verify parallel uploads were requested
                    mock_writer.write_all_groups.assert_called_once()
                    call_args = mock_writer.write_all_groups.call_args
                    assert call_args[1]["max_concurrent"] == 10  # From config

                    assert results["success"] is True
                    assert results["events_processed"] == 100
                    assert results["clients_processed"] == 5
