"""Tests for S3 writer."""

import pytest
import json
import csv
from datetime import datetime, timezone
from unittest.mock import Mock, patch, MagicMock, call
import boto3
from botocore.exceptions import ClientError
from moto import mock_aws
import io

from s3_writer import S3Writer


class TestS3Writer:
    """Test S3 writing functionality."""

    def test_init(self):
        """Test S3Writer initialization."""
        writer = S3Writer("test-bucket", "us-east-1", "json")

        assert writer.bucket_prefix == "test-bucket"
        assert writer.region == "us-east-1"
        assert writer.output_format == "json"
        assert writer.upload_stats["successful_uploads"] == 0
        assert writer.upload_stats["failed_uploads"] == 0

    @mock_aws
    def test_write_all_groups(self, aws_credentials):
        """Test writing all client groups to S3."""
        # Create S3 buckets
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket="test-bucket-client-1")
        s3_client.create_bucket(Bucket="test-bucket-client-2")

        writer = S3Writer("test-bucket", "us-east-1", "json")

        grouped_events = {
            "client-1": [
                {
                    "eventId": "evt-1",
                    "clientId": "client-1",
                    "time": "2025-01-01T12:00:00Z",
                },
                {
                    "eventId": "evt-2",
                    "clientId": "client-1",
                    "time": "2025-01-01T12:01:00Z",
                },
            ],
            "client-2": [
                {
                    "eventId": "evt-3",
                    "clientId": "client-2",
                    "time": "2025-01-01T12:02:00Z",
                }
            ],
        }

        results = writer.write_all_groups(grouped_events, max_concurrent=2)

        assert results["client-1"] is True
        assert results["client-2"] is True
        assert writer.upload_stats["successful_uploads"] == 2
        assert writer.upload_stats["failed_uploads"] == 0

        # Verify files were created in S3
        response = s3_client.list_objects_v2(Bucket="test-bucket-client-1")
        assert "Contents" in response
        assert len(response["Contents"]) == 1

    @mock_aws
    def test_upload_client_events_json(self, aws_credentials):
        """Test uploading events in JSON format."""
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket="test-bucket-client-1")

        writer = S3Writer("test-bucket", "us-east-1", "json")

        events = [
            {
                "eventId": "evt-1",
                "clientId": "client-1",
                "time": "2025-01-01T12:00:00Z",
                "params": ["p1", "p2"],
            },
            {
                "eventId": "evt-2",
                "clientId": "client-1",
                "time": "2025-01-01T12:01:00Z",
                "params": ["p3"],
            },
        ]

        timestamp = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        success = writer._upload_client_events("client-1", events, timestamp)

        assert success is True

        # Verify file content
        response = s3_client.get_object(
            Bucket="test-bucket-client-1", Key="events-2025-01-01-12.json"
        )
        content = json.loads(response["Body"].read().decode("utf-8"))

        assert len(content) == 2
        assert content[0]["eventId"] == "evt-1"
        assert content[1]["eventId"] == "evt-2"

    @mock_aws
    def test_upload_client_events_csv(self, aws_credentials):
        """Test uploading events in CSV format."""
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket="test-bucket-client-1")

        writer = S3Writer("test-bucket", "us-east-1", "csv")

        events = [
            {
                "eventId": "evt-1",
                "clientId": "client-1",
                "time": "2025-01-01T12:00:00Z",
                "params": ["p1", "p2"],
            },
            {
                "eventId": "evt-2",
                "clientId": "client-1",
                "time": "2025-01-01T12:01:00Z",
                "params": ["p3"],
            },
        ]

        timestamp = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        success = writer._upload_client_events("client-1", events, timestamp)

        assert success is True

        # Verify CSV content
        response = s3_client.get_object(
            Bucket="test-bucket-client-1", Key="events-2025-01-01-12.csv"
        )
        content = response["Body"].read().decode("utf-8")

        # Parse CSV
        reader = csv.DictReader(io.StringIO(content))
        rows = list(reader)

        assert len(rows) == 2
        assert rows[0]["eventId"] == "evt-1"
        assert rows[1]["eventId"] == "evt-2"
        # params should be JSON stringified in CSV
        assert json.loads(rows[0]["params"]) == ["p1", "p2"]

    def test_verify_bucket_exists(self, aws_credentials):
        """Test bucket existence verification."""
        writer = S3Writer("test-bucket", "us-east-1", "json")

        # Mock successful head_bucket
        with patch.object(writer.s3_client, "head_bucket") as mock_head:
            result = writer._verify_bucket_exists("existing-bucket")
            assert result is True
            mock_head.assert_called_once_with(Bucket="existing-bucket")

        # Mock 404 error (bucket doesn't exist)
        error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
        with patch.object(
            writer.s3_client,
            "head_bucket",
            side_effect=ClientError(error_response, "HeadBucket"),
        ):
            result = writer._verify_bucket_exists("non-existent-bucket")
            assert result is False

        # Mock other error
        error_response = {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}
        with patch.object(
            writer.s3_client,
            "head_bucket",
            side_effect=ClientError(error_response, "HeadBucket"),
        ):
            result = writer._verify_bucket_exists("forbidden-bucket")
            assert result is False

    def test_generate_file_key(self):
        """Test S3 file key generation."""
        writer = S3Writer("test-bucket", "us-east-1", "json")

        timestamp = datetime(2025, 1, 15, 14, 30, 0, tzinfo=timezone.utc)
        key = writer._generate_file_key(timestamp)

        assert key == "events-2025-01-15-14.json"

        # Test with CSV format
        writer.output_format = "csv"
        key = writer._generate_file_key(timestamp)
        assert key == "events-2025-01-15-14.csv"

    def test_convert_to_json(self):
        """Test JSON conversion."""
        writer = S3Writer("test-bucket", "us-east-1", "json")

        events = [
            {"eventId": "evt-1", "clientId": "client-1", "params": ["p1", "p2"]},
            {"eventId": "evt-2", "clientId": "client-1", "params": ["p3"]},
        ]

        json_str = writer._convert_to_json(events)
        parsed = json.loads(json_str)

        assert len(parsed) == 2
        assert parsed[0]["eventId"] == "evt-1"
        assert parsed[1]["params"] == ["p3"]

    def test_convert_to_csv(self):
        """Test CSV conversion."""
        writer = S3Writer("test-bucket", "us-east-1", "csv")

        events = [
            {
                "eventId": "evt-1",
                "clientId": "client-1",
                "time": "2025-01-01T12:00:00Z",
                "params": ["p1", "p2"],
            },
            {
                "eventId": "evt-2",
                "clientId": "client-2",
                "time": "2025-01-01T12:01:00Z",
                "data": "test-data",
            },
        ]

        csv_str = writer._convert_to_csv(events)

        # Parse CSV to verify
        reader = csv.DictReader(io.StringIO(csv_str))
        rows = list(reader)

        assert len(rows) == 2
        # Check all fields are present (union of all event keys)
        assert "eventId" in rows[0]
        assert "clientId" in rows[0]
        assert "time" in rows[0]
        assert "params" in rows[0]
        assert "data" in rows[0]  # From second event

        # Verify params is JSON stringified
        assert json.loads(rows[0]["params"]) == ["p1", "p2"]

    def test_convert_to_csv_empty_events(self):
        """Test CSV conversion with empty events list."""
        writer = S3Writer("test-bucket", "us-east-1", "csv")

        csv_str = writer._convert_to_csv([])
        assert csv_str == ""

    def test_upload_with_retry(self, aws_credentials):
        """Test upload with retry logic."""
        writer = S3Writer("test-bucket", "us-east-1", "json")

        content = '{"test": "data"}'

        # Test successful upload on first try
        with patch.object(writer.s3_client, "put_object") as mock_put:
            result = writer._upload_with_retry(
                "test-bucket", "test-key", content, "application/json"
            )
            assert result is True
            mock_put.assert_called_once()

        # Test retry on rate limiting
        call_count = 0

        def mock_put_with_retry(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise ClientError(
                    {"Error": {"Code": "SlowDown", "Message": "Slow down"}}, "PutObject"
                )
            return {}

        with patch.object(
            writer.s3_client, "put_object", side_effect=mock_put_with_retry
        ):
            with patch("time.sleep"):  # Mock sleep to speed up test
                result = writer._upload_with_retry(
                    "test-bucket", "test-key", content, "application/json"
                )
                assert result is True
                assert call_count == 3

        # Test max retries exceeded
        error_response = {"Error": {"Code": "SlowDown", "Message": "Slow down"}}
        with patch.object(
            writer.s3_client,
            "put_object",
            side_effect=ClientError(error_response, "PutObject"),
        ):
            with patch("time.sleep"):
                result = writer._upload_with_retry(
                    "test-bucket",
                    "test-key",
                    content,
                    "application/json",
                    max_retries=2,
                )
                assert result is False

    @mock_aws
    def test_concurrent_uploads(self, aws_credentials):
        """Test concurrent uploads to multiple buckets."""
        s3_client = boto3.client("s3", region_name="us-east-1")

        # Create multiple buckets
        for i in range(1, 6):
            s3_client.create_bucket(Bucket=f"test-bucket-client-{i}")

        writer = S3Writer("test-bucket", "us-east-1", "json")

        # Create events for multiple clients
        grouped_events = {}
        for i in range(1, 6):
            grouped_events[f"client-{i}"] = [
                {
                    "eventId": f"evt-{i}-1",
                    "clientId": f"client-{i}",
                    "time": "2025-01-01T12:00:00Z",
                },
                {
                    "eventId": f"evt-{i}-2",
                    "clientId": f"client-{i}",
                    "time": "2025-01-01T12:01:00Z",
                },
            ]

        # Test with limited concurrency
        results = writer.write_all_groups(grouped_events, max_concurrent=3)

        # All uploads should succeed
        assert all(results.values())
        assert writer.upload_stats["successful_uploads"] == 5
        assert writer.upload_stats["failed_uploads"] == 0

    def test_upload_with_missing_bucket(self, aws_credentials):
        """Test handling of missing S3 bucket."""
        writer = S3Writer("test-bucket", "us-east-1", "json")

        events = [
            {"eventId": "evt-1", "clientId": "client-1", "time": "2025-01-01T12:00:00Z"}
        ]

        # Mock bucket doesn't exist
        with patch.object(writer, "_verify_bucket_exists", return_value=False):
            timestamp = datetime.now(timezone.utc)
            success = writer._upload_client_events("client-1", events, timestamp)

            assert success is False

    def test_get_upload_statistics(self):
        """Test getting upload statistics."""
        writer = S3Writer("test-bucket", "us-east-1", "json")

        # Modify stats
        writer.upload_stats["successful_uploads"] = 10
        writer.upload_stats["failed_uploads"] = 2
        writer.upload_stats["total_size_bytes"] = 1024000

        stats = writer.get_upload_statistics()

        assert stats["successful_uploads"] == 10
        assert stats["failed_uploads"] == 2
        assert stats["total_size_bytes"] == 1024000

        # Verify it's a copy
        stats["successful_uploads"] = 999
        assert writer.upload_stats["successful_uploads"] == 10

    @mock_aws
    def test_server_side_encryption(self, aws_credentials):
        """Test that server-side encryption is applied."""
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket="test-bucket-client-1")

        writer = S3Writer("test-bucket", "us-east-1", "json")

        events = [
            {"eventId": "evt-1", "clientId": "client-1", "time": "2025-01-01T12:00:00Z"}
        ]

        # Spy on put_object to verify encryption parameter
        with patch.object(
            writer.s3_client, "put_object", wraps=writer.s3_client.put_object
        ) as mock_put:
            timestamp = datetime.now(timezone.utc)
            writer._upload_client_events("client-1", events, timestamp)

            # Verify ServerSideEncryption parameter was included
            call_args = mock_put.call_args[1]
            assert call_args["ServerSideEncryption"] == "AES256"

    def test_large_file_upload(self):
        """Test handling of large files."""
        writer = S3Writer("test-bucket", "us-east-1", "json")

        # Generate large dataset (1MB+ of JSON)
        large_events = []
        for i in range(10000):
            large_events.append(
                {
                    "eventId": f"evt-{i:08d}",
                    "clientId": "client-1",
                    "time": "2025-01-01T12:00:00Z",
                    "data": "x" * 100,  # Add some bulk to each event
                }
            )

        json_content = writer._convert_to_json(large_events)
        size_mb = len(json_content.encode("utf-8")) / (1024 * 1024)

        # Should handle large content without issues
        assert size_mb > 1.0  # Verify it's actually large
        assert json_content.startswith("[")
        assert json_content.endswith("]")
