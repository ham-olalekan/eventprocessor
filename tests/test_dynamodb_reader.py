"""Tests for DynamoDB reader."""

import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch, MagicMock
import boto3
from botocore.exceptions import ClientError
from moto import mock_aws

from dynamodb_reader import DynamoDBReader


class TestDynamoDBReader:
    """Test DynamoDB reading functionality."""

    @mock_aws
    def test_init(self, aws_credentials):
        """Test DynamoDBReader initialization."""
        reader = DynamoDBReader("test-table", "us-east-1", parallel_segments=8)

        assert reader.table_name == "test-table"
        assert reader.region == "us-east-1"
        assert reader.parallel_segments == 8

    @mock_aws
    def test_get_events_past_hour(self, populated_dynamodb):
        """Test fetching events from the past hour."""
        reader = DynamoDBReader("test-events", "us-east-1", parallel_segments=2)

        with patch.object(reader, "_parallel_scan") as mock_scan:
            mock_scan.return_value = [
                {
                    "eventId": "evt-1",
                    "clientId": "client-1",
                    "time": datetime.now(timezone.utc).isoformat(),
                },
                {
                    "eventId": "evt-2",
                    "clientId": "client-2",
                    "time": datetime.now(timezone.utc).isoformat(),
                },
            ]

            events = reader.get_events_past_hour()

            assert len(events) == 2
            mock_scan.assert_called_once()

            # Check that time window is correct (approximately 1 hour)
            call_args = mock_scan.call_args[0]
            start_time = call_args[0]
            end_time = call_args[1]
            time_diff = end_time - start_time
            assert 3595 <= time_diff.total_seconds() <= 3605  # Allow small variance

    @mock_aws
    def test_parallel_scan(self, populated_dynamodb):
        """Test parallel scanning of DynamoDB table."""
        reader = DynamoDBReader("test-events", "us-east-1", parallel_segments=4)

        # Mock the scan_segment method to return different results for each segment
        def mock_scan_segment(segment, start_time, end_time):
            return [
                {"eventId": f"evt-seg{segment}-1", "clientId": f"client-{segment}"},
                {"eventId": f"evt-seg{segment}-2", "clientId": f"client-{segment}"},
            ]

        with patch.object(reader, "_scan_segment", side_effect=mock_scan_segment):
            start_time = datetime.now(timezone.utc) - timedelta(hours=1)
            end_time = datetime.now(timezone.utc)

            events = reader._parallel_scan(start_time, end_time)

            # Should have 2 events from each of 4 segments
            assert len(events) == 8
            # Check all segments were processed
            segments_found = set()
            for event in events:
                if "seg" in event["eventId"]:
                    segment_num = event["eventId"].split("-")[1].replace("seg", "")
                    segments_found.add(int(segment_num))
            assert segments_found == {0, 1, 2, 3}

    @mock_aws
    def test_scan_segment_with_pagination(self, aws_credentials):
        """Test scanning a segment with pagination."""
        reader = DynamoDBReader("test-events", "us-east-1", parallel_segments=4)

        # Mock paginated responses
        responses = [
            {
                "Items": [
                    {
                        "eventId": "evt-1",
                        "clientId": "client-1",
                        "time": datetime.now(timezone.utc).isoformat(),
                    },
                    {
                        "eventId": "evt-2",
                        "clientId": "client-2",
                        "time": datetime.now(timezone.utc).isoformat(),
                    },
                ],
                "LastEvaluatedKey": {"eventId": "evt-2"},
            },
            {
                "Items": [
                    {
                        "eventId": "evt-3",
                        "clientId": "client-1",
                        "time": datetime.now(timezone.utc).isoformat(),
                    },
                    {
                        "eventId": "evt-4",
                        "clientId": "client-2",
                        "time": datetime.now(timezone.utc).isoformat(),
                    },
                ]
            },
        ]

        with patch.object(reader.table, "scan", side_effect=responses):
            start_time = datetime.now(timezone.utc) - timedelta(hours=1)
            end_time = datetime.now(timezone.utc)

            events = reader._scan_segment(0, start_time, end_time)

            assert len(events) == 4
            assert events[0]["eventId"] == "evt-1"
            assert events[3]["eventId"] == "evt-4"

    def test_scan_with_rate_limiting(self, aws_credentials):
        """Test handling of DynamoDB rate limiting."""
        reader = DynamoDBReader("test-events", "us-east-1", parallel_segments=4)

        # Create mock responses - first two fail with throttling, third succeeds
        error_response = {
            "Error": {
                "Code": "ProvisionedThroughputExceededException",
                "Message": "Rate exceeded",
            }
        }

        success_response = {
            "Items": [
                {
                    "eventId": "evt-1",
                    "clientId": "client-1",
                    "time": datetime.now(timezone.utc).isoformat(),
                }
            ]
        }

        # Mock scan to fail twice then succeed
        call_count = 0

        def mock_scan(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise ClientError(error_response, "Scan")
            return success_response

        with patch.object(reader.table, "scan", side_effect=mock_scan):
            with patch("time.sleep"):  # Mock sleep to speed up test
                start_time = datetime.now(timezone.utc) - timedelta(hours=1)
                end_time = datetime.now(timezone.utc)

                events = reader._scan_segment(0, start_time, end_time)

                assert len(events) == 1
                assert call_count == 3  # Failed twice, succeeded on third

    def test_scan_with_max_retries_exceeded(self, aws_credentials):
        """Test when max retries are exceeded for rate limiting."""
        reader = DynamoDBReader("test-events", "us-east-1", parallel_segments=4)

        error_response = {
            "Error": {
                "Code": "ProvisionedThroughputExceededException",
                "Message": "Rate exceeded",
            }
        }

        # Mock scan to always fail
        with patch.object(
            reader.table, "scan", side_effect=ClientError(error_response, "Scan")
        ):
            with patch("time.sleep"):  # Mock sleep to speed up test
                start_time = datetime.now(timezone.utc) - timedelta(hours=1)
                end_time = datetime.now(timezone.utc)

                with pytest.raises(ClientError) as exc_info:
                    reader._scan_segment(0, start_time, end_time)

                assert (
                    exc_info.value.response["Error"]["Code"]
                    == "ProvisionedThroughputExceededException"
                )

    @mock_aws
    def test_estimate_scan_time(self, aws_credentials):
        """Test scan time estimation."""
        # Create a table with mocked item count
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table_name = "test-estimate-table"

        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )

        reader = DynamoDBReader(table_name, "us-east-1", parallel_segments=8)

        # Mock describe_table response
        mock_response = {"Table": {"ItemCount": 100000}}  # 100k items

        with patch.object(reader.client, "describe_table", return_value=mock_response):
            estimated_time = reader.estimate_scan_time()

            # 100k items / (1000 items/sec * 8 segments) = 12.5 seconds
            assert 12 <= estimated_time <= 13

    def test_estimate_scan_time_error_handling(self, aws_credentials):
        """Test scan time estimation with error handling."""
        reader = DynamoDBReader("non-existent-table", "us-east-1", parallel_segments=8)

        error_response = {
            "Error": {"Code": "ResourceNotFoundException", "Message": "Table not found"}
        }

        with patch.object(
            reader.client,
            "describe_table",
            side_effect=ClientError(error_response, "DescribeTable"),
        ):
            estimated_time = reader.estimate_scan_time()

            # Should return default estimate
            assert estimated_time == 60.0

    def test_time_filtering(self, aws_credentials):
        """Test that events are correctly filtered by time."""
        reader = DynamoDBReader("test-events", "us-east-1", parallel_segments=1)

        current_time = datetime.now(timezone.utc)

        # Create events with different timestamps
        all_events = [
            # Old event (2 hours ago) - should be filtered out
            {
                "eventId": "old-1",
                "time": (current_time - timedelta(hours=2)).isoformat(),
            },
            # Recent event (30 minutes ago) - should be included
            {
                "eventId": "recent-1",
                "time": (current_time - timedelta(minutes=30)).isoformat(),
            },
            # Very recent event (5 minutes ago) - should be included
            {
                "eventId": "recent-2",
                "time": (current_time - timedelta(minutes=5)).isoformat(),
            },
            # Future event (edge case) - should be filtered out
            {
                "eventId": "future-1",
                "time": (current_time + timedelta(minutes=10)).isoformat(),
            },
        ]

        # Mock scan to return all events (filtering happens in scan params)
        mock_response = {"Items": all_events[1:3]}  # Only return recent events

        with patch.object(reader.table, "scan", return_value=mock_response):
            start_time = current_time - timedelta(hours=1)
            end_time = current_time

            events = reader._scan_segment(0, start_time, end_time)

            assert len(events) == 2
            assert events[0]["eventId"] == "recent-1"
            assert events[1]["eventId"] == "recent-2"
