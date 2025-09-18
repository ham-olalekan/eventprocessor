"""Tests for data processor."""

import pytest
from datetime import datetime, timezone

from data_processor import DataProcessor


class TestDataProcessor:
    """Test data processing functionality."""

    def test_init(self):
        """Test DataProcessor initialization."""
        processor = DataProcessor()

        assert processor.stats["total_events"] == 0
        assert processor.stats["unique_clients"] == 0
        assert processor.stats["events_per_client"] == {}

    def test_group_events_by_client(self, sample_events_small):
        """Test grouping events by clientId."""
        processor = DataProcessor()
        grouped = processor.group_events_by_client(sample_events_small)

        # Check that events are correctly grouped
        assert len(grouped) == 3  # 3 unique clients in sample data
        assert "client-001" in grouped
        assert "client-002" in grouped
        assert "client-003" in grouped

        # Verify event counts
        total_events = sum(len(events) for events in grouped.values())
        assert total_events == 100

        # Check statistics
        assert processor.stats["total_events"] == 100
        assert processor.stats["unique_clients"] == 3

    def test_group_events_missing_client_id(self):
        """Test handling of events with missing clientId."""
        processor = DataProcessor()

        events = [
            {
                "eventId": "evt-1",
                "clientId": "client-1",
                "time": datetime.now(timezone.utc).isoformat(),
            },
            {
                "eventId": "evt-2",
                "time": datetime.now(timezone.utc).isoformat(),
            },  # Missing clientId
            {
                "eventId": "evt-3",
                "clientId": "client-2",
                "time": datetime.now(timezone.utc).isoformat(),
            },
            {
                "eventId": "evt-4",
                "clientId": None,
                "time": datetime.now(timezone.utc).isoformat(),
            },  # None clientId
        ]

        grouped = processor.group_events_by_client(events)

        # Only events with valid clientId should be grouped
        assert len(grouped) == 2
        assert "client-1" in grouped
        assert "client-2" in grouped
        assert len(grouped["client-1"]) == 1
        assert len(grouped["client-2"]) == 1

    def test_validate_event(self):
        """Test event validation."""
        processor = DataProcessor()

        # Valid event
        valid_event = {
            "eventId": "evt-1",
            "clientId": "client-1",
            "time": datetime.now(timezone.utc).isoformat(),
            "params": ["param1"],
        }
        assert processor.validate_event(valid_event) is True

        # Missing eventId
        invalid_event1 = {
            "clientId": "client-1",
            "time": datetime.now(timezone.utc).isoformat(),
        }
        assert processor.validate_event(invalid_event1) is False

        # Missing clientId
        invalid_event2 = {
            "eventId": "evt-1",
            "time": datetime.now(timezone.utc).isoformat(),
        }
        assert processor.validate_event(invalid_event2) is False

        # Missing time
        invalid_event3 = {"eventId": "evt-1", "clientId": "client-1"}
        assert processor.validate_event(invalid_event3) is False

        # None values
        invalid_event4 = {
            "eventId": None,
            "clientId": "client-1",
            "time": datetime.now(timezone.utc).isoformat(),
        }
        assert processor.validate_event(invalid_event4) is False

    def test_filter_valid_events(self, invalid_events):
        """Test filtering of invalid events."""
        processor = DataProcessor()

        valid_events = processor.filter_valid_events(invalid_events)

        # Only 1 valid event in the invalid_events fixture
        assert len(valid_events) == 1
        assert valid_events[0]["eventId"] == "evt-valid-001"

    def test_sort_events_by_time(self):
        """Test sorting events within groups by time."""
        processor = DataProcessor()

        # Create unsorted events
        events = {
            "client-1": [
                {"eventId": "evt-3", "time": "2025-01-01T12:30:00Z"},
                {"eventId": "evt-1", "time": "2025-01-01T12:00:00Z"},
                {"eventId": "evt-2", "time": "2025-01-01T12:15:00Z"},
            ],
            "client-2": [
                {"eventId": "evt-5", "time": "2025-01-01T13:00:00Z"},
                {"eventId": "evt-4", "time": "2025-01-01T12:45:00Z"},
            ],
        }

        sorted_events = processor.sort_events_by_time(events)

        # Check client-1 events are sorted
        assert sorted_events["client-1"][0]["eventId"] == "evt-1"
        assert sorted_events["client-1"][1]["eventId"] == "evt-2"
        assert sorted_events["client-1"][2]["eventId"] == "evt-3"

        # Check client-2 events are sorted
        assert sorted_events["client-2"][0]["eventId"] == "evt-4"
        assert sorted_events["client-2"][1]["eventId"] == "evt-5"

    def test_process_events_pipeline(self, sample_events_small):
        """Test the complete processing pipeline."""
        processor = DataProcessor()

        # Add some invalid events to test filtering
        invalid_event = {"eventId": None, "time": "2025-01-01T12:00:00Z"}
        all_events = sample_events_small + [invalid_event]

        result = processor.process_events(all_events)

        # Check that result is grouped and sorted
        assert len(result) == 3  # 3 unique clients
        assert processor.stats["total_events"] == 100  # Invalid event filtered out

        # Verify events are sorted within each group
        for client_id, events in result.items():
            for i in range(1, len(events)):
                assert events[i - 1]["time"] <= events[i]["time"]

    def test_get_statistics(self):
        """Test getting processing statistics."""
        processor = DataProcessor()

        # Process some events
        events = [
            {
                "eventId": f"evt-{i}",
                "clientId": f"client-{i % 3}",
                "time": "2025-01-01T12:00:00Z",
            }
            for i in range(30)
        ]

        processor.group_events_by_client(events)
        stats = processor.get_statistics()

        assert stats["total_events"] == 30
        assert stats["unique_clients"] == 3
        assert len(stats["events_per_client"]) == 3

        # Verify it's a copy, not a reference
        stats["total_events"] = 999
        assert processor.stats["total_events"] == 30

    def test_large_dataset_performance(self):
        """Test processing performance with large dataset."""
        processor = DataProcessor()

        # Generate 100k events across 10 clients
        large_dataset = []
        for i in range(100000):
            large_dataset.append(
                {
                    "eventId": f"evt-{i:08d}",
                    "clientId": f"client-{i % 10:03d}",
                    "time": f"2025-01-01T{12 + (i % 12):02d}:00:00Z",
                    "params": [f"param-{i}"],
                }
            )

        import time

        start_time = time.time()

        result = processor.process_events(large_dataset)

        processing_time = time.time() - start_time

        # Should process 100k events reasonably quickly
        assert processing_time < 5.0  # Should take less than 5 seconds
        assert len(result) == 10
        assert processor.stats["total_events"] == 100000

        # Verify distribution is roughly even
        for client_id, events in result.items():
            assert 9000 <= len(events) <= 11000  # Allow some variance

    def test_empty_events_list(self):
        """Test handling of empty events list."""
        processor = DataProcessor()

        result = processor.process_events([])

        assert result == {}
        assert processor.stats["total_events"] == 0
        assert processor.stats["unique_clients"] == 0

    def test_duplicate_event_ids(self):
        """Test handling of duplicate eventIds."""
        processor = DataProcessor()

        events = [
            {
                "eventId": "evt-1",
                "clientId": "client-1",
                "time": "2025-01-01T12:00:00Z",
            },
            {
                "eventId": "evt-1",
                "clientId": "client-1",
                "time": "2025-01-01T12:01:00Z",
            },  # Duplicate ID
            {
                "eventId": "evt-2",
                "clientId": "client-1",
                "time": "2025-01-01T12:02:00Z",
            },
        ]

        result = processor.group_events_by_client(events)

        # All events should be included (processor doesn't deduplicate)
        assert len(result["client-1"]) == 3

    def test_special_characters_in_client_id(self):
        """Test handling of special characters in clientId."""
        processor = DataProcessor()

        events = [
            {
                "eventId": "evt-1",
                "clientId": "client-with-dash",
                "time": "2025-01-01T12:00:00Z",
            },
            {
                "eventId": "evt-2",
                "clientId": "client.with.dots",
                "time": "2025-01-01T12:00:00Z",
            },
            {
                "eventId": "evt-3",
                "clientId": "client_with_underscore",
                "time": "2025-01-01T12:00:00Z",
            },
            {
                "eventId": "evt-4",
                "clientId": "client@special#chars",
                "time": "2025-01-01T12:00:00Z",
            },
        ]

        result = processor.group_events_by_client(events)

        assert len(result) == 4
        assert "client-with-dash" in result
        assert "client.with.dots" in result
        assert "client_with_underscore" in result
        assert "client@special#chars" in result
