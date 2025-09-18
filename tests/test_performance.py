"""Performance and stress tests for the event processor."""

import pytest
import time
import random
import string
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch
import concurrent.futures
import boto3
from moto import mock_aws

from dynamodb_reader import DynamoDBReader
from data_processor import DataProcessor
from s3_writer import S3Writer


class TestPerformance:
    """Performance and stress tests."""

    def generate_random_events(self, count: int, client_count: int = 10):
        """Generate random events for testing."""
        events = []
        current_time = datetime.now(timezone.utc)

        for i in range(count):
            client_id = f"client-{(i % client_count) + 1:03d}"
            # Distribute events across the past hour
            time_offset = random.randint(0, 3600)
            event_time = current_time - timedelta(seconds=time_offset)

            events.append(
                {
                    "eventId": f"evt-{i:08d}",
                    "clientId": client_id,
                    "time": event_time.isoformat(),
                    "params": [
                        "".join(random.choices(string.ascii_letters, k=10))
                        for _ in range(random.randint(1, 5))
                    ],
                    "data": "".join(
                        random.choices(string.ascii_letters + string.digits, k=100)
                    ),
                }
            )

        return events

    def test_data_processor_performance(self):
        """Test data processor with varying data sizes."""
        processor = DataProcessor()

        test_sizes = [100, 1000, 10000, 50000, 100000]
        results = []

        for size in test_sizes:
            events = self.generate_random_events(size)

            start_time = time.time()
            grouped = processor.process_events(events)
            processing_time = time.time() - start_time

            results.append(
                {
                    "size": size,
                    "time": processing_time,
                    "events_per_second": size / processing_time,
                }
            )

            # Performance assertions
            if size <= 10000:
                assert processing_time < 1.0  # Should be very fast for small datasets
            elif size <= 50000:
                assert processing_time < 3.0  # Reasonable time for medium datasets
            else:
                assert processing_time < 10.0  # Should handle 100k in under 10 seconds

        # Log performance results
        print("\nData Processor Performance:")
        for result in results:
            print(
                f"  {result['size']:6d} events: {result['time']:.3f}s "
                f"({result['events_per_second']:.0f} events/sec)"
            )

    @mock_aws
    def test_dynamodb_parallel_scan_performance(self, aws_credentials):
        """Test DynamoDB parallel scanning performance."""
        # Create and populate table
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamodb.create_table(
            TableName="perf-test-events",
            KeySchema=[{"AttributeName": "eventId", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "eventId", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        table.meta.client.get_waiter("table_exists").wait(TableName="perf-test-events")

        # Add test data
        events = self.generate_random_events(5000)
        with table.batch_writer() as batch:
            for event in events:
                batch.put_item(Item=event)

        # Test with different parallel segment counts
        segment_counts = [1, 2, 4, 8, 16]
        results = []

        for segments in segment_counts:
            reader = DynamoDBReader(
                "perf-test-events", "us-east-1", parallel_segments=segments
            )

            start_time = time.time()
            fetched_events = reader.get_events_past_hour()
            scan_time = time.time() - start_time

            results.append(
                {"segments": segments, "time": scan_time, "events": len(fetched_events)}
            )

        # Log results
        print("\nDynamoDB Parallel Scan Performance (5000 events):")
        for result in results:
            print(f"  {result['segments']:2d} segments: {result['time']:.3f}s")

        # Performance should improve with more segments (to a point)
        assert results[0]["time"] > results[2]["time"]  # 4 segments faster than 1

    @mock_aws
    def test_s3_concurrent_upload_performance(self, aws_credentials):
        """Test S3 concurrent upload performance."""
        # Create S3 buckets
        s3_client = boto3.client("s3", region_name="us-east-1")
        for i in range(1, 11):
            s3_client.create_bucket(Bucket=f"perf-test-client-{i:03d}")

        writer = S3Writer("perf-test", "us-east-1", "json")

        # Generate grouped events (10k events per client)
        grouped_events = {}
        for i in range(1, 11):
            grouped_events[f"client-{i:03d}"] = self.generate_random_events(
                10000, client_count=1
            )

        # Test with different concurrency levels
        concurrency_levels = [1, 5, 10, 20]
        results = []

        for concurrency in concurrency_levels:
            # Reset stats
            writer.upload_stats = {
                "successful_uploads": 0,
                "failed_uploads": 0,
                "total_size_bytes": 0,
            }

            start_time = time.time()
            upload_results = writer.write_all_groups(
                grouped_events, max_concurrent=concurrency
            )
            upload_time = time.time() - start_time

            results.append(
                {
                    "concurrency": concurrency,
                    "time": upload_time,
                    "success_rate": sum(1 for v in upload_results.values() if v)
                    / len(upload_results),
                }
            )

        # Log results
        print("\nS3 Concurrent Upload Performance (10 clients, 10k events each):")
        for result in results:
            print(
                f"  Concurrency {result['concurrency']:2d}: {result['time']:.3f}s "
                f"(success rate: {result['success_rate']:.0%})"
            )

        # Higher concurrency should be faster (to a point)
        assert results[0]["time"] > results[2]["time"]  # 10 concurrent faster than 1

    def test_memory_efficiency(self):
        """Test memory efficiency with large datasets."""
        import tracemalloc

        processor = DataProcessor()

        # Generate large dataset
        events = self.generate_random_events(100000)

        # Start memory tracking
        tracemalloc.start()
        snapshot_start = tracemalloc.take_snapshot()

        # Process events
        grouped = processor.process_events(events)

        # Take memory snapshot
        snapshot_end = tracemalloc.take_snapshot()
        tracemalloc.stop()

        # Calculate memory usage
        stats = snapshot_end.compare_to(snapshot_start, "lineno")
        total_memory = sum(stat.size_diff for stat in stats) / (
            1024 * 1024
        )  # Convert to MB

        print(f"\nMemory usage for 100k events: {total_memory:.2f} MB")

        # Memory usage should be reasonable (less than 500MB for 100k events)
        assert total_memory < 500

    def test_concurrent_request_handling(self):
        """Test handling of concurrent processing requests."""
        processor = DataProcessor()

        # Generate different event sets
        event_sets = [
            self.generate_random_events(1000, client_count=5) for _ in range(10)
        ]

        results = []
        errors = []

        def process_events(events):
            try:
                start = time.time()
                result = processor.process_events(events)
                return {
                    "success": True,
                    "time": time.time() - start,
                    "events": len(events),
                }
            except Exception as e:
                return {"success": False, "error": str(e)}

        # Process concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(process_events, events) for events in event_sets]

            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result["success"]:
                    results.append(result)
                else:
                    errors.append(result)

        # All should succeed
        assert len(results) == 10
        assert len(errors) == 0

        # Log results
        avg_time = sum(r["time"] for r in results) / len(results)
        print(f"\nConcurrent processing average time: {avg_time:.3f}s")

    def test_rate_limiting_resilience(self):
        """Test resilience to rate limiting."""
        reader = DynamoDBReader("test-table", "us-east-1", parallel_segments=4)

        # Mock scan with intermittent rate limiting
        call_count = [0]

        def mock_scan(**kwargs):
            call_count[0] += 1
            # Fail every 3rd call
            if call_count[0] % 3 == 0:
                from botocore.exceptions import ClientError

                raise ClientError(
                    {"Error": {"Code": "ProvisionedThroughputExceededException"}},
                    "Scan",
                )
            return {"Items": [{"eventId": f"evt-{call_count[0]}"}]}

        with patch.object(reader.table, "scan", side_effect=mock_scan):
            with patch("time.sleep", return_value=None):  # Speed up test
                start_time = time.time()
                events = reader._scan_segment(
                    0,
                    datetime.now(timezone.utc) - timedelta(hours=1),
                    datetime.now(timezone.utc),
                )
                elapsed = time.time() - start_time

                # Should handle rate limiting and complete
                assert len(events) > 0
                assert elapsed < 5.0  # Should complete reasonably quickly

    def test_scaling_characteristics(self):
        """Test how the system scales with different parameters."""
        processor = DataProcessor()

        scaling_results = []

        # Test different client counts with fixed total events
        total_events = 50000
        client_counts = [1, 5, 10, 20, 50, 100]

        for client_count in client_counts:
            events = self.generate_random_events(
                total_events, client_count=client_count
            )

            start_time = time.time()
            grouped = processor.group_events_by_client(events)
            processing_time = time.time() - start_time

            scaling_results.append(
                {
                    "clients": client_count,
                    "events_per_client": total_events // client_count,
                    "time": processing_time,
                }
            )

        # Log scaling results
        print(f"\nScaling Characteristics ({total_events} total events):")
        for result in scaling_results:
            print(
                f"  {result['clients']:3d} clients "
                f"({result['events_per_client']:5d} events/client): "
                f"{result['time']:.3f}s"
            )

        # Processing time should not vary dramatically with client count
        times = [r["time"] for r in scaling_results]
        assert max(times) / min(times) < 3.0  # Max should be less than 3x min

    @pytest.mark.slow
    def test_sustained_load(self):
        """Test sustained processing load over time."""
        processor = DataProcessor()

        # Simulate hourly processing for 24 iterations
        iteration_times = []

        for hour in range(24):
            # Generate varying load (simulating daily patterns)
            if 8 <= hour <= 20:  # Business hours
                event_count = random.randint(80000, 120000)
            else:  # Off hours
                event_count = random.randint(20000, 40000)

            events = self.generate_random_events(event_count)

            start_time = time.time()
            grouped = processor.process_events(events)
            processing_time = time.time() - start_time

            iteration_times.append(
                {
                    "hour": hour,
                    "events": event_count,
                    "time": processing_time,
                    "rate": event_count / processing_time,
                }
            )

            # Ensure no performance degradation over time
            if hour > 0:
                # Compare with average of previous iterations
                avg_rate = sum(t["rate"] for t in iteration_times[:-1]) / len(
                    iteration_times[:-1]
                )
                current_rate = iteration_times[-1]["rate"]
                # Current rate should not be less than 80% of average
                assert current_rate > avg_rate * 0.8

        # Log sustained load results
        print("\nSustained Load Test (24 hours simulation):")
        print(f"  Total events processed: {sum(t['events'] for t in iteration_times)}")
        print(
            f"  Average processing rate: {sum(t['rate'] for t in iteration_times) / 24:.0f} events/sec"
        )
        print(f"  Min rate: {min(t['rate'] for t in iteration_times):.0f} events/sec")
        print(f"  Max rate: {max(t['rate'] for t in iteration_times):.0f} events/sec")
