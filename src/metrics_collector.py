"""Comprehensive metrics collection for DynamoDB to S3 Event Processor"""

import time
import boto3
import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from collections import defaultdict
import threading

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False


@dataclass
class ProcessingMetrics:
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    events_processed: int = 0
    events_per_second: float = 0.0
    memory_usage_mb: float = 0.0
    peak_memory_mb: float = 0.0
    grouping_time_seconds: float = 0.0
    processing_stages: Dict[str, float] = field(default_factory=dict)
    error_count: int = 0

    @property
    def duration_seconds(self) -> float:
        end = self.end_time or time.time()
        return end - self.start_time


@dataclass
class S3UploadMetrics:
    client_id: str
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    file_size_bytes: int = 0
    upload_success: bool = False
    retry_count: int = 0
    error_message: Optional[str] = None

    @property
    def duration_seconds(self) -> float:
        end = self.end_time or time.time()
        return end - self.start_time

    @property
    def throughput_mbps(self) -> float:
        if self.duration_seconds > 0 and self.file_size_bytes > 0:
            return (self.file_size_bytes / 1024 / 1024) / self.duration_seconds
        return 0.0


class MetricsCollector:
    def __init__(self, region: str = 'us-east-1', enable_cloudwatch: bool = True):
        self.region = region
        self.enable_cloudwatch = enable_cloudwatch
        self.logger = logging.getLogger(__name__)

        if self.enable_cloudwatch:
            try:
                self.cloudwatch = boto3.client('cloudwatch', region_name=region)
            except Exception as e:
                self.logger.warning(f"Failed to initialize CloudWatch client: {e}")
                self.enable_cloudwatch = False
                self.cloudwatch = None
        else:
            self.cloudwatch = None

        self.processing_metrics = ProcessingMetrics()
        self.s3_upload_metrics: Dict[str, S3UploadMetrics] = {}
        self.stage_timers: Dict[str, float] = {}
        self._lock = threading.Lock()

        if PSUTIL_AVAILABLE:
            self.process = psutil.Process()
        else:
            self.process = None

    def start_processing_timer(self):
        self.processing_metrics.start_time = time.time()
        self.processing_metrics.memory_usage_mb = self._get_memory_usage_mb()
        self.logger.info("Started processing metrics collection")

    def end_processing_timer(self, events_processed: int):
        self.processing_metrics.end_time = time.time()
        self.processing_metrics.events_processed = events_processed

        duration = self.processing_metrics.duration_seconds
        if duration > 0:
            self.processing_metrics.events_per_second = events_processed / duration

        current_memory = self._get_memory_usage_mb()
        self.processing_metrics.peak_memory_mb = max(
            self.processing_metrics.peak_memory_mb,
            current_memory
        )

        self.logger.info(f"Processing completed: {events_processed:,} events in {duration:.2f}s "
                        f"({self.processing_metrics.events_per_second:.0f} events/sec)")

    def start_stage_timer(self, stage_name: str):
        self.stage_timers[stage_name] = time.time()

    def end_stage_timer(self, stage_name: str):
        if stage_name in self.stage_timers:
            duration = time.time() - self.stage_timers[stage_name]
            self.processing_metrics.processing_stages[stage_name] = duration
            self.logger.info(f"Stage '{stage_name}' completed in {duration:.2f}s")
            del self.stage_timers[stage_name]

    def record_grouping_time(self, duration_seconds: float):
        self.processing_metrics.grouping_time_seconds = duration_seconds
        self.logger.info(f"Event grouping completed in {duration_seconds:.2f}s")

    def start_s3_upload(self, client_id: str) -> S3UploadMetrics:
        upload_metrics = S3UploadMetrics(client_id=client_id)
        with self._lock:
            self.s3_upload_metrics[client_id] = upload_metrics
        return upload_metrics

    def end_s3_upload(self, client_id: str, file_size_bytes: int,
                     success: bool, error_message: Optional[str] = None):
        with self._lock:
            if client_id in self.s3_upload_metrics:
                metrics = self.s3_upload_metrics[client_id]
                metrics.end_time = time.time()
                metrics.file_size_bytes = file_size_bytes
                metrics.upload_success = success
                metrics.error_message = error_message

                if success:
                    self.logger.info(f"S3 upload {client_id}: {file_size_bytes:,} bytes in "
                                   f"{metrics.duration_seconds:.2f}s ({metrics.throughput_mbps:.1f} MB/s)")
                else:
                    self.logger.error(f"S3 upload {client_id} failed: {error_message}")

    def record_s3_retry(self, client_id: str):
        with self._lock:
            if client_id in self.s3_upload_metrics:
                self.s3_upload_metrics[client_id].retry_count += 1

    def record_processing_error(self, error_message: str):
        self.processing_metrics.error_count += 1
        self.logger.error(f"Processing error recorded: {error_message}")

    def update_memory_usage(self):
        current_memory = self._get_memory_usage_mb()
        self.processing_metrics.memory_usage_mb = current_memory
        self.processing_metrics.peak_memory_mb = max(
            self.processing_metrics.peak_memory_mb,
            current_memory
        )

    def get_processing_summary(self) -> Dict[str, Any]:
        s3_metrics = list(self.s3_upload_metrics.values())
        successful_uploads = [m for m in s3_metrics if m.upload_success]
        failed_uploads = [m for m in s3_metrics if not m.upload_success]

        total_file_size = sum(m.file_size_bytes for m in successful_uploads)
        total_upload_time = sum(m.duration_seconds for m in s3_metrics)
        avg_upload_time = total_upload_time / len(s3_metrics) if s3_metrics else 0
        total_retries = sum(m.retry_count for m in s3_metrics)

        overall_throughput_mbps = 0.0
        if total_upload_time > 0 and total_file_size > 0:
            overall_throughput_mbps = (total_file_size / 1024 / 1024) / total_upload_time

        return {
            'processing_metrics': {
                'duration_seconds': self.processing_metrics.duration_seconds,
                'events_processed': self.processing_metrics.events_processed,
                'events_per_second': self.processing_metrics.events_per_second,
                'memory_usage_mb': self.processing_metrics.memory_usage_mb,
                'peak_memory_mb': self.processing_metrics.peak_memory_mb,
                'grouping_time_seconds': self.processing_metrics.grouping_time_seconds,
                'processing_stages': self.processing_metrics.processing_stages,
                'error_count': self.processing_metrics.error_count
            },
            's3_upload_metrics': {
                'total_uploads': len(s3_metrics),
                'successful_uploads': len(successful_uploads),
                'failed_uploads': len(failed_uploads),
                'success_rate_percent': (len(successful_uploads) / len(s3_metrics) * 100) if s3_metrics else 0,
                'total_file_size_bytes': total_file_size,
                'total_file_size_mb': total_file_size / 1024 / 1024,
                'average_upload_time_seconds': avg_upload_time,
                'overall_throughput_mbps': overall_throughput_mbps,
                'total_retries': total_retries,
                'client_details': {
                    m.client_id: {
                        'duration_seconds': m.duration_seconds,
                        'file_size_bytes': m.file_size_bytes,
                        'throughput_mbps': m.throughput_mbps,
                        'success': m.upload_success,
                        'retry_count': m.retry_count,
                        'error_message': m.error_message
                    } for m in s3_metrics
                }
            },
            'timestamp': datetime.now(timezone.utc).isoformat()
        }

    def publish_cloudwatch_metrics(self, namespace: str = 'EventProcessor'):
        if not self.enable_cloudwatch or not self.cloudwatch:
            self.logger.warning("CloudWatch publishing disabled or unavailable")
            return

        try:
            metrics_data = []

            metrics_data.extend([
                {
                    'MetricName': 'EventsProcessed',
                    'Value': self.processing_metrics.events_processed,
                    'Unit': 'Count',
                    'Timestamp': datetime.now(timezone.utc)
                },
                {
                    'MetricName': 'ProcessingRate',
                    'Value': self.processing_metrics.events_per_second,
                    'Unit': 'Count/Second',
                    'Timestamp': datetime.now(timezone.utc)
                },
                {
                    'MetricName': 'ProcessingDuration',
                    'Value': self.processing_metrics.duration_seconds,
                    'Unit': 'Seconds',
                    'Timestamp': datetime.now(timezone.utc)
                },
                {
                    'MetricName': 'PeakMemoryUsage',
                    'Value': self.processing_metrics.peak_memory_mb,
                    'Unit': 'Megabytes',
                    'Timestamp': datetime.now(timezone.utc)
                },
                {
                    'MetricName': 'GroupingTime',
                    'Value': self.processing_metrics.grouping_time_seconds,
                    'Unit': 'Seconds',
                    'Timestamp': datetime.now(timezone.utc)
                }
            ])

            s3_metrics = list(self.s3_upload_metrics.values())
            successful_uploads = len([m for m in s3_metrics if m.upload_success])
            failed_uploads = len([m for m in s3_metrics if not m.upload_success])
            total_file_size = sum(m.file_size_bytes for m in s3_metrics if m.upload_success)
            total_retries = sum(m.retry_count for m in s3_metrics)

            metrics_data.extend([
                {
                    'MetricName': 'S3UploadSuccess',
                    'Value': successful_uploads,
                    'Unit': 'Count',
                    'Timestamp': datetime.now(timezone.utc)
                },
                {
                    'MetricName': 'S3UploadFailures',
                    'Value': failed_uploads,
                    'Unit': 'Count',
                    'Timestamp': datetime.now(timezone.utc)
                },
                {
                    'MetricName': 'S3TotalDataUploaded',
                    'Value': total_file_size,
                    'Unit': 'Bytes',
                    'Timestamp': datetime.now(timezone.utc)
                },
                {
                    'MetricName': 'S3UploadRetries',
                    'Value': total_retries,
                    'Unit': 'Count',
                    'Timestamp': datetime.now(timezone.utc)
                }
            ])

            for i in range(0, len(metrics_data), 20):
                batch = metrics_data[i:i+20]
                self.cloudwatch.put_metric_data(
                    Namespace=namespace,
                    MetricData=batch
                )

            self.logger.info(f"Published {len(metrics_data)} metrics to CloudWatch namespace '{namespace}'")

        except Exception as e:
            self.logger.error(f"Failed to publish CloudWatch metrics: {e}")

    def log_detailed_metrics(self):
        summary = self.get_processing_summary()

        self.logger.info("DETAILED METRICS SUMMARY")
        self.logger.info("=" * 50)

        proc = summary['processing_metrics']
        self.logger.info(f"Processing Performance:")
        self.logger.info(f"   Duration: {proc['duration_seconds']:.2f} seconds")
        self.logger.info(f"   Events Processed: {proc['events_processed']:,}")
        self.logger.info(f"   Processing Rate: {proc['events_per_second']:.0f} events/second")
        self.logger.info(f"   Peak Memory: {proc['peak_memory_mb']:.1f} MB")
        self.logger.info(f"   Grouping Time: {proc['grouping_time_seconds']:.2f} seconds")

        if proc['processing_stages']:
            self.logger.info(f"   Stage Timings:")
            for stage, duration in proc['processing_stages'].items():
                self.logger.info(f"     {stage}: {duration:.2f}s")

        s3 = summary['s3_upload_metrics']
        self.logger.info(f"S3 Upload Performance:")
        self.logger.info(f"   Total Uploads: {s3['total_uploads']}")
        self.logger.info(f"   Success Rate: {s3['success_rate_percent']:.1f}%")
        self.logger.info(f"   Total Data: {s3['total_file_size_mb']:.1f} MB")
        self.logger.info(f"   Avg Upload Time: {s3['average_upload_time_seconds']:.2f} seconds")
        self.logger.info(f"   Overall Throughput: {s3['overall_throughput_mbps']:.1f} MB/s")
        self.logger.info(f"   Total Retries: {s3['total_retries']}")

        self.logger.info("=" * 50)

    def _get_memory_usage_mb(self) -> float:
        try:
            if PSUTIL_AVAILABLE and self.process:
                memory_info = self.process.memory_info()
                return memory_info.rss / 1024 / 1024
            else:
                with open('/proc/self/status', 'r') as f:
                    for line in f:
                        if line.startswith('VmRSS:'):
                            return float(line.split()[1]) / 1024
        except Exception:
            pass
        return 0.0


class MetricsContext:
    def __init__(self, collector: MetricsCollector, stage_name: str):
        self.collector = collector
        self.stage_name = stage_name

    def __enter__(self):
        self.collector.start_stage_timer(self.stage_name)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.collector.end_stage_timer(self.stage_name)
        if exc_type:
            self.collector.record_processing_error(f"Stage '{self.stage_name}' failed: {exc_val}")