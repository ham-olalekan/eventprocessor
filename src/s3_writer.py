"""S3 writer for storing grouped events."""

import boto3
import json
import csv
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError
import io
import time

logger = logging.getLogger(__name__)


class S3Writer:
    """Write grouped events to S3 buckets."""

    def __init__(self, bucket_prefix: str, region: str, output_format: str = "json"):
        self.bucket_prefix = bucket_prefix
        self.region = region
        self.output_format = output_format.lower()
        self.s3_client = boto3.client("s3", region_name=region)
        self.upload_stats = {
            "successful_uploads": 0,
            "failed_uploads": 0,
            "total_size_bytes": 0,
        }

    def write_all_groups(
        self, grouped_events: Dict[str, List[Dict[str, Any]]], max_concurrent: int = 10
    ) -> Dict[str, bool]:
        upload_results = {}
        timestamp = datetime.now(timezone.utc)

        logger.info(f"Starting upload for {len(grouped_events)} client groups")

        with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
            futures = {}

            for client_id, events in grouped_events.items():
                future = executor.submit(
                    self._upload_client_events, client_id, events, timestamp
                )
                futures[future] = client_id

            for future in as_completed(futures):
                client_id = futures[future]
                try:
                    success = future.result()
                    upload_results[client_id] = success
                    if success:
                        self.upload_stats["successful_uploads"] += 1
                    else:
                        self.upload_stats["failed_uploads"] += 1
                except Exception as e:
                    logger.error(
                        f"Failed to upload events for client {client_id}: {str(e)}"
                    )
                    upload_results[client_id] = False
                    self.upload_stats["failed_uploads"] += 1

        self._log_upload_statistics()
        return upload_results

    def _upload_client_events(
        self, client_id: str, events: List[Dict[str, Any]], timestamp: datetime
    ) -> bool:
        bucket_name = f"{self.bucket_prefix}-{client_id}"
        file_key = self._generate_file_key(timestamp)

        try:
            if not self._verify_bucket_exists(bucket_name):
                logger.error(f"Bucket {bucket_name} does not exist")
                return False

            if self.output_format == "csv":
                file_content = self._convert_to_csv(events)
                content_type = "text/csv"
            else:
                file_content = self._convert_to_json(events)
                content_type = "application/json"

            success = self._upload_with_retry(
                bucket_name, file_key, file_content, content_type
            )

            if success:
                file_size = len(file_content.encode("utf-8"))
                self.upload_stats["total_size_bytes"] += file_size
                logger.info(
                    f"Successfully uploaded {len(events)} events to s3://{bucket_name}/{file_key} ({file_size} bytes)"
                )
            else:
                logger.error(f"Failed to upload to s3://{bucket_name}/{file_key}")

            return success

        except Exception as e:
            logger.error(f"Error uploading events for client {client_id}: {str(e)}")
            return False

    def _verify_bucket_exists(self, bucket_name: str) -> bool:
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "404":
                logger.warning(f"Bucket {bucket_name} does not exist")
            else:
                logger.error(f"Error checking bucket {bucket_name}: {str(e)}")
            return False

    def _generate_file_key(self, timestamp: datetime) -> str:
        date_str = timestamp.strftime("%Y-%m-%d-%H")
        extension = self.output_format
        return f"events-{date_str}.{extension}"

    def _convert_to_json(self, events: List[Dict[str, Any]]) -> str:
        return json.dumps(events, indent=2, default=str)

    def _convert_to_csv(self, events: List[Dict[str, Any]]) -> str:
        if not events:
            return ""

        output = io.StringIO()

        fieldnames = set()
        for event in events:
            fieldnames.update(event.keys())
        fieldnames = sorted(list(fieldnames))

        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        for event in events:
            if "params" in event and isinstance(event["params"], list):
                event = event.copy()
                event["params"] = json.dumps(event["params"])
            writer.writerow(event)

        return output.getvalue()

    def _upload_with_retry(
        self,
        bucket_name: str,
        key: str,
        content: str,
        content_type: str,
        max_retries: int = 3,
    ) -> bool:
        for attempt in range(max_retries + 1):
            try:
                self.s3_client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=content.encode("utf-8"),
                    ContentType=content_type,
                    ServerSideEncryption="AES256",
                    Metadata={
                        "processing-timestamp": datetime.now(timezone.utc).isoformat(),
                        "event-count": str(
                            len(json.loads(content))
                            if self.output_format == "json"
                            else content.count("\n") - 1
                        ),
                    },
                )
                return True

            except ClientError as e:
                error_code = e.response["Error"]["Code"]

                if error_code == "SlowDown" or error_code == "RequestTimeout":
                    if attempt < max_retries:
                        wait_time = 2**attempt
                        logger.warning(
                            f"S3 rate limited, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})"
                        )
                        time.sleep(wait_time)
                        continue

                logger.error(f"Failed to upload to s3://{bucket_name}/{key}: {str(e)}")
                return False

            except Exception as e:
                logger.error(
                    f"Unexpected error uploading to s3://{bucket_name}/{key}: {str(e)}"
                )
                return False

        return False

    def _log_upload_statistics(self):
        total_uploads = (
            self.upload_stats["successful_uploads"]
            + self.upload_stats["failed_uploads"]
        )
        success_rate = (
            (self.upload_stats["successful_uploads"] / total_uploads * 100)
            if total_uploads > 0
            else 0
        )

        logger.info("Upload statistics:")
        logger.info(f"  Successful uploads: {self.upload_stats['successful_uploads']}")
        logger.info(f"  Failed uploads: {self.upload_stats['failed_uploads']}")
        logger.info(f"  Success rate: {success_rate:.1f}%")
        logger.info(
            f"  Total data uploaded: {self.upload_stats['total_size_bytes'] / 1024 / 1024:.2f} MB"
        )

    def get_upload_statistics(self) -> Dict[str, Any]:
        return self.upload_stats.copy()
