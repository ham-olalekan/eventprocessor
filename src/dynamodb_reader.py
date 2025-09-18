"""DynamoDB reader with parallel scanning capability."""

import boto3
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError
import time

logger = logging.getLogger(__name__)


class DynamoDBReader:
    """Read events from DynamoDB with parallel scanning."""

    def __init__(self, table_name: str, region: str, parallel_segments: int = 8):
        self.table_name = table_name
        self.region = region
        self.parallel_segments = parallel_segments
        self.dynamodb = boto3.resource("dynamodb", region_name=region)
        self.table = self.dynamodb.Table(table_name)
        self.client = boto3.client("dynamodb", region_name=region)

    def get_events_past_hour(self) -> List[Dict[str, Any]]:
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=1)

        logger.info(
            f"Fetching events from {start_time.isoformat()} to {end_time.isoformat()}"
        )

        all_events = self._parallel_scan(start_time, end_time)

        logger.info(f"Retrieved {len(all_events)} events from DynamoDB")
        return all_events

    def _parallel_scan(
        self, start_time: datetime, end_time: datetime
    ) -> List[Dict[str, Any]]:
        all_events = []

        with ThreadPoolExecutor(max_workers=self.parallel_segments) as executor:
            futures = []

            for segment in range(self.parallel_segments):
                future = executor.submit(
                    self._scan_segment, segment, start_time, end_time
                )
                futures.append(future)

            for future in as_completed(futures):
                try:
                    segment_events = future.result()
                    all_events.extend(segment_events)
                    logger.debug(f"Segment completed with {len(segment_events)} events")
                except Exception as e:
                    logger.error(f"Error scanning segment: {str(e)}")
                    raise

        return all_events

    def _scan_segment(
        self, segment: int, start_time: datetime, end_time: datetime
    ) -> List[Dict[str, Any]]:
        events = []
        last_evaluated_key = None
        retry_count = 0
        max_retries = 3

        start_time_str = start_time.isoformat()
        end_time_str = end_time.isoformat()

        while True:
            try:
                scan_params = {
                    "FilterExpression": "#time BETWEEN :start_time AND :end_time",
                    "ExpressionAttributeNames": {"#time": "time"},
                    "ExpressionAttributeValues": {
                        ":start_time": start_time_str,
                        ":end_time": end_time_str,
                    },
                    "Segment": segment,
                    "TotalSegments": self.parallel_segments,
                    "Limit": 1000,
                }

                if last_evaluated_key:
                    scan_params["ExclusiveStartKey"] = last_evaluated_key

                response = self._scan_with_backoff(
                    scan_params, retry_count, max_retries
                )

                events.extend(response.get("Items", []))

                last_evaluated_key = response.get("LastEvaluatedKey")
                if not last_evaluated_key:
                    break

                retry_count = 0

            except ClientError as e:
                if (
                    e.response["Error"]["Code"]
                    == "ProvisionedThroughputExceededException"
                ):
                    retry_count += 1
                    if retry_count > max_retries:
                        logger.error(f"Max retries exceeded for segment {segment}")
                        raise
                    wait_time = (2**retry_count) + (segment * 0.1)
                    logger.warning(
                        f"Rate limited on segment {segment}, waiting {wait_time}s"
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(f"Error scanning segment {segment}: {str(e)}")
                    raise

        logger.info(f"Segment {segment} completed: {len(events)} events")
        return events

    def _scan_with_backoff(
        self, scan_params: Dict, retry_count: int, max_retries: int
    ) -> Dict:
        while retry_count <= max_retries:
            try:
                return self.table.scan(**scan_params)
            except ClientError as e:
                if (
                    e.response["Error"]["Code"]
                    == "ProvisionedThroughputExceededException"
                ):
                    if retry_count >= max_retries:
                        raise
                    wait_time = 2**retry_count
                    logger.warning(f"Rate limited, retrying in {wait_time}s")
                    time.sleep(wait_time)
                    retry_count += 1
                else:
                    raise

    def estimate_scan_time(self) -> float:
        try:
            response = self.client.describe_table(TableName=self.table_name)
            item_count = response["Table"]["ItemCount"]

            items_per_second = 1000 * self.parallel_segments
            estimated_time = item_count / items_per_second

            logger.info(
                f"Table has ~{item_count} items, estimated scan time: {estimated_time:.2f}s"
            )
            return estimated_time

        except ClientError as e:
            logger.warning(f"Could not estimate scan time: {str(e)}")
            return 60.0
