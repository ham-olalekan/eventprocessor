"""Main entry point for the DynamoDB to S3 event processor."""

import logging
import sys
import time
from datetime import datetime, timezone
import json
from typing import Dict, Any, Optional

from config import Config
from dynamodb_reader import DynamoDBReader
from data_processor import DataProcessor
from s3_writer import S3Writer
from metrics_collector import MetricsCollector, MetricsContext


def setup_logging(config: Config):
    log_level = getattr(logging, config.log_level.upper(), logging.INFO)
    log_format = config.get(
        "logging.format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    logging.basicConfig(
        level=log_level, format=log_format, handlers=[logging.StreamHandler(sys.stdout)]
    )

    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def process_events(config: Config) -> Dict[str, Any]:
    logger = logging.getLogger(__name__)
    start_time = time.time()

    metrics = MetricsCollector(region=config.dynamodb_region)
    metrics.start_processing_timer()

    results = {
        "success": False,
        "start_time": datetime.now(timezone.utc).isoformat(),
        "events_processed": 0,
        "clients_processed": 0,
        "errors": [],
    }

    try:
        with MetricsContext(metrics, "component_initialization"):
            logger.info("Initializing components...")

        dynamodb_reader = DynamoDBReader(
            table_name=config.dynamodb_table_name,
            region=config.dynamodb_region,
            parallel_segments=config.parallel_segments,
        )

        data_processor = DataProcessor()

        s3_writer = S3Writer(
            bucket_prefix=config.s3_bucket_prefix,
            region=config.s3_region,
            output_format=config.output_format,
        )

        estimated_time = dynamodb_reader.estimate_scan_time()
        logger.info(f"Estimated processing time: {estimated_time:.2f} seconds")

        logger.info("Reading events from DynamoDB...")
        try:
            events = dynamodb_reader.get_events_past_hour()
            results["events_processed"] = len(events)

            if not events:
                logger.warning("No events found in the past hour")
                results["success"] = True
                results["message"] = "No events to process"
                return results

        except Exception as e:
            error_msg = f"Failed to read from DynamoDB: {str(e)}"
            logger.error(error_msg)
            results["errors"].append(error_msg)
            raise

        logger.info("Processing and grouping events...")
        try:
            grouped_events = data_processor.process_events(events)
            results["clients_processed"] = len(grouped_events)
            results["processing_stats"] = data_processor.get_statistics()

        except Exception as e:
            error_msg = f"Failed to process events: {str(e)}"
            logger.error(error_msg)
            results["errors"].append(error_msg)
            raise

        logger.info("Writing events to S3...")
        try:
            max_concurrent_uploads = config.get(
                "performance.max_concurrent_uploads", 10
            )
            upload_results = s3_writer.write_all_groups(
                grouped_events, max_concurrent=max_concurrent_uploads
            )

            results["upload_results"] = upload_results
            results["upload_stats"] = s3_writer.get_upload_statistics()

            failed_uploads = [
                client_id
                for client_id, success in upload_results.items()
                if not success
            ]

            if failed_uploads:
                error_msg = f"Failed to upload events for clients: {failed_uploads}"
                logger.error(error_msg)
                results["errors"].append(error_msg)
                results["success"] = False
            else:
                results["success"] = True

        except Exception as e:
            error_msg = f"Failed to write to S3: {str(e)}"
            logger.error(error_msg)
            results["errors"].append(error_msg)
            raise

    except Exception as e:
        logger.error(f"Processing failed with error: {str(e)}")
        results["error"] = str(e)
        results["success"] = False

    finally:
        processing_time = time.time() - start_time
        results["end_time"] = datetime.now(timezone.utc).isoformat()
        results["processing_time_seconds"] = round(processing_time, 2)

        metrics.end_processing_timer(results.get("events_processed", 0))
        metrics.log_detailed_metrics()
        metrics.publish_cloudwatch_metrics()

        logger.info("=" * 50)
        logger.info("Processing Summary:")
        logger.info(f"  Success: {results['success']}")
        logger.info(f"  Events processed: {results['events_processed']}")
        logger.info(f"  Clients processed: {results['clients_processed']}")
        logger.info(f"  Processing time: {processing_time:.2f} seconds")

        if results.get("upload_stats"):
            logger.info(
                f"  Successful uploads: {results['upload_stats']['successful_uploads']}"
            )
            logger.info(
                f"  Failed uploads: {results['upload_stats']['failed_uploads']}"
            )

        if results["errors"]:
            logger.error(f"  Errors: {results['errors']}")

        logger.info("=" * 50)

    return results


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    config = Config()

    setup_logging(config)

    logger = logging.getLogger(__name__)
    logger.info(f"Lambda function invoked with event: {json.dumps(event)}")

    results = process_events(config)
    status_code = 200 if results["success"] else 500
    return {"statusCode": status_code, "body": json.dumps(results, default=str)}


def main():
    """Main entry point for command-line execution."""
    try:
        config = Config()
    except FileNotFoundError as e:
        logging.error(f"Configuration error: {e}")
        sys.exit(1)

    setup_logging(config)

    logger = logging.getLogger(__name__)
    logger.info("Starting DynamoDB to S3 event processor")

    results = process_events(config)
    if results["success"]:
        logger.info("Processing completed successfully")
        sys.exit(0)
    else:
        logger.error("Processing failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
