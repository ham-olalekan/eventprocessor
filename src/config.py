"""Configuration management for the event processor."""

import os
import yaml
from typing import Dict, Any
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


class Config:
    """Configuration loader with environment variable support."""

    def __init__(self, config_path: str = None):
        if config_path is None:
            config_path = Path(__file__).parent.parent / "config" / "config.yaml"

        self.config_path = Path(config_path)
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        if self.config_path.exists():
            with open(self.config_path, "r") as f:
                config_text = f.read()
            config_text = os.path.expandvars(config_text)
            return yaml.safe_load(config_text)
        else:
            return {
                'dynamodb': {
                    'table_name': os.getenv('DYNAMODB_TABLE_NAME', 'events'),
                    'region': os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
                },
                's3': {
                    'bucket_prefix': os.getenv('S3_BUCKET_PREFIX', 'client-events'),
                    'region': os.getenv('AWS_DEFAULT_REGION', 'us-east-1'),
                    'output_format': os.getenv('OUTPUT_FORMAT', 'json')
                },
                'processing': {
                    'parallel_segments': 8,
                    'max_concurrent_uploads': 10,
                    'max_retries': 3,
                    'retry_delay_base': 1
                },
                'logging': {
                    'level': os.getenv('LOG_LEVEL', 'INFO'),
                    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                }
            }

    def get(self, key: str, default: Any = None) -> Any:
        keys = key.split(".")
        value = self.config

        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default

        return value

    @property
    def dynamodb_table_name(self) -> str:
        return self.get("dynamodb.table_name", "events")

    @property
    def dynamodb_region(self) -> str:
        return self.get("dynamodb.region", "us-east-1")

    @property
    def s3_bucket_prefix(self) -> str:
        return self.get("s3.bucket_prefix", "client-events")

    @property
    def s3_region(self) -> str:
        return self.get("s3.region", "us-east-1")

    @property
    def output_format(self) -> str:
        return self.get("s3.output_format", "json")

    @property
    def time_window_hours(self) -> int:
        return self.get("processing.time_window_hours", 1)

    @property
    def parallel_segments(self) -> int:
        return self.get("dynamodb.parallel_segments", 8)

    @property
    def log_level(self) -> str:
        return self.get("logging.level", "INFO")
