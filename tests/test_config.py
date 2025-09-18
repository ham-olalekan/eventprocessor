"""Tests for configuration management."""

import pytest
import os
import tempfile
from pathlib import Path

from config import Config


class TestConfig:
    """Test configuration loading and management."""

    def test_load_valid_config(self, mock_config):
        """Test loading a valid configuration file."""
        config = Config(mock_config)

        assert config.dynamodb_table_name == "test-events"
        assert config.dynamodb_region == "us-east-1"
        assert config.s3_bucket_prefix == "test-client-events"
        assert config.parallel_segments == 4
        assert config.output_format == "json"

    def test_config_not_found(self):
        """Test handling of missing configuration file."""
        with pytest.raises(FileNotFoundError):
            Config("/non/existent/path.yaml")

    def test_environment_variable_substitution(self, tmp_path):
        """Test that environment variables are substituted in config."""
        # Set environment variables
        os.environ["TEST_TABLE_NAME"] = "env-table"
        os.environ["TEST_BUCKET_PREFIX"] = "env-bucket"

        # Create config with env vars
        config_file = tmp_path / "config.yaml"
        config_content = """
dynamodb:
  table_name: ${TEST_TABLE_NAME}
  region: us-east-1

s3:
  bucket_prefix: ${TEST_BUCKET_PREFIX}
  region: us-east-1
"""
        config_file.write_text(config_content)

        config = Config(str(config_file))

        assert config.dynamodb_table_name == "env-table"
        assert config.s3_bucket_prefix == "env-bucket"

        # Clean up
        del os.environ["TEST_TABLE_NAME"]
        del os.environ["TEST_BUCKET_PREFIX"]

    def test_get_with_dot_notation(self, mock_config):
        """Test getting configuration values with dot notation."""
        config = Config(mock_config)

        assert config.get("dynamodb.table_name") == "test-events"
        assert config.get("s3.output_format") == "json"
        assert config.get("processing.max_retries") == 3

    def test_get_with_default(self, mock_config):
        """Test getting configuration values with defaults."""
        config = Config(mock_config)

        assert config.get("non.existent.key", "default") == "default"
        assert config.get("dynamodb.non_existent", 42) == 42

    def test_property_accessors(self, mock_config):
        """Test configuration property accessors."""
        config = Config(mock_config)

        assert config.dynamodb_table_name == "test-events"
        assert config.dynamodb_region == "us-east-1"
        assert config.s3_bucket_prefix == "test-client-events"
        assert config.s3_region == "us-east-1"
        assert config.output_format == "json"
        assert config.time_window_hours == 1
        assert config.parallel_segments == 4
        assert config.log_level == "DEBUG"

    def test_default_values(self, tmp_path):
        """Test that default values are returned for missing config keys."""
        # Create minimal config
        config_file = tmp_path / "minimal.yaml"
        config_content = """
dynamodb:
  region: us-west-2
"""
        config_file.write_text(config_content)

        config = Config(str(config_file))

        # Check defaults are used
        assert config.dynamodb_table_name == "events"  # default
        assert config.dynamodb_region == "us-west-2"  # from config
        assert config.s3_bucket_prefix == "client-events"  # default
        assert config.output_format == "json"  # default
        assert config.log_level == "INFO"  # default

    def test_yaml_parsing_error(self, tmp_path):
        """Test handling of invalid YAML syntax."""
        config_file = tmp_path / "invalid.yaml"
        config_content = """
invalid yaml content
  - this is not valid
  : yaml syntax
"""
        config_file.write_text(config_content)

        with pytest.raises(Exception):  # YAML parsing error
            Config(str(config_file))
