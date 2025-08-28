#!/usr/bin/env python3
"""Connection utilities for Snowflake."""

import os

# Handle both Python 3.11+ (tomllib) and older versions (toml)
try:
    import tomllib
except ImportError:
    try:
        import toml as tomllib
    except ImportError:
        raise ImportError("Either 'tomllib' (Python 3.11+) or 'toml' package is required")


def load_connection_config(is_ci=False, connections_file="connections.toml", connection_name="SRC"):
    """Load connection configuration based on environment."""
    if is_ci:
        # CI environment - use environment variables
        return {
            'account': os.environ.get('SNOWFLAKE_ACCOUNT'),
            'user': os.environ.get('SNOWFLAKE_USER'),
            'password': os.environ.get('SNOWFLAKE_PASSWORD'),
            'role': os.environ.get('SNOWFLAKE_ROLE'),
            'warehouse': os.environ.get('SNOWFLAKE_WAREHOUSE'),
            'database': os.environ.get('SNOWFLAKE_DATABASE'),
            'schema': os.environ.get('SNOWFLAKE_SCHEMA', 'PUBLIC')
        }
    else:
        # Local environment - use connections.toml
        with open(connections_file, "rb") as f:
            data = tomllib.load(f)
        return data[connection_name]


def load_local_config(connections_file="connections.toml", connection_name="SRC", schema=None):
    """Load local connection config from connections.toml.
    
    Args:
        connections_file: Path to connections.toml file
        connection_name: Base connection name (SRC or TGT)
        schema: Schema name to append to connection_name (e.g., DATA_AMS)
    """
    with open(connections_file, "rb") as f:
        data = tomllib.load(f)
    
    # Try schema-specific connection first, fall back to generic connection
    if schema:
        schema_specific_name = f"{connection_name}_{schema}"
        if schema_specific_name in data:
            return data[schema_specific_name]
    
    # Fall back to generic connection
    return data[connection_name]
