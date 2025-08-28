#!/usr/bin/env python3
"""Centralized configuration for schema management operations."""

# Object types we extract and manage, in dependency order
# Format: (object_type, show_command)
SCHEMA_OBJECT_TYPES = [
    ("FILE FORMAT",       "SHOW FILE FORMATS IN SCHEMA {db}.{schema}"),
    ("SEQUENCE",          "SHOW SEQUENCES IN SCHEMA {db}.{schema}"),
    ("STAGE",             "SHOW STAGES IN SCHEMA {db}.{schema}"),
    ("TABLE",             "SHOW TABLES IN SCHEMA {db}.{schema}"),
    ("VIEW",              "SHOW VIEWS IN SCHEMA {db}.{schema}"),
    ("MATERIALIZED VIEW", "SHOW MATERIALIZED VIEWS IN SCHEMA {db}.{schema}"),
    ("DYNAMIC TABLE",     "SHOW DYNAMIC TABLES IN SCHEMA {db}.{schema}"),
    ("STREAM",            "SHOW STREAMS IN SCHEMA {db}.{schema}"),
    ("PIPE",              "SHOW PIPES IN SCHEMA {db}.{schema}"),
    ("TASK",              "SHOW TASKS IN SCHEMA {db}.{schema}"),
]

# Object types that require CREATE privileges for grant filtering
OBJECT_TYPES_FOR_GRANT_FILTERING = [
    "TABLES", "VIEWS", "PROCEDURES", "FUNCTIONS", "STAGES", 
    "PIPES", "STREAMS", "TASKS", "MATERIALIZED VIEWS", "DYNAMIC TABLES"
]

# Essential privileges that should always be preserved
ESSENTIAL_PRIVILEGES = {
    "USAGE", "OWNERSHIP", "MONITOR", "CREATE TABLE", "CREATE VIEW", "MODIFY"
}

def get_object_types():
    """Get the list of object types to extract."""
    return SCHEMA_OBJECT_TYPES

def get_grant_filter_objects():
    """Get object types for grant filtering."""
    return OBJECT_TYPES_FOR_GRANT_FILTERING

def get_essential_privileges():
    """Get essential privileges that should always be preserved."""
    return ESSENTIAL_PRIVILEGES

def is_user_defined_object(obj_name):
    """Check if an object is user-defined (not system)."""
    if not obj_name:
        return False
    
    # Filter out system objects
    system_prefixes = ['SYSTEM$', 'INFORMATION_SCHEMA']
    return not any(obj_name.upper().startswith(prefix) for prefix in system_prefixes)
