#!/usr/bin/env python3
"""
Compare current target schema state with proposed schemachange files.
Provides a dry-run report showing what would change.
"""

import argparse
import sys
import os
import json
import re
from datetime import datetime
import snowflake.connector as sf
import tomllib
from utils.connection import load_connection_config
from utils.extraction import replace_environment_references
from utils.schema_config import get_object_types, is_user_defined_object, get_essential_privileges


def rows_dict(cursor):
    """Convert cursor rows to list of dictionaries."""
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def connect_to_target(target_env, local_config, db_prefix=None):
    """Connect to target environment."""
    if db_prefix and db_prefix.lower() != "none":
        target_database = f"{db_prefix}_PLATFORM_{target_env.upper()}"
    else:
        # Extract base from current connection and add target env
        current_db = local_config.get('database', 'PLATFORM_SIT')
        base_db = re.sub(r'_(SIT|DEV|QA|UAT|PROD)$', '', current_db)
        target_database = f"{base_db}_{target_env.upper()}"
    
    # Create target connection config
    target_config = local_config.copy()
    target_config['database'] = target_database
    
    print(f"🔗 Connecting to target: {target_database}")
    return sf.connect(**target_config), target_database


def extract_current_state(connection, database, schema, target_env=None):
    """Extract current DDL and grants from target schema."""
    current_objects = {}
    current_grants = []
    
    cursor = connection.cursor()
    
    try:
        # Check if schema exists
        cursor.execute(f"SHOW SCHEMAS IN DATABASE {database}")
        schemas = [r[1] for r in cursor.fetchall()]
        
        if schema not in schemas:
            print(f"⚠️  Schema {database}.{schema} does not exist in target")
            return current_objects, current_grants
        
        print(f"📋 Extracting current state of {database}.{schema}")
        
        # Extract object types from centralized config
        object_types = get_object_types()
        
        for obj_type, show_sql in object_types:
            try:
                cursor.execute(show_sql.format(db=database, schema=schema))
                for row in rows_dict(cursor):
                    name = row.get("name")
                    # Only include user-defined objects (exclude system objects)
                    if name and is_user_defined_object(name):
                        try:
                            cursor.execute(f"SELECT GET_DDL('{obj_type}', '{database}.{schema}.{name}')")
                            ddl = cursor.fetchone()[0]
                            current_objects[f"{obj_type}:{name}"] = ddl.strip()
                        except Exception as e:
                            current_objects[f"{obj_type}:{name}"] = f"-- ERROR: {e}"
            except Exception:
                pass  # Object type might not exist
        
        # Extract current grants (only for object types we manage)
        try:
            # Get managed object types (same logic as extraction)
            managed_object_types = set()
            for obj_type, _ in get_object_types():
                managed_object_types.add(obj_type)
                if obj_type.endswith('Y'):
                    managed_object_types.add(obj_type[:-1] + 'IES')
                else:
                    managed_object_types.add(obj_type + 'S')
            essential_privileges = get_essential_privileges()
            
            cursor.execute(f"SHOW GRANTS ON SCHEMA {database}.{schema}")
            for row in rows_dict(cursor):
                privilege = row.get("privilege")
                granted_on = row.get("granted_on") 
                granted_to = row.get("granted_to")
                grantee = row.get("grantee_name")
                grant_option = row.get("grant_option")
                name = row.get("name", "")
                
                # Only include schema-level grants (granted_on = 'SCHEMA')
                if all([privilege, granted_on, granted_to, grantee]) and granted_on == "SCHEMA":
                    
                    # ADDITIVE FILTERING: Only include grants for object types we manage
                    if privilege.startswith("CREATE "):
                        obj_type = privilege.replace("CREATE ", "")
                        if obj_type not in managed_object_types and privilege not in essential_privileges:
                            continue  # Skip grants for object types we don't manage
                    
                    # Skip wrong environment roles - don't manage them
                    wrong_env_patterns = ['PROD_', 'DEV_', 'QA_', 'UAT_']
                    current_env_pattern = f'{target_env.upper()}_'
                    if any(grantee.startswith(env) for env in wrong_env_patterns if env != current_env_pattern):
                        continue  # Skip grants for wrong environment roles
                    
                    grant_str = f"GRANT {privilege} ON {granted_on}"
                    if name:
                        grant_str += f" {name}"
                    grant_str += f" TO {granted_to} {grantee}"
                    if str(grant_option).upper() == "TRUE":
                        grant_str += " WITH GRANT OPTION"
                    current_grants.append(grant_str.upper())
        except Exception as e:
            print(f"⚠️  Could not extract grants: {e}")
            
    finally:
        cursor.close()
    
    return current_objects, current_grants


def load_proposed_changes(schema_dir, target_env, target_database):
    """Load proposed DDL and grants from schemachange files."""
    proposed_objects = {}
    proposed_grants = []
    
    # Load DDL file
    ddl_files = [f for f in os.listdir(schema_dir) if f.startswith("V1000__") and f.endswith(".sql")]
    if ddl_files:
        ddl_file = os.path.join(schema_dir, ddl_files[0])
        print(f"📄 Reading proposed DDL: {ddl_file}")
        
        with open(ddl_file, 'r') as f:
            content = f.read()
            
        # Replace variables with actual values
        content = content.replace('{{ ENV }}', target_env.upper())
        content = content.replace('{{ DB_PREFIX }}', 'TEST')  # Use default for comparison
        
        # Parse DDL into objects (improved parsing)
        # Split by semicolons first, then identify CREATE statements
        statements = content.split(';')
        
        for stmt in statements:
            stmt = stmt.strip()
            if stmt:
                # Remove leading comments from the statement
                stmt_lines = stmt.split('\n')
                clean_lines = []
                for line in stmt_lines:
                    line = line.strip()
                    if line and not line.startswith('--'):
                        clean_lines.append(line)
                clean_stmt = '\n'.join(clean_lines)
                
                if clean_stmt:
                    # Look for CREATE statements (table or view)
                    match = re.match(r'create\s+(?:or\s+replace\s+)?(secure\s+)?(table|view)\s+([A-Z_][A-Z0-9_]*)[\s\(]', clean_stmt, re.IGNORECASE | re.DOTALL)
                    if match:
                        obj_type = match.group(2).upper()  # TABLE or VIEW
                        obj_name = match.group(3).upper()
                        # Only include objects we care about
                        if obj_type in ['TABLE', 'VIEW']:
                            # Use the clean statement without comments
                            proposed_objects[f"{obj_type}:{obj_name}"] = clean_stmt
    
    # Load grants file
    grant_files = [f for f in os.listdir(schema_dir) if f.startswith("V1001__") and f.endswith(".sql")]
    if grant_files:
        grant_file = os.path.join(schema_dir, grant_files[0])
        print(f"🔐 Reading proposed grants: {grant_file}")
        
        with open(grant_file, 'r') as f:
            content = f.read()
            
        # Replace variables with actual values
        content = content.replace('{{ ENV }}', target_env.upper())
        content = content.replace('{{ DB_PREFIX }}', 'TEST')  # Use default for comparison
        content = content.replace(f'PLATFORM_{{ ENV }}', target_database)
        
        # Extract grant statements (normalize format)
        lines = content.split('\n')
        for line in lines:
            line = line.strip()
            if line and line.lower().startswith('grant') and not line.startswith('--'):
                # Normalize grant statement
                clean_grant = line.rstrip(';').upper()
                proposed_grants.append(clean_grant)
    
    return proposed_objects, proposed_grants


def normalize_ddl(ddl_text):
    """Normalize DDL for comparison by removing whitespace and formatting differences."""
    if not ddl_text:
        return ''
    
    # Convert to uppercase for case-insensitive comparison
    normalized = ddl_text.upper().strip()
    
    # Remove comments
    normalized = re.sub(r'--.*$', '', normalized, flags=re.MULTILINE)
    
    # Normalize all whitespace (including newlines) to single spaces
    normalized = re.sub(r'\s+', ' ', normalized)
    
    # Remove spaces around specific SQL punctuation
    normalized = re.sub(r'\s*([(),;])\s*', r'\1', normalized)
    
    # Normalize common SQL formatting
    normalized = re.sub(r'\s*,\s*', ',', normalized)
    normalized = re.sub(r'\s*(=)\s*', r'\1', normalized)
    
    # Remove trailing semicolons and whitespace
    normalized = re.sub(r';\s*$', '', normalized)
    
    # Normalize template variables (in case any leaked through)
    normalized = re.sub(r'\{\{\s*VAR\s*\(\s*["\']ENV["\']\s*\)\s*\}\}', 'SIT', normalized)
    
    return normalized.strip()


def compare_objects(current_objects, proposed_objects):
    """Compare current vs proposed objects using normalized DDL."""
    print("\n" + "="*80)
    print("📊 OBJECT COMPARISON REPORT")
    print("="*80)
    
    all_objects = set(current_objects.keys()) | set(proposed_objects.keys())
    
    changes = {
        'new': [],
        'modified': [],
        'removed': [],
        'unchanged': []
    }
    
    for obj_key in sorted(all_objects):
        current_ddl = current_objects.get(obj_key, '').strip()
        proposed_ddl = proposed_objects.get(obj_key, '').strip()
        
        # Normalize both DDL strings for comparison
        current_normalized = normalize_ddl(current_ddl)
        proposed_normalized = normalize_ddl(proposed_ddl)
        
        if not current_normalized and proposed_normalized:
            changes['new'].append(obj_key)
        elif current_normalized and not proposed_normalized:
            changes['removed'].append(obj_key)
        elif current_normalized != proposed_normalized:
            changes['modified'].append(obj_key)
            # Show a hint about what type of change this is
            if len(current_ddl) != len(proposed_ddl):
                size_diff = len(proposed_ddl) - len(current_ddl)
                print(f"   📝 {obj_key}: structural change ({size_diff:+d} chars)")
        else:
            changes['unchanged'].append(obj_key)
    
    # Print summary
    print(f"🆕 NEW OBJECTS: {len(changes['new'])}")
    for obj in changes['new']:
        print(f"   + {obj}")
    
    print(f"\n🔄 MODIFIED OBJECTS: {len(changes['modified'])}")
    for obj in changes['modified']:
        print(f"   ~ {obj}")
        
    print(f"\n🗑️  REMOVED OBJECTS: {len(changes['removed'])}")
    for obj in changes['removed']:
        print(f"   - {obj}")
        
    print(f"\n✅ UNCHANGED OBJECTS: {len(changes['unchanged'])}")
    
    return changes


def normalize_grant(grant_statement):
    """Normalize grant statement for semantic comparison."""
    import re
    
    # Convert to uppercase and strip whitespace
    normalized = grant_statement.upper().strip()
    
    # Remove IDENTIFIER() wrapper - IDENTIFIER('SCHEMA') becomes SCHEMA  
    normalized = re.sub(r"IDENTIFIER\('([^']+)'\)", r'\1', normalized)
    normalized = re.sub(r'IDENTIFIER\("([^"]+)"\)', r'\1', normalized)
    
    # Normalize whitespace - multiple spaces become single space
    normalized = re.sub(r'\s+', ' ', normalized)
    
    # Standardize schema references - remove extra quotes if any
    normalized = re.sub(r"'([^']+)'", r'\1', normalized)
    normalized = re.sub(r'"([^"]+)"', r'\1', normalized)
    
    return normalized.strip()


def compare_grants(current_grants, proposed_grants):
    """Compare current vs proposed grants using semantic normalization."""
    print("\n" + "="*80)
    print("🔐 GRANTS COMPARISON REPORT") 
    print("="*80)
    
    # Normalize grants for semantic comparison
    current_normalized = {normalize_grant(g): g for g in current_grants}
    proposed_normalized = {normalize_grant(g): g for g in proposed_grants}
    
    current_set = set(current_normalized.keys())
    proposed_set = set(proposed_normalized.keys())
    
    new_grants = proposed_set - current_set
    removed_grants = current_set - proposed_set
    unchanged_grants = current_set & proposed_set
    
    print(f"🆕 NEW GRANTS: {len(new_grants)}")
    for normalized_grant in sorted(new_grants):
        original_grant = proposed_normalized[normalized_grant]
        print(f"   + {original_grant}")
    
    print(f"\n🗑️  REMOVED GRANTS: {len(removed_grants)}")
    for normalized_grant in sorted(removed_grants):
        original_grant = current_normalized[normalized_grant]
        print(f"   - {original_grant}")
        
    print(f"\n✅ UNCHANGED GRANTS: {len(unchanged_grants)}")
    
    return {
        'new': new_grants,
        'removed': removed_grants, 
        'unchanged': unchanged_grants
    }


def main():
    """Compare target schema with proposed changes."""
    parser = argparse.ArgumentParser(description="Compare target schema with proposed schemachange files")
    parser.add_argument("--connections", default="connections.toml", help="Path to connections.toml file")
    parser.add_argument("--src", default="SRC", help="Source connection name")
    parser.add_argument("--target-env", required=True, help="Target environment (SIT, DEV, QA, UAT, PROD)")
    parser.add_argument("--schema", required=True, help="Schema name to compare")
    parser.add_argument("--schema-dir", help="Schema directory (default: schemas/SCHEMA)")
    parser.add_argument("--db-prefix", help="Database prefix for target environment")
    
    args = parser.parse_args()
    
    schema_dir = args.schema_dir or f"schemas/{args.schema.upper()}"
    
    if not os.path.exists(schema_dir):
        print(f"❌ Schema directory not found: {schema_dir}")
        return 1
    
    try:
        print(f"🔍 Comparing {args.schema} schema for {args.target_env} environment")
        print(f"📁 Using schema files from: {schema_dir}")
        
        # Load local connection config
        local_config = load_connection_config(is_ci=False, connections_file=args.connections, connection_name=args.src)
        
        # Connect to target
        target_conn, target_database = connect_to_target(args.target_env, local_config, args.db_prefix)
        
        # Extract current state
        current_objects, current_grants = extract_current_state(target_conn, target_database, args.schema.upper(), args.target_env)
        
        # Load proposed changes
        proposed_objects, proposed_grants = load_proposed_changes(schema_dir, args.target_env, target_database)
        
        # Compare objects
        object_changes = compare_objects(current_objects, proposed_objects)
        
        # Compare grants
        grant_changes = compare_grants(current_grants, proposed_grants)
        
        # Enhanced Summary with categorization
        managed_object_changes = len(object_changes['new']) + len(object_changes['modified']) + len(object_changes['removed'])
        meaningful_grant_changes = len(grant_changes['new'])
        
        # Categorize removed grants
        scope_cleanup_grants = []
        environment_cleanup_grants = []
        
        for grant in grant_changes['removed']:
            if any(env in grant for env in ['_PROD_', '_DEV_', '_QA_', '_UAT_'] if env != f'_{args.target_env}_'):
                environment_cleanup_grants.append(grant)
            elif any(policy_type in grant for policy_type in [
                'AGGREGATION POLICY', 'ALERT', 'AUTHENTICATION POLICY', 'CORTEX SEARCH',
                'DATA METRIC FUNCTION', 'DATASET', 'EVENT TABLE', 'EXTERNAL TABLE',
                'GIT REPOSITORY', 'ICEBERG TABLE', 'IMAGE REPOSITORY', 'MASKING POLICY',
                'MODEL', 'NETWORK RULE', 'NOTEBOOK', 'PACKAGES POLICY', 'PASSWORD POLICY',
                'PRIVACY POLICY', 'PROJECTION POLICY', 'RESOURCE GROUP', 'ROW ACCESS POLICY',
                'SECRET', 'SERVICE CLASS', 'SERVICE', 'SESSION POLICY', 'SNAPSHOT',
                'STREAMLIT', 'TAG', 'TEMPORARY TABLE'
            ]):
                scope_cleanup_grants.append(grant)
        
        meaningful_changes = managed_object_changes + meaningful_grant_changes + len(environment_cleanup_grants)
        
        print("\n" + "="*80)
        print("📋 ENHANCED SUMMARY")
        print("="*80)
        
        if meaningful_changes == 0 and len(scope_cleanup_grants) == 0:
            print("✅ No changes detected - target schema matches proposed state")
            print("🎯 MIGRATION STATUS: COMPLETE - Existing database already matches schemachange files")
            print("📋 This indicates the target database is already in the desired state")
            print("🚀 Safe to proceed with schemachange deployment (will be a no-op)")
            return 0
        
        print(f"📝 MEANINGFUL CHANGES: {meaningful_changes}")
        if managed_object_changes > 0:
            print(f"   • {managed_object_changes} object changes (tables, views, etc.)")
        if meaningful_grant_changes > 0:
            print(f"   • {meaningful_grant_changes} new grants")
        if len(environment_cleanup_grants) > 0:
            print(f"   • {len(environment_cleanup_grants)} environment cleanup (wrong env roles)")
            
        if len(scope_cleanup_grants) > 0:
            print(f"\n🧹 SCOPE CLEANUP: {len(scope_cleanup_grants)} grants for unmanaged object types")
            print("   (This is expected - removing grants for object types outside your scope)")
        
        total_changes = meaningful_changes + len(scope_cleanup_grants)
        print(f"\n📊 TOTAL REPORTED: {total_changes} changes")
        print("🚀 Run deployment to apply these changes")
        
        return 2 if meaningful_changes > 0 else 0
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    finally:
        if 'target_conn' in locals():
            target_conn.close()


if __name__ == "__main__":
    sys.exit(main())
