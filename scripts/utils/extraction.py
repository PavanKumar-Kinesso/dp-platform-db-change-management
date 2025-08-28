#!/usr/bin/env python3
"""Schema extraction utilities for Snowflake."""

import os
import re
from datetime import datetime
import snowflake.connector as sf
from .schema_config import get_object_types, is_user_defined_object, get_essential_privileges


def rows_dict(cursor):
    """Convert cursor rows to list of dictionaries."""
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def replace_environment_references(content, target_env="DEV", db_prefix=None, db_base=None):
    """Replace environment-specific references in SQL content."""
    # Replace environment placeholders
    content = content.replace('{{ ENV }}', target_env.upper())
    content = content.replace('{{ENV}}', target_env.upper())
    
    # Replace DB_PREFIX placeholder
    if db_prefix:
        content = content.replace('{{ DB_PREFIX }}', db_prefix)
        content = content.replace('{{DB_PREFIX}}', db_prefix)
    
    # Replace DB_BASE placeholder
    if db_base:
        content = content.replace('{{ DB_BASE }}', db_base)
        content = content.replace('{{DB_BASE}}', db_base)
    
    # Replace specific environment patterns (SIT -> target)
    env_patterns = ['SIT', 'QA', 'UAT', 'PROD']
    for env in env_patterns:
        if env != target_env.upper():
            content = re.sub(f'_{env}(?=_|\\s|$)', f'_{target_env.upper()}', content)
    
    return content


def template_environment_references(content, source_env, target_env, db_base=None):
    """Smart templating: Only template references to the CURRENT database, not cross-database references."""
    # IMPORTANT: Only template references to the CURRENT database, not cross-database references
    # Cross-database references like CONFIG.DP_CLIENT_VIEW should remain as-is
    
    # Replace database references (e.g., PLATFORM_SIT -> {{DB_BASE}}_{{ENV}}) 
    # BUT ONLY when they refer to the current database being extracted
    if db_base:
        # Replace references to the current database type + environment
        current_db_pattern = rf'{re.escape(db_base)}_{re.escape(source_env)}'
        content = re.sub(current_db_pattern, '{{DB_BASE}}_{{ENV}}', content)
        
        # Also replace lowercase versions
        current_db_pattern_lower = rf'{re.escape(db_base.lower())}_{re.escape(source_env)}'
        content = re.sub(current_db_pattern_lower, '{{DB_BASE}}_{{ENV}}', content)
    
    # Replace other environment-specific references that are NOT cross-database
    # This is more conservative - only replace when we're sure it's safe
    env_patterns = ['SIT', 'QA', 'UAT', 'PROD']
    for env in env_patterns:
        if env != target_env.upper():
            # Only replace environment references that are part of database names
            # NOT standalone environment references in other contexts
            content = re.sub(rf'_{env}(?=_|\.|$)', f'_{target_env.upper()}', content)
    
    return content


def extract_schemas(database, schemas, connection_config, role_map=None, output_dir="schemas", db_prefix=None, db_base=None):
    """Extract DDL and grants for multiple schemas."""
    results = []
    role_map = role_map or {}
    
    # Connect to Snowflake
    conn = sf.connect(**connection_config)
    cursor = conn.cursor()
    
    try:
        for schema in schemas:
            # Create output directory
            schema_dir = os.path.join(output_dir, schema)
            os.makedirs(schema_dir, exist_ok=True)
            
            # Extract objects
            ddl_statements = extract_schema_objects(cursor, database, schema)
            
            # Extract grants
            grant_statements = extract_schema_grants(cursor, database, schema, role_map)
            
            # Determine source environment from database name
            source_env = None
            if '_' in database:
                source_env = database.split('_')[-1]  # Extract SIT from PLATFORM_SIT
            
            # Smart templating: Only template references to the CURRENT database
            templated_ddl = []
            for ddl in ddl_statements:
                templated_ddl.append(template_environment_references(
                    ddl, source_env or 'SIT', '{{ENV}}', db_base
                ))
            
            # Write DDL file
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            ddl_file = os.path.join(schema_dir, f"V1000__baseline_from_{database}.sql")
            
            with open(ddl_file, 'w') as f:
                f.write(f"-- Exported from {database} at {timestamp}\n")
                f.write("-- Environment references templated for multi-environment deployment\n")
                f.write("-- NOTE: Only current database references are templated, cross-database references remain as-is\n")
                if templated_ddl:
                    f.write('\n'.join(templated_ddl))
                else:
                    f.write("-- No objects found in schema\n")
            
            # Write grants file
            grant_file = os.path.join(schema_dir, f"V1001__grants_from_{database}.sql")
            
            with open(grant_file, 'w') as f:
                f.write(f"-- Grants exported from {database} at {timestamp}\n")
                f.write("-- Environment references templated for multi-environment deployment\n")
                if grant_statements:
                    f.write('\n'.join(grant_statements))
                else:
                    f.write("-- No grants found for schema\n")
            
            results.append({
                'schema': schema,
                'ddl_file': ddl_file,
                'grant_file': grant_file,
                'object_count': len(templated_ddl),
                'grant_count': len(grant_statements)
            })
            
    finally:
        cursor.close()
        conn.close()
    
    return results


def extract_schema_objects(cursor, database, schema):
    """Extract DDL for all objects in a schema."""
    ddl_statements = []
    
    # Get object types from centralized config
    object_types = get_object_types()
    
    for obj_type, show_sql in object_types:
        try:
            cursor.execute(show_sql.format(db=database, schema=schema))
            objects = rows_dict(cursor)
            
            for obj in objects:
                obj_name = obj.get("name")
                if obj_name and is_user_defined_object(obj_name):
                    try:
                        # Get DDL
                        cursor.execute(f"SELECT GET_DDL('{obj_type}', '{database}.{schema}.{obj_name}')")
                        ddl = cursor.fetchone()[0]
                        
                        # Clean up DDL
                        ddl = ddl.strip()
                        if not ddl.endswith(';'):
                            ddl += ';'
                        
                        ddl_statements.append(ddl)
                        
                    except Exception as e:
                        print(f"  ⚠️  Could not extract {obj_type} {obj_name}: {e}")
                        
        except Exception as e:
            # Object type might not exist in this version
            pass
    
    return ddl_statements


def extract_schema_grants(cursor, database, schema, role_map=None):
    """Extract grants for a schema."""
    grant_statements = []
    role_map = role_map or {}
    
    try:
        # Get schema grants
        cursor.execute(f"SHOW GRANTS ON SCHEMA {database}.{schema}")
        grants = rows_dict(cursor)
        
        # Get essential privileges from config
        essential_privileges = get_essential_privileges()
        
        for grant in grants:
            privilege = grant.get("privilege")
            grantee = grant.get("grantee_name")
            granted_to = grant.get("granted_to")
            grant_option = grant.get("grant_option")
            
            if privilege and grantee and granted_to:
                # Apply role mapping
                mapped_role = role_map.get(grantee, grantee)
                
                # Build grant statement - using correct template variables
                grant_stmt = f"GRANT {privilege} ON SCHEMA IDENTIFIER('{{{{ DB_PREFIX }}}}_{{{{ DB_BASE }}}}_{{{{ ENV }}}}.{schema}') TO {granted_to} {mapped_role}"
                
                if str(grant_option).upper() == "TRUE":
                    grant_stmt += " WITH GRANT OPTION"
                
                grant_stmt += ";"
                grant_statements.append(grant_stmt)
        
    except Exception as e:
        print(f"  ⚠️  Could not extract grants: {e}")
    
    return grant_statements
