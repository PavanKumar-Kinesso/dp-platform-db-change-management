#!/usr/bin/env python3
"""
Apply/deploy Snowflake schemas locally using schemachange.
Simple wrapper around schemachange for local testing.
"""

import argparse
import sys
import os
import re
import subprocess
import tempfile
import yaml
import snowflake.connector as sf
import shutil
from pathlib import Path

from utils.connection import load_local_config


def preprocess_schema_files(schema_dir, env, db_prefix, db_base):
    """Preprocess schema files by replacing template variables using the new workflow system."""
    print(f"🔧 Preprocessing schema files for deployment using workflow system...")
    
    # Create a temporary processed directory
    temp_dir = tempfile.mkdtemp(prefix=f"schema_processed_{env}_")
    
    try:
        # Use the new workflow system for preprocessing
        from scripts.utils.workflow import FinalVersionGenerator
        
        # Create final generator to process the schema
        generator = FinalVersionGenerator(schema_dir)
        
        # Generate processed version with template variables replaced
        success = generator.generate_final_version(
            os.path.basename(schema_dir), 
            target_env=env,
            db_prefix=db_prefix,
            db_base=db_base
        )
        
        if success:
            print(f"✅ Preprocessing complete using workflow system")
            return temp_dir
        else:
            raise Exception("Workflow preprocessing failed")
        
    except Exception as e:
        print(f"❌ Preprocessing failed: {e}")
        # Clean up temp directory
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise


def create_schemachange_config(target_env, local_config, db_prefix="TEST", schema=None, db_base=None):
    """Create schemachange configuration for target environment (v4 format)."""
    # Get database from connection config if available
    target_database = local_config.get('database', '')
    
    # Replace template variables in database name
    if target_database and '{{' in target_database:
        target_database = target_database.replace('{{ENV}}', target_env.upper())
        target_database = target_database.replace('{{DB_PREFIX}}', db_prefix or 'TEST')
        if db_base:
            target_database = target_database.replace('{{DB_BASE}}', db_base)
    
    # If no database specified, create one using the pattern
    if not target_database:
        base_db = db_base or 'PLATFORM'
        target_database = f'{db_prefix}_{base_db}_{target_env.upper()}'
    
    # For schemachange v4, we use minimal config and rely on connections.toml
    # Don't specify change-history-table to let schemachange use default
    config = {
        'config-version': 1,
        'create-change-history-table': True,
        'vars': {
            'ENV': target_env.upper(),
            'DB_PREFIX': db_prefix or 'TEST',
            'DB_BASE': db_base or 'PLATFORM',
        }
    }
    
    # Add schema if provided
    if schema:
        config['vars']['SCHEMA'] = schema
    
    return config


def run_schemachange(schema_dir, config_file, connection_name, target_env, db_prefix="TEST", dry_run=False, verbose=True, db_base=None):
    """Run schemachange for a schema directory using connections.toml (v4 format)."""
    # Set environment variables for schemachange to use
    env = os.environ.copy()
    env['ENV'] = target_env.upper()
    env['DB_PREFIX'] = db_prefix or 'TEST'  # Default to TEST if None
    
    # Set schema name from directory
    schema_name = os.path.basename(schema_dir)
    if schema_name:
        env['SCHEMA'] = schema_name
        # Set connection type (SRC or TGT) and schema-specific connection
        env['SC_CONNECTION_TYPE'] = connection_name  # SRC or TGT
        # Full connection name will be constructed in config
    
    # Set database base name (ALTO, PLATFORM, etc.)
    if db_base:
        env['DB_BASE'] = db_base
    
    # Load the target database from connection config
    try:
        # Try to load schema-specific connection first
        target_config = load_local_config(
            connections_file="connections.toml", 
            connection_name=connection_name,
            schema=schema_name
        )
        
        # Get database name from connection
        target_database = target_config.get('database', '')
        
        # Replace template variables in database name
        if target_database and '{{' in target_database:
            target_database = target_database.replace('{{ENV}}', target_env.upper())
            target_database = target_database.replace('{{DB_PREFIX}}', db_prefix or 'TEST')
            if db_base:
                target_database = target_database.replace('{{DB_BASE}}', db_base)
        
        if target_database:
            env['TARGET_DATABASE'] = target_database
            env['DB_NAME'] = target_database
            print(f"📊 Using database from connection: {target_database}")
    except Exception as e:
        print(f"⚠️  Could not load target database from connection: {e}")
        
    # Set target schema from the schema directory name
    if schema_name:
        env['TARGET_SCHEMA'] = schema_name
        print(f"📊 Using schema from folder name: {schema_name}")
    
    cmd = [
        'schemachange', 'deploy',
        '--config-folder', os.path.dirname(config_file) or '.',
        '--config-file-name', os.path.basename(config_file),
        '--root-folder', schema_dir
    ]
    
    if dry_run:
        cmd.append('--dry-run')
    
    if verbose:
        cmd.append('--verbose')
    
    print(f"Running: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, env=env, check=True, capture_output=True, text=True)
        if verbose and result.stdout:
            print("Schemachange output:")
            print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Schemachange failed with exit code {e.returncode}")
        print("STDOUT:", e.stdout)
        print("STDERR:", e.stderr)
        return False


def run_comparison(target_env, schema, local_config, schema_dir, db_prefix):
    """Run pre-deployment comparison. Returns 0 if no changes, 2 if changes, 1 if error."""
    try:
        # Import comparison functions
        sys.path.append(os.path.dirname(__file__))
        from compare_schema import extract_current_state, load_proposed_changes, normalize_ddl, normalize_grant
        
        # Connect to target (same logic as compare_schema.py)
        if db_prefix and db_prefix.lower() != "none":
            target_database = f"{db_prefix}_PLATFORM_{target_env.upper()}"
        else:
            current_db = local_config.get('database', 'PLATFORM_SIT')
            import re
            base_db = re.sub(r'_(SIT|DEV|QA|UAT|PROD)$', '', current_db)
            target_database = f"{base_db}_{target_env.upper()}"
        
        # Create target connection config
        target_config = local_config.copy()
        target_config['database'] = target_database
        
        target_conn = sf.connect(**target_config)
        
        # Extract current state (handle case where schema doesn't exist yet)
        try:
            current_objects, current_grants = extract_current_state(target_conn, target_database, schema, target_env)
        except Exception as e:
            if "does not exist" in str(e):
                print(f"📋 Schema {schema} doesn't exist in target database - this is a first-time deployment")
                return 2  # Proceed with deployment
            else:
                raise e
        
        # Load proposed changes
        proposed_objects, proposed_grants = load_proposed_changes(schema_dir, target_env, target_database)
        
        # Compare objects (simplified)
        all_objects = set(current_objects.keys()) | set(proposed_objects.keys())
        
        modified_objects = 0
        for obj_key in all_objects:
            current_ddl = current_objects.get(obj_key, '').strip()
            proposed_ddl = proposed_objects.get(obj_key, '').strip()
            current_normalized = normalize_ddl(current_ddl)
            proposed_normalized = normalize_ddl(proposed_ddl)
            if current_normalized != proposed_normalized:
                modified_objects += 1
        
        # Compare grants (simplified)
        current_normalized_grants = {normalize_grant(g) for g in current_grants}
        proposed_normalized_grants = {normalize_grant(g) for g in proposed_grants}
        grant_differences = len(proposed_normalized_grants - current_normalized_grants)
        
        total_meaningful = modified_objects + grant_differences
        
        target_conn.close()
        
        if total_meaningful == 0:
            return 0  # No changes
        else:
            return 2  # Changes detected
            
    except Exception as e:
        print(f"⚠️  Comparison check failed: {e}")
        return 1  # Error


def main():
    """Apply schemas to target environment using schemachange."""
    ap = argparse.ArgumentParser(description="Apply Snowflake schemas using schemachange")
    ap.add_argument("--connections", default="connections.toml", help="Path to connections.toml file")
    ap.add_argument("--connection", default="TGT", help="Connection type for deployment (SRC or TGT)")
    ap.add_argument("--target", default="DEV", help="Target environment (DEV, SIT, QA, UAT, PROD)")
    ap.add_argument("--db-prefix", help="Database name prefix (e.g., TEST, PROD)")
    ap.add_argument("--db-base", help="Database base name (e.g., ALTO, PLATFORM)")
    ap.add_argument("--target-database", help="Exact target database name (overrides connection config)")
    ap.add_argument("--schema-folder", help="Schema folder name to deploy (e.g., DATA_AMS, REPORTINGAPPS)")
    ap.add_argument("--schema-root", default="schemas", help="Root directory containing schema folders")
    ap.add_argument("--dry-run", action="store_true", help="Validate only, don't deploy")
    ap.add_argument("--verbose", action="store_true", default=True, help="Enable verbose logging")
    args = ap.parse_args()

    try:
        # Validate target environment
        valid_envs = ['DEV', 'SIT', 'QA', 'UAT', 'PROD']
        if args.target.upper() not in valid_envs:
            print(f"❌ Invalid target environment: {args.target}")
            print(f"Valid environments: {', '.join(valid_envs)}")
            return 1
        
        target_env = args.target.upper()
        db_prefix = args.db_prefix.upper() if args.db_prefix else None
        mode = "Validation" if args.dry_run else "Deployment"
        
        # Load local configuration to get schema if not provided
        try:
            # First determine schema from folder structure
            if args.schema_folder:
                # Use specified schema folder
                schema_name = args.schema_folder
                schema_dir = os.path.join(args.schema_root, schema_name)
                if not os.path.exists(schema_dir):
                    print(f"❌ Schema folder not found: {schema_dir}")
                    return 1
            else:
                # Auto-detect available schema folders
                available_schemas = [d for d in os.listdir(args.schema_root) 
                                  if os.path.isdir(os.path.join(args.schema_root, d)) and not d.startswith('.')]
                if len(available_schemas) == 1:
                    schema_name = available_schemas[0]
                    schema_dir = os.path.join(args.schema_root, schema_name)
                    print(f"📁 Auto-detected schema folder: {schema_name}")
                elif len(available_schemas) > 1:
                    print(f"❌ Multiple schema folders found: {', '.join(available_schemas)}")
                    print("Please specify --schema-folder to choose one")
                    return 1
                else:
                    print(f"❌ No schema folders found in {args.schema_root}")
                    return 1
            
            # Try to load schema-specific connection first
            try:
                local_config = load_local_config(
                    args.connections, 
                    args.connection,
                    schema_name
                )
                print(f"📊 Using schema-specific connection: {args.connection}_{schema_name}")
            except KeyError:
                # Fall back to generic connection
                try:
                    local_config = load_local_config(args.connections, args.connection)
                    print(f"📊 Using generic connection: {args.connection}")
                except KeyError as e:
                    print(f"❌ Connection '{args.connection}' not found in {args.connections}")
                    return 1
        except FileNotFoundError:
            print(f"❌ Configuration file not found: {args.connections}")
            return 1
        
        # Schema name and directory are already determined above
        
        # Determine database base name (ALTO, PLATFORM, etc.)
        db_base = args.db_base
        if not db_base:
            # Map schema names to database types based on connections.toml structure
            schema_to_db_mapping = {
                'DATA_AMS': 'ALTO',
                'Monitoring': 'PLATFORM',
                'ReportingApps': 'PLATFORM'
            }
            
            db_base = schema_to_db_mapping.get(schema_name, 'PLATFORM')
            print(f"📊 Mapped schema {schema_name} to database type: {db_base}")
        
        # Determine target database
        if args.target_database:
            # Use exact database name provided
            target_database = args.target_database
        else:
            # Get database from connection config
            target_database = local_config.get('database', '')
            
            # Replace template variables in database name
            if target_database and '{{' in target_database:
                target_database = target_database.replace('{{ENV}}', target_env)
                target_database = target_database.replace('{{DB_PREFIX}}', db_prefix or 'TEST')
                target_database = target_database.replace('{{DB_BASE}}', db_base)
            elif not target_database:
                # Create database name using pattern
                target_database = f'{db_prefix or "TEST"}_{db_base}_{target_env}'
        
        print(f"🚀 Starting {mode} to {target_env} environment")
        print(f"📂 Schema: {schema_name}")
        print(f"🎯 Target Database: {target_database}")
        print(f"📁 Schema Folder: {schema_dir}")
        
        # Create schemachange config
        sc_config = create_schemachange_config(target_env, local_config, db_prefix)
        
        # Process the schema
        print(f"\n📋 Processing schema: {schema_name}")
        
        # Run pre-deployment comparison if available
        try:
            print(f"🔍 Running pre-deployment comparison...")
            comparison_result = run_comparison(target_env, schema_name.upper(), local_config, schema_dir, db_prefix)
            
            if comparison_result == 0:
                print(f"✅ MIGRATION STATUS: COMPLETE - Schema {schema_name} already matches target state")
                print(f"🎯 Proceeding with schemachange to establish change tracking (no-op deployment)")
            elif comparison_result == 2:
                print(f"📝 Changes detected - proceeding with deployment")
            else:
                print(f"❌ Comparison failed for schema {schema_name}")
                return 1
        except ImportError:
            print(f"📋 Schema {schema_name} doesn't exist in target database - this is a first-time deployment")
        except Exception as e:
            print(f"⚠️  Comparison failed: {e}")
            print(f"📝 Changes detected - proceeding with deployment")
        
        # Find SQL files
        sql_files = []
        for root, dirs, files in os.walk(schema_dir):
            for file in files:
                if file.endswith('.sql'):
                    sql_files.append(os.path.join(root, file))
        
        if not sql_files:
            print(f"⚠️  No SQL files found in {schema_dir}")
            return 1
        
        print(f"📄 Found {len(sql_files)} SQL file(s):")
        for sql_file in sorted(sql_files):
            print(f"  - {os.path.basename(sql_file)}")
        
        # Preprocess schema files to replace template variables
        try:
            processed_schema_dir = preprocess_schema_files(schema_dir, target_env, db_prefix, db_base)
            print(f"🔧 Using preprocessed schema directory: {processed_schema_dir}")
        except Exception as e:
            print(f"⚠️  Preprocessing failed, using original schema directory: {e}")
            processed_schema_dir = schema_dir
        
        # Use schema-specific config if available, otherwise fall back to main config
        schema_config_path = os.path.join(schema_dir, "schema-config.yml")
        if os.path.exists(schema_config_path):
            config_file_path = schema_config_path
            print(f"📄 Using schema-specific config: {schema_config_path}")
        else:
            config_file_path = "schemachange-config.yml"
            print(f"📄 Using default config: {config_file_path}")
        
        # Run schemachange with preprocessed directory
        try:
            if run_schemachange(processed_schema_dir, config_file_path, args.connection, target_env, db_prefix, args.dry_run, args.verbose, db_base):
                print(f"\n✅ Successfully {'validated' if args.dry_run else 'deployed'} {schema_name}")
                print(f"📊 {mode} Summary:")
                print(f"  - Target: {target_database}")
                print(f"  - Schema: {schema_name}")
                print(f"  - Mode: {'Dry Run' if args.dry_run else 'Live Deployment'}")
                return 0
            else:
                print(f"\n❌ Failed to {'validate' if args.dry_run else 'deploy'} {schema_name}")
                return 1
        finally:
            # Clean up temporary processed directory if it was created
            if processed_schema_dir != schema_dir and os.path.exists(processed_schema_dir):
                try:
                    shutil.rmtree(processed_schema_dir)
                    print(f"🧹 Cleaned up temporary directory: {processed_schema_dir}")
                except Exception as e:
                    print(f"⚠️  Could not clean up temporary directory: {e}")
        
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
