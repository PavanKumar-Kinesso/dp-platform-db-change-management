#!/usr/bin/env python3
"""
Preprocesses schema files by replacing template variables before schemachange application.
This script handles environment-specific references and database naming.
"""

import argparse
import sys
import os
import re
from pathlib import Path
from utils.connection import load_local_config


def replace_template_variables(content, env, db_prefix, db_base):
    """Replace template variables in SQL content."""
    # Replace environment variables
    content = content.replace('{{ENV}}', env.upper())
    content = content.replace('{{ ENV }}', env.upper())
    
    # Replace database variables
    content = content.replace('{{DB_PREFIX}}', db_prefix)
    content = content.replace('{{ DB_PREFIX }}', db_prefix)
    content = content.replace('{{DB_BASE}}', db_base)
    content = content.replace('{{ DB_BASE }}', db_base)
    
    return content


def preprocess_schema_files(schema_dir, env, db_prefix, db_base, output_dir=None):
    """Preprocess all SQL files in a schema directory."""
    schema_path = Path(schema_dir)
    if not schema_path.exists():
        raise ValueError(f"Schema directory does not exist: {schema_dir}")
    
    # Use output_dir if specified, otherwise create a temp directory
    if output_dir:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
    else:
        # Create a temporary directory with _processed suffix
        output_path = schema_path.parent / f"{schema_path.name}_processed"
        output_path.mkdir(exist_ok=True)
    
    processed_files = []
    
    # Process all SQL files in the schema directory
    for sql_file in schema_path.glob("*.sql"):
        print(f"🔧 Processing: {sql_file.name}")
        
        # Read the original file
        with open(sql_file, 'r') as f:
            content = f.read()
        
        # Replace template variables
        processed_content = replace_template_variables(content, env, db_prefix, db_base)
        
        # Write to output directory
        output_file = output_path / sql_file.name
        with open(output_file, 'w') as f:
            f.write(processed_content)
        
        processed_files.append(str(output_file))
        print(f"  ✅ Processed: {output_file}")
    
    return str(output_path), processed_files


def main():
    """Main function to preprocess schema files."""
    ap = argparse.ArgumentParser(description="Preprocess schema files by replacing template variables")
    ap.add_argument("--schema-dir", required=True, help="Schema directory containing SQL files")
    ap.add_argument("--env", required=True, help="Target environment (SIT, QA, UAT, PROD)")
    ap.add_argument("--db-prefix", required=True, help="Database prefix for target environment")
    ap.add_argument("--db-base", required=True, help="Database base type (ALTO, PLATFORM, etc.)")
    ap.add_argument("--output-dir", help="Output directory (defaults to schema_dir_processed)")
    ap.add_argument("--connections", default="connections.toml", help="Path to connections.toml file")
    ap.add_argument("--connection-name", default="TGT", help="Connection name to use for validation")
    
    args = ap.parse_args()
    
    try:
        # Validate environment
        valid_envs = ['SIT', 'QA', 'UAT', 'PROD']
        if args.env.upper() not in valid_envs:
            raise ValueError(f"Invalid environment: {args.env}. Must be one of: {', '.join(valid_envs)}")
        
        # Validate database base type
        valid_db_bases = ['ALTO', 'PLATFORM']
        if args.db_base.upper() not in valid_db_bases:
            raise ValueError(f"Invalid database base type: {args.db_base}. Must be one of: {', '.join(valid_db_bases)}")
        
        print(f"🔧 Preprocessing schema files...")
        print(f"  📁 Schema directory: {args.schema_dir}")
        print(f"  🌍 Environment: {args.env.upper()}")
        print(f"  🗄️  Database prefix: {args.db_prefix}")
        print(f"  🏗️  Database base: {args.db_base.upper()}")
        
        # Preprocess the schema files
        output_dir, processed_files = preprocess_schema_files(
            args.schema_dir,
            args.env.upper(),
            args.db_prefix,
            args.db_base.upper(),
            args.output_dir
        )
        
        print(f"\n✅ Successfully preprocessed {len(processed_files)} files")
        print(f"📁 Output directory: {output_dir}")
        print(f"📋 Processed files:")
        for file_path in processed_files:
            print(f"  - {os.path.basename(file_path)}")
        
        print(f"\n🚀 Ready for schemachange deployment!")
        print(f"💡 Use this command to apply the schema:")
        print(f"   schemachange -f {output_dir} -c {args.connections} -a {args.connection_name}")
        
        return 0
        
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
