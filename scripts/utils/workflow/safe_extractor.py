"""
Safe schema extraction with temporary files and workflow state management.
"""

import os
import shutil
import tempfile
from pathlib import Path
from datetime import datetime

from ..extraction import extract_schemas
from .state_manager import WorkflowStateManager


class SafeSchemaExtractor:
    """Safely extracts schemas to temporary directories for review."""
    
    def __init__(self, schema_name, database, connection_config, output_root="schemas"):
        self.schema_name = schema_name
        self.database = database
        self.connection_config = connection_config
        self.output_root = Path(output_root)
        self.temp_dir = self.output_root / schema_name / "temp"
        self.state_manager = WorkflowStateManager(self.temp_dir)
    
    def extract_safe(self, db_base=None, db_prefix=None):
        """Extract schema safely to temporary directory."""
        
        # AUTO-CLEAN: Remove any existing temp files and reset workflow state
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
        
        # Create fresh temp structure
        temp_structure = {
            'raw': self.temp_dir / "raw",
            'analysis': self.temp_dir / "analysis", 
            'suggested': self.temp_dir / "suggested",
            'final': self.temp_dir / "final"
        }
        
        for dir_path in temp_structure.values():
            dir_path.mkdir(parents=True, exist_ok=True)
        
        # Reset workflow state to NOT_STARTED
        self.state_manager.reset_workflow()
        
        print(f"🔄 Fresh extraction started...")
        
        try:
            # Extract raw DDL (no templating)
            results = extract_schemas(
                database=self.database,
                schemas=[self.schema_name],
                connection_config=self.connection_config,
                output_dir=str(temp_structure['raw']),
                db_prefix=db_prefix,
                db_base=db_base
            )
            
            if not results:
                raise Exception("No schemas extracted")
            
            # Validate extraction success
            schema_result = results[0]  # We only extract one schema
            if schema_result['object_count'] == 0 and schema_result['grant_count'] == 0:
                # Check if this is due to schema not existing or access denied
                raise Exception(f"Schema '{self.database}.{self.schema_name}' not found or access denied. No objects or grants extracted.")
            
            # Move extracted files to raw directory
            for result in results:
                # Move DDL file
                ddl_file = Path(result['ddl_file'])
                if ddl_file.exists():
                    new_ddl_file = temp_structure['raw'] / ddl_file.name
                    shutil.move(str(ddl_file), str(new_ddl_file))
                
                # Move grants file
                grant_file = Path(result['grant_file'])
                if grant_file.exists():
                    new_grant_file = temp_structure['raw'] / grant_file.name
                    shutil.move(str(grant_file), str(new_grant_file))
            
            # Update workflow state
            self.state_manager.update_state("EXTRACTION_COMPLETE", {
                'extracted_at': datetime.now().isoformat(),
                'database': self.database,
                'schema': self.schema_name,
                'files': [str(f) for f in temp_structure['raw'].glob("*.sql")]
            })
            
            # Show extraction summary
            print(f"\n📋 EXTRACTION SUMMARY")
            print(f"Status: Extraction complete")
            print(f"Schema: {self.schema_name}")
            print(f"Database: {self.database}")
            print(f"Files: {len(list(temp_structure['raw'].glob('*.sql')))} SQL files extracted")
            print(f"Location: {self.temp_dir}")
            
            return True
            
        except Exception as e:
            print(f"❌ Extraction failed: {e}")
            # Clean up on failure
            if self.temp_dir.exists():
                shutil.rmtree(self.temp_dir)
            return False
    
    def get_temp_structure(self):
        """Get temporary directory structure."""
        return {
            'raw': self.temp_dir / "raw",
            'analysis': self.temp_dir / "analysis", 
            'suggested': self.temp_dir / "suggested",
            'final': self.temp_dir / "final"
        }
    
    def cleanup(self):
        """Clean up temporary files."""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
            print(f"🧹 Cleaned temporary files for {self.schema_name}")
    
    def is_extraction_complete(self):
        """Check if extraction is complete."""
        return self.state_manager.validate_state_transition("EXTRACTION_COMPLETE")
