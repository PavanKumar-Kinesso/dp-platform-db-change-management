"""
Manages committing approved schemas from temporary directories to main schema folders.
"""

import json
import os
import shutil
from pathlib import Path
from datetime import datetime

from .state_manager import WorkflowStateManager
from .workflow_utils import WorkflowUtils


class CommitManager:
    """Manages committing approved schemas."""
    
    def __init__(self, temp_dir):
        self.temp_dir = Path(temp_dir)
        self.final_dir = self.temp_dir / "final"
        self.main_schema_dir = self.temp_dir.parent
        self.state_manager = WorkflowStateManager(temp_dir)
        self.decisions_file = self.temp_dir / "decisions.json"
    
    def commit_schema(self, schema_name, dry_run=False):
        """Commit the final schema version to the main schema directory."""
        
        # Validate workflow state
        if not self.state_manager.validate_state_transition("FINAL_GENERATED"):
            current_state = self.state_manager.get_current_state()
            print(f"❌ ERROR: Workflow state is '{current_state}', not 'FINAL_GENERATED'")
            self.state_manager.show_workflow_help(schema_name)
            return False
        
        # Check if final files exist
        if not self.final_dir.exists():
            print(f"❌ Final directory not found: {self.final_dir}")
            print(f"🔄 Generate final version first:")
            print(f"   python scripts/generate_final.py --schema {schema_name}")
            return False
        
        final_files = list(self.final_dir.glob("*.sql"))
        if not final_files:
            print(f"❌ No final files found in: {self.final_dir}")
            return False
        
        # Perform commit (no confirmation needed)
        if dry_run:
            print(f"\n🔍 DRY RUN MODE - No files will be modified")
            return self._simulate_commit(schema_name, final_files, [])
        else:
            result = self._perform_commit(schema_name, final_files)
            
            if result:
                # Show commit summary
                print(f"\n📋 COMMIT SUMMARY")
                print(f"Status: Schema committed successfully")
                print(f"Schema: {schema_name}")
                print(f"Files: {len(final_files)} SQL files committed")
                print(f"Source: {self.final_dir}")
                print(f"Target: {self.main_schema_dir}")
                
                # Show completion message
                print(f"\n🎉 WORKFLOW COMPLETE")
                print(f"Schema '{schema_name}' has been successfully extracted, reviewed, and committed!")
                print(f"Your schema is now ready for deployment using schemachange.")
            
            return result
    

    

    

    
    def _simulate_commit(self, schema_name, final_files, existing_files):
        """Simulate the commit operation (dry run)."""
        
        print(f"\n🔍 DRY RUN: Simulating commit operation")
        
        print(f"📥 Files that would be committed:")
        for final_file in final_files:
            print(f"   - {final_file.name} → {self.main_schema_dir / final_file.name}")
        
        print(f"\n✅ DRY RUN COMPLETE - No files were modified")
        print(f"💡 To perform actual commit, run without --dry-run")
        
        return True
    
    def _perform_commit(self, schema_name, final_files):
        """Perform the actual commit operation."""
        
        print(f"\n🚀 COMMITTING: {schema_name} Schema")
        
        # Copy final files to main schema directory
        committed_files = []
        for final_file in final_files:
            target_file = self.main_schema_dir / final_file.name
            
            # Copy the file (Git handles version control)
            shutil.copy2(final_file, target_file)
            committed_files.append(target_file)
            print(f"📄 Committed: {final_file.name}")
        
        # Update workflow state
        self.state_manager.update_state("COMMITTED", {
            'committed_at': datetime.now().isoformat(),
            'committed_files': [f.name for f in committed_files]
        })
        
        return True
    

    

    
    def show_commit_status(self, schema_name):
        """Show the current commit status."""
        
        current_state = self.state_manager.get_current_state()
        
        print(f"📋 COMMIT STATUS: {schema_name}")
        print(f"Current State: {current_state}")
        
        if current_state == "COMMITTED":
            # Show committed files
            committed_files = list(self.main_schema_dir.glob("*.sql"))
            if committed_files:
                print(f"\n✅ Committed Files:")
                for file_path in committed_files:
                    if not str(file_path).startswith(str(self.temp_dir)):
                        print(f"   - {file_path.name}")
            

        else:
            print(f"\n💡 Schema not yet committed")
            print(f"Current workflow state: {current_state}")
            self.state_manager.show_workflow_help(schema_name)
    
    def is_committed(self):
        """Check if schema has been committed."""
        return self.state_manager.validate_state_transition("COMMITTED")
