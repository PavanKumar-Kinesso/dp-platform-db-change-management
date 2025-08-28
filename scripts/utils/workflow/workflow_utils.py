"""
Common workflow utilities and helper functions.
"""

import os
import shutil
from pathlib import Path
from .state_manager import WorkflowStateManager


class WorkflowUtils:
    """Common workflow utility functions."""
    
    @staticmethod
    def validate_workflow_prerequisites(schema_name, expected_state, temp_dir):
        """Validate workflow prerequisites before proceeding."""
        
        # Check if temp directory exists
        if not os.path.exists(temp_dir):
            return False, f"""
❌ ERROR: No temporary extraction found for {schema_name}

💡 You must run extraction first:
   python scripts/export_schema.py --workflow extract --schema {schema_name}
"""
        
        # Check workflow state
        state_manager = WorkflowStateManager(temp_dir)
        current_state = state_manager.get_current_state()
        
        if current_state != expected_state:
            return False, f"""
❌ ERROR: Workflow state is '{current_state}', not '{expected_state}'

💡 Current workflow state: {current_state}
🔄 You may need to restart extraction:
   python scripts/export_schema.py --workflow extract --schema {schema_name}
"""
        
        return True, None
    
    @staticmethod
    def check_required_files(temp_dir, required_files):
        """Check if required files exist."""
        missing_files = []
        
        for file_path in required_files:
            full_path = os.path.join(temp_dir, file_path)
            if not os.path.exists(full_path):
                missing_files.append(file_path)
        
        return missing_files
    
    @staticmethod
    def show_workflow_error(schema_name, error_type, details=None):
        """Show helpful error messages with workflow guidance."""
        
        error_messages = {
            "NO_EXTRACTION": f"""
❌ ERROR: No extraction found for {schema_name}

💡 START HERE:
   python scripts/export_schema.py --workflow extract --schema {schema_name}

📋 This will:
   1. Clean any existing temp files
   2. Extract raw DDL safely
   3. Analyze templating opportunities
   4. Generate suggestions for review
""",
            
            "WRONG_STATE": f"""
❌ ERROR: Workflow state mismatch

💡 CURRENT STATE: {details.get('current_state', 'UNKNOWN')}
💡 EXPECTED STATE: {details.get('expected_state', 'UNKNOWN')}

🔄 RESTART EXTRACTION:
   python scripts/export_schema.py --workflow extract --schema {schema_name}

📋 This will clean everything and start fresh.
""",
            
            "MISSING_FILES": f"""
❌ ERROR: Required files missing

💡 MISSING FILES:
{chr(10).join(f"   - {f}" for f in details.get('missing_files', []))}

🔄 RESTART EXTRACTION:
   python scripts/export_schema.py --workflow extract --schema {schema_name}

📋 This will regenerate all required files.
"""
        }
        
        return error_messages.get(error_type, f"❌ Unknown error: {error_type}")
    
    @staticmethod
    def clean_temp_files(schema_name):
        """Clean all temporary files and start fresh."""
        temp_dir = f"schemas/{schema_name}/temp"
        
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
    
    @staticmethod
    def get_temp_directory_structure(schema_name):
        """Get the standard temp directory structure for a schema."""
        base_dir = f"schemas/{schema_name}/temp"
        return {
            'base': base_dir,
            'raw': f"{base_dir}/raw",
            'analysis': f"{base_dir}/analysis",
            'suggested': f"{base_dir}/suggested", 
            'final': f"{base_dir}/final"
        }
