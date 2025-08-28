"""
Workflow state management for schema extraction and templating workflow.
"""

import json
import os
from datetime import datetime
from pathlib import Path


class WorkflowStateManager:
    """Manages workflow state and transitions."""
    
    WORKFLOW_STEPS = {
        "NOT_STARTED": {
            "next": "EXTRACTION_COMPLETE",
            "script": "export_schema.py --workflow extract",
            "description": "Start extraction"
        },
        "EXTRACTION_COMPLETE": {
            "next": "REVIEW_COMPLETE", 
            "script": "review_templating.py",
            "description": "Review templating suggestions"
        },
        "REVIEW_COMPLETE": {
            "next": "FINAL_GENERATED",
            "script": "export_schema.py --workflow generate",
            "description": "Generate final version"
        },
        "FINAL_GENERATED": {
            "next": "COMMITTED",
            "script": "commit_schema.py",
            "description": "Commit to main schema folder"
        }
    }
    
    def __init__(self, temp_dir):
        self.temp_dir = Path(temp_dir)
        self.state_file = self.temp_dir / "workflow_state.json"
    
    def get_current_state(self):
        """Get current workflow state."""
        if not self.state_file.exists():
            return "NOT_STARTED"
        
        try:
            with open(self.state_file, 'r') as f:
                state = json.load(f)
                return state.get('current_step', 'UNKNOWN')
        except Exception:
            return 'UNKNOWN'
    
    def update_state(self, step, details=None):
        """Update workflow state."""
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        
        state = {
            'current_step': step,
            'last_updated': datetime.now().isoformat(),
            'details': details or {}
        }
        
        with open(self.state_file, 'w') as f:
            json.dump(state, f, indent=2)
    
    def validate_state_transition(self, expected_state):
        """Validate current state allows the requested operation."""
        current_state = self.get_current_state()
        return current_state == expected_state
    
    def get_next_step_info(self):
        """Get information about the next step."""
        current_state = self.get_current_state()
        
        if current_state in self.WORKFLOW_STEPS:
            return self.WORKFLOW_STEPS[current_state]
        return None
    
    def show_workflow_help(self, schema_name):
        """Show helpful workflow guidance."""
        current_state = self.get_current_state()
        
        print(f"--------------------------------")
        print(f"WORKFLOW HELP for {schema_name}")
        print(f" Current state: {current_state}")
        
        if current_state in self.WORKFLOW_STEPS:
            step_info = self.WORKFLOW_STEPS[current_state]
            print(f" Next step: {step_info['description']}")
            # print(f"Command: python scripts/{step_info['script']} --schema {schema_name}")
        else:
            print(f"Unknown state. Restart extraction:")
            print(f"python scripts/export_schema.py --workflow extract --schema {schema_name}")
    
    def reset_workflow(self):
        """Reset workflow to initial state."""
        if self.state_file.exists():
            self.state_file.unlink()
        self.update_state("NOT_STARTED")
