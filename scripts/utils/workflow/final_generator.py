"""
Generates final approved schema versions based on review decisions.
"""

import json
import os
import shutil
from pathlib import Path
from datetime import datetime

from .state_manager import WorkflowStateManager
from .workflow_utils import WorkflowUtils


class FinalVersionGenerator:
    """Generates final approved schema versions."""
    
    def __init__(self, temp_dir):
        self.temp_dir = Path(temp_dir)
        self.raw_dir = self.temp_dir / "raw"
        self.suggested_dir = self.temp_dir / "suggested"
        self.final_dir = self.temp_dir / "final"
        self.state_manager = WorkflowStateManager(temp_dir)
        self.decisions_file = self.temp_dir / "decisions.json"
    
    def generate_final_version(self, schema_name):
        """Generate final version based on review decisions."""
        
        # Validate workflow state
        if not self.state_manager.validate_state_transition("REVIEW_COMPLETE"):
            current_state = self.state_manager.get_current_state()
            print(f"❌ ERROR: Workflow state is '{current_state}', not 'REVIEW_COMPLETE'")
            self.state_manager.show_workflow_help(schema_name)
            return False
        
        # Check required files
        required_files = [
            "raw",
            "decisions.json"
        ]
        
        missing_files = WorkflowUtils.check_required_files(self.temp_dir, required_files)
        if missing_files:
            print(f"❌ ERROR: Missing required files:")
            for f in missing_files:
                print(f"   - {f}")
            print(f"🔄 Review may be incomplete. Restart extraction:")
            print(f"   python scripts/export_schema.py --workflow extract --schema {schema_name}")
            return False
        
        print(f"✅ Generating final version (bcoz workflow state valid)...")
        
        # Load decisions
        decisions = self._load_decisions()
        if not decisions:
            print(f"⚠️  No review decisions found")
            # Still generate final version if no decisions
            self._generate_final_without_decisions(schema_name)
            return True
        
        # Generate final version based on decisions
        self._generate_final_with_decisions(schema_name, decisions)
        
        # Show generation summary
        print(f"\n📋 GENERATION SUMMARY")
        print(f"Status: Final version generated")
        print(f"Schema: {schema_name}")
        print(f"Method: Based on {len(decisions)} review decisions")
        print(f"Files: {len(list(self.final_dir.glob('*.sql')))} SQL files created")
        print(f"Location: {self.final_dir}")
        
        # Show next step
        print(f"\n🚀 NEXT STEP")
        print(f"Command: python scripts/export_schema.py --workflow commit --schema {schema_name}")
        print(f"Purpose: Commit final version to main schema directory")
        
        return True
    
    def _load_decisions(self):
        """Load review decisions from file."""
        if not self.decisions_file.exists():
            return []
        
        try:
            with open(self.decisions_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️  Could not load decisions: {e}")
            return []
    
    def _generate_final_without_decisions(self, schema_name):
        """Generate final version when no decisions exist."""
        
        self.final_dir.mkdir(parents=True, exist_ok=True)
        
        # Copy raw files to final directory
        for sql_file in self.raw_dir.glob("*.sql"):
            final_file = self.final_dir / sql_file.name
            shutil.copy2(sql_file, final_file)
            
            # Add header comment
            with open(final_file, 'r') as f:
                content = f.read()
            
            header = f"""-- FINAL VERSION: {schema_name} Schema
-- Generated at: {datetime.now().isoformat()}
-- Note: No templating decisions were made - using raw extraction
-- ⚠️  This version may contain environment-specific references

"""
            
            with open(final_file, 'w') as f:
                f.write(header + content)
            
            print(f"📄 Final version generated: {final_file}")
        
        # Update workflow state
        self.state_manager.update_state("FINAL_GENERATED", {
            'generated_at': datetime.now().isoformat(),
            'method': 'no_decisions',
            'files_generated': [f.name for f in self.final_dir.glob("*.sql")]
        })
        
        # Show generation summary
        print(f"\n📋 GENERATION SUMMARY")
        print(f"Status: Final version generated")
        print(f"Schema: {schema_name}")
        print(f"Method: No review decisions - using raw extraction")
        print(f"Files: {len(list(self.final_dir.glob('*.sql')))} SQL files created")
        print(f"Location: {self.final_dir}")
        
        # Show next step
        print(f"\n🚀 NEXT STEP")
        print(f"Command: python scripts/export_schema.py --workflow commit --schema {schema_name}")
        print(f"Purpose: Commit final version to main schema directory")
    
    def _generate_final_with_decisions(self, schema_name, decisions):
        """Generate final version based on review decisions."""
        
        self.final_dir.mkdir(parents=True, exist_ok=True)
        
        # Group decisions by file
        file_decisions = self._group_decisions_by_file(decisions)
        
        # Process each SQL file
        for sql_file in self.raw_dir.glob("*.sql"):
            final_file = self.final_dir / sql_file.name
            
            # Load original content
            with open(sql_file, 'r') as f:
                content = f.read()
            
            # Apply accepted decisions for this file
            applied_changes = []
            file_name = sql_file.name
            
            if file_name in file_decisions:
                for decision in file_decisions[file_name]:
                    if decision['decision'] == 'accept':
                        # Apply the change
                        original_count = content.count(decision['original'])
                        content = content.replace(decision['original'], decision['suggested'])
                        new_count = content.count(decision['suggested'])
                        
                        if new_count > 0:
                            applied_changes.append({
                                'original': decision['original'],
                                'suggested': decision['suggested'],
                                'context': decision['context'],
                                'reason': decision['reason']
                            })
            
            # Write final version with header
            header = self._generate_final_header(schema_name, file_name, applied_changes)
            
            with open(final_file, 'w') as f:
                f.write(header + content)
            
            print(f"📄 Final version generated: {final_file}")
            if applied_changes:
                print(f"   Applied {len(applied_changes)} templating changes")
            else:
                print(f"   No changes applied (using raw version)")
        
        # Update workflow state
        self.state_manager.update_state("FINAL_GENERATED", {
            'generated_at': datetime.now().isoformat(),
            'method': 'with_decisions',
            'total_decisions': len(decisions),
            'accepted_decisions': len([d for d in decisions if d['decision'] == 'accept']),
            'files_generated': [f.name for f in self.final_dir.glob("*.sql")]
        })
        
        # Show summary
        self._show_generation_summary(decisions)
        
        print(f"\n✅ Final version generated based on {len(decisions)} decisions")
        print(f"📁 Final files: {self.final_dir}")
        print(f"🚀 Next: Review final version and commit if satisfied")
    
    def _group_decisions_by_file(self, decisions):
        """Group decisions by the file they apply to."""
        file_decisions = {}
        
        for decision in decisions:
            # For now, assume all decisions apply to all files
            # In the future, we could make this more sophisticated
            # by analyzing which files contain which references
            
            # Get all SQL files
            for sql_file in self.raw_dir.glob("*.sql"):
                file_name = sql_file.name
                if file_name not in file_decisions:
                    file_decisions[file_name] = []
                file_decisions[file_name].append(decision)
        
        return file_decisions
    
    def _generate_final_header(self, schema_name, file_name, applied_changes):
        """Generate header for final version file."""
        
        header = f"""-- FINAL VERSION: {schema_name} Schema
-- File: {file_name}
-- Generated at: {datetime.now().isoformat()}
-- Based on: {len(applied_changes)} templating decisions

"""
        
        if applied_changes:
            header += "-- APPLIED TEMPLATING CHANGES:\n"
            for i, change in enumerate(applied_changes, 1):
                header += f"-- {i}. {change['original']} → {change['suggested']}\n"
                header += f"--    Context: {change['context']}\n"
                header += f"--    Reason: {change['reason']}\n"
            header += "\n"
        else:
            header += "-- No templating changes applied - using raw extraction\n\n"
        
        header += "-- ==========================================\n"
        header += "-- SCHEMA CONTENT BELOW\n"
        header += "-- ==========================================\n\n"
        
        return header
    
    def _show_generation_summary(self, decisions):
        """Show summary of final version generation."""
        
        if not decisions:
            print(f"\n📊 Generation Summary: No decisions to apply")
            return
        
        accepted = [d for d in decisions if d['decision'] == 'accept']
        rejected = [d for d in decisions if d['decision'] == 'reject']
        
        print(f"\n📊 FINAL VERSION SUMMARY")
        print(f"Total Decisions: {len(decisions)}")
        print(f"✅ Applied Changes: {len(accepted)}")
        print(f"❌ Ignored Changes: {len(rejected)}")
        
        if accepted:
            print(f"\n✅ APPLIED TEMPLATING CHANGES:")
            for decision in accepted:
                print(f"  - {decision['original']} → {decision['suggested']}")
        
        if rejected:
            print(f"\n❌ IGNORED TEMPLATING CHANGES:")
            for decision in rejected:
                print(f"  - {decision['original']} → {decision['suggested']}")
                if decision['user_notes']:
                    print(f"    Note: {decision['user_notes']}")
    
    def get_final_files(self):
        """Get list of generated final files."""
        if not self.final_dir.exists():
            return []
        
        return [f for f in self.final_dir.glob("*.sql")]
    
    def is_final_generated(self):
        """Check if final version has been generated."""
        return self.state_manager.validate_state_transition("FINAL_GENERATED")
    
    def show_final_preview(self, schema_name):
        """Show preview of final generated files."""
        
        final_files = self.get_final_files()
        if not final_files:
            print(f"❌ No final files found. Generate final version first:")
            print(f"   python scripts/generate_final.py --schema {schema_name}")
            return
        
        print(f"\n📋 FINAL VERSION PREVIEW: {schema_name}")
        print(f"Generated {len(final_files)} files:")
        
        for final_file in final_files:
            print(f"\n📄 {final_file.name}:")
            
            # Show first few lines as preview
            with open(final_file, 'r') as f:
                lines = f.readlines()[:10]  # First 10 lines
            
            for line in lines:
                if line.strip():
                    print(f"   {line.rstrip()}")
            
            if len(lines) == 10:
                print(f"   ... (showing first 10 lines)")
        
        print(f"\n💡 To see full content: cat {self.final_dir}/*.sql")
        print(f"🚀 Ready for commit: python scripts/commit_schema.py --schema {schema_name}")
