"""
Interactive review of templating suggestions with human decision making.
"""

import json
import os
from pathlib import Path
from datetime import datetime

from .state_manager import WorkflowStateManager
from .workflow_utils import WorkflowUtils


class InteractiveReviewer:
    """Interactive review of templating suggestions."""
    
    def __init__(self, temp_dir):
        self.temp_dir = Path(temp_dir)
        self.analysis_dir = self.temp_dir / "analysis"
        self.suggested_dir = self.temp_dir / "suggested"
        self.state_manager = WorkflowStateManager(temp_dir)
        self.decisions_file = self.temp_dir / "decisions.json"
    
    def start_review(self, schema_name):
        """Start the interactive review process."""
        
        # Validate workflow state
        if not self.state_manager.validate_state_transition("EXTRACTION_COMPLETE"):
            current_state = self.state_manager.get_current_state()
            print(f"❌ ERROR: Workflow state is '{current_state}', not 'EXTRACTION_COMPLETE'")
            self.state_manager.show_workflow_help(schema_name)
            return False
        
        print(f"✅ Starting review (bcoz workflow state valid)...")
        
        # Run templating analysis first (if not already done)
        analysis_file = self.analysis_dir / "templating_analysis.md"
        if not analysis_file.exists():
            print(f"🔍 Running templating analysis...")
            from .templating_analyzer import TemplatingAnalyzer
            analyzer = TemplatingAnalyzer(self.temp_dir)
            analyzer.analyze_schema_files()
        
        # Check required files after analysis
        raw_dir = Path(self.temp_dir) / "raw"
        if not raw_dir.exists() or not list(raw_dir.glob("*.sql")):
            print(f"❌ ERROR: No raw SQL files found in {raw_dir}")
            print(f"🔄 Extraction may be incomplete. Restart extraction:")
            print(f"   python scripts/export_schema.py --workflow extract --schema {schema_name}")
            return False
        
        # Load analysis
        if not analysis_file.exists():
            print(f"❌ Analysis file still not found after running analysis")
            return False
        
        # Load suggestions from suggested directory
        suggestions = self._load_suggestions()
        if not suggestions:
            self._complete_review(schema_name, [])
            
            # Show next step
            print(f"\n🚀 NEXT STEP")
            print(f"Command: python scripts/export_schema.py --workflow generate --schema {schema_name}")
            print(f"Purpose: Generate final version based on review decisions")
            
            return True
        
        print(f"📋 Reviewing {len(suggestions)} suggestions...")
        
        # Interactive review
        decisions = []
        for i, suggestion in enumerate(suggestions, 1):
            decision = self._review_single_suggestion(i, suggestion, len(suggestions))
            decisions.append(decision)
            
            if decision['decision'] == 'quit':
                print(f"🛑 Review cancelled")
                return False
        
        # Complete review
        self._complete_review(schema_name, decisions)
        
        # Show review summary
        print(f"\n📋 REVIEW SUMMARY")
        print(f"Status: Review complete")
        print(f"Schema: {schema_name}")
        print(f"Suggestions: {len(decisions)} reviewed")
        print(f"Decisions: Saved to {self.temp_dir}/decisions.json")
        print(f"Report: {self.temp_dir}/analysis/templating_analysis.md")
        
        # Show next step
        print(f"\n🚀 NEXT STEP")
        print(f"Command: python scripts/export_schema.py --workflow generate --schema {schema_name}")
        print(f"Purpose: Generate final version based on review decisions")
        
        return True
    
    def _load_suggestions(self):
        """Load templating suggestions from the suggested directory."""
        suggestions = []
        
        if not self.suggested_dir.exists():
            return suggestions
        
        # Load analysis report to get detailed suggestions
        analysis_file = self.analysis_dir / "templating_analysis.md"
        if analysis_file.exists():
            # Parse the markdown to extract suggestions
            suggestions = self._parse_analysis_report(analysis_file)
        
        return suggestions
    
    def _parse_analysis_report(self, analysis_file):
        """Parse the analysis report to extract suggestions."""
        with open(analysis_file, 'r') as f:
            content = f.read()
        
        suggestions = []
        
        # Parse safe suggestions
        safe_section = self._extract_section(content, "Safe to Template")
        if safe_section:
            suggestions.extend(self._parse_suggestions_from_section(safe_section, True))
        
        # Parse unsafe suggestions
        unsafe_section = self._extract_section(content, "Needs Manual Review")
        if unsafe_section:
            suggestions.extend(self._parse_suggestions_from_section(unsafe_section, False))
        
        return suggestions
    
    def _extract_section(self, content, section_name):
        """Extract a section from the markdown content."""
        start_marker = f"### {section_name}"
        end_marker = "### "
        
        start_pos = content.find(start_marker)
        if start_pos == -1:
            return None
        
        end_pos = content.find(end_marker, start_pos + len(start_marker))
        if end_pos == -1:
            # Section goes to end of file
            return content[start_pos:]
        
        return content[start_pos:end_pos]
    
    def _parse_suggestions_from_section(self, section_content, is_safe):
        """Parse suggestions from a section of the analysis report."""
        suggestions = []
        
        # Look for numbered suggestions
        lines = section_content.split('\n')
        current_suggestion = None
        
        for line in lines:
            line = line.strip()
            
            # Check if this is a new suggestion (numbered line)
            if line and line[0].isdigit() and '. ' in line:
                # Save previous suggestion if exists
                if current_suggestion:
                    suggestions.append(current_suggestion)
                
                # Start new suggestion
                parts = line.split(' → ')
                if len(parts) == 2:
                    original = parts[0].split('. ')[1].strip('*')
                    suggested = parts[1].strip('*')
                    
                    current_suggestion = {
                        'type': 'database_reference',
                        'original': original,
                        'suggested': suggested,
                        'is_safe': is_safe,
                        'context': '',
                        'reason': '',
                        'risk_level': 'LOW' if is_safe else 'MEDIUM'
                    }
            
            # Extract context and reason from subsequent lines
            elif current_suggestion and line.startswith('- Context:'):
                current_suggestion['context'] = line.split('Context:')[1].strip().strip('`')
            elif current_suggestion and line.startswith('- Reason:'):
                current_suggestion['reason'] = line.split('Reason:')[1].strip()
        
        # Add last suggestion
        if current_suggestion:
            suggestions.append(current_suggestion)
        
        return suggestions
    
    def _review_single_suggestion(self, suggestion_num, suggestion, total_suggestions):
        """Review a single templating suggestion."""
        
        print(f"--- Suggestion {suggestion_num}/{total_suggestions} ---")
        print(f"Original: {suggestion['original']}")
        print(f"Suggested: {suggestion['suggested']}")
        print(f"Context: {suggestion['context']}")
        print(f"Reason: {suggestion['reason']}")
        print(f"Risk Level: {suggestion['risk_level']}")
        print(f"Safe to Template: {'✅ YES' if suggestion['is_safe'] else '⚠️  REVIEW REQUIRED'}")
        print()
        
        # Show options
        if suggestion['is_safe']:
            print("Options:")
            print("  [Y] Accept (recommended - low risk)")
            print("  [n] Reject")
            print("  [q] Quit review")
            print("  [h] Help")
            
            while True:
                choice = input("Accept this suggestion? [Y/n/q/h]: ").strip().lower()
                
                if choice in ['', 'y', 'yes']:
                    decision = 'accept'
                    break
                elif choice in ['n', 'no']:
                    decision = 'reject'
                    break
                elif choice in ['q', 'quit']:
                    decision = 'quit'
                    break
                elif choice in ['h', 'help']:
                    self._show_help()
                else:
                    print("Invalid choice. Please enter Y, n, q, or h.")
        else:
            print("Options:")
            print("  [y] Accept (use with caution)")
            print("  [N] Reject (recommended - needs review)")
            print("  [q] Quit review")
            print("  [h] Help")
            
            while True:
                choice = input("Accept this suggestion? [y/N/q/h]: ").strip().lower()
                
                if choice in ['y', 'yes']:
                    decision = 'accept'
                    break
                elif choice in ['', 'n', 'no']:
                    decision = 'reject'
                    break
                elif choice in ['q', 'quit']:
                    decision = 'quit'
                    break
                elif choice in ['h', 'help']:
                    self._show_help()
                else:
                    print("Invalid choice. Please enter y, N, q, or h.")
        
        # Record decision
        decision_record = {
            'suggestion_num': suggestion_num,
            'original': suggestion['original'],
            'suggested': suggestion['suggested'],
            'context': suggestion['context'],
            'reason': suggestion['reason'],
            'is_safe': suggestion['is_safe'],
            'risk_level': suggestion['risk_level'],
            'decision': decision,
            'reviewed_at': datetime.now().isoformat(),
            'user_notes': ''
        }
        
        # Allow user to add notes for rejected items
        if decision == 'reject':
            notes = input("Add notes about why you rejected this (optional): ").strip()
            if notes:
                decision_record['user_notes'] = notes
        
        print(f"Decision: {decision.upper()}")
        print()
        
        return decision_record
    
    def _show_help(self):
        """Show help for the review process."""
        help_text = """
📋 REVIEW HELP

This tool helps you review templating suggestions for multi-environment deployment.

DECISION GUIDANCE:
✅ ACCEPT (Y): 
   - Safe suggestions (low risk)
   - You understand the context and agree with the change
   - The suggestion makes sense for your target environment

❌ REJECT (N):
   - Unsafe suggestions (medium/high risk)
   - You're unsure about the context
   - The change doesn't make sense for your use case
   - You want to keep the original reference

🛑 QUIT (Q):
   - Exit the review process
   - You can resume later by running the review again

💡 TIPS:
- When in doubt, REJECT and review manually
- Cross-database references should usually be REJECTED
- Table/view names with environment references need careful review
- FROM/JOIN clause references are usually safe to ACCEPT
"""
        print(help_text)
    
    def _complete_review(self, schema_name, decisions):
        """Complete the review process."""
        
        # Save decisions (silently)
        self._save_decisions(decisions)
        
        # Update workflow state
        self.state_manager.update_state("REVIEW_COMPLETE", {
            'reviewed_at': datetime.now().isoformat(),
            'total_suggestions': len(decisions),
            'accepted': len([d for d in decisions if d['decision'] == 'accept']),
            'rejected': len([d for d in decisions if d['decision'] == 'reject']),
            'quit_early': any(d['decision'] == 'quit' for d in decisions)
        })
        
        # Show summary
        self._show_review_summary(decisions)
    
    def _save_decisions(self, decisions):
        """Save review decisions to file."""
        with open(self.decisions_file, 'w') as f:
            json.dump(decisions, f, indent=2)
    
    def _show_review_summary(self, decisions):
        """Show summary of review decisions."""
        print(f"\n📋 REVIEW SUMMARY")
        
        if not decisions:
            print(f"Status: No suggestions to review")
            print(f"Analysis: 0 suggestions found")
            print(f"Report: {self.temp_dir}/analysis/templating_analysis.md")
            print(f"Decisions: Saved to {self.temp_dir}/decisions.json")
            print(f"Result: Review complete")
            return
        
        accepted = [d for d in decisions if d['decision'] == 'accept']
        rejected = [d for d in decisions if d['decision'] == 'reject']
        
        print(f"Status: Review complete")
        print(f"Analysis: {len(decisions)} suggestions found")
        print(f"Report: {self.temp_dir}/analysis/templating_analysis.md")
        print(f"Decisions: Saved to {self.temp_dir}/decisions.json")
        print(f"✅ Accepted: {len(accepted)}")
        print(f"❌ Rejected: {len(rejected)}")
        
        if accepted:
            print(f"\n✅ ACCEPTED SUGGESTIONS:")
            for decision in accepted:
                print(f"  - {decision['original']} → {decision['suggested']}")
        
        if rejected:
            print(f"\n❌ REJECTED SUGGESTIONS:")
            for decision in rejected:
                print(f"  - {decision['original']} → {decision['suggested']}")
                if decision['user_notes']:
                    print(f"    Note: {decision['user_notes']}")
    
    def get_decisions(self):
        """Get the review decisions."""
        if not self.decisions_file.exists():
            return []
        
        with open(self.decisions_file, 'r') as f:
            return json.load(f)
    
    def is_review_complete(self):
        """Check if review is complete."""
        return self.state_manager.validate_state_transition("REVIEW_COMPLETE")

