#!/usr/bin/env python3
"""
Workflow-based Snowflake schema extraction and management.
Supports safe extraction, templating analysis, interactive review, and final generation.
"""

import argparse
import sys
import os

# Import workflow modules
try:
    # Add parent directory to path to find utils module
    import sys
    import os
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)
    
    from scripts.utils.workflow import (
        SafeSchemaExtractor, 
        TemplatingAnalyzer,
        InteractiveReviewer,
        FinalVersionGenerator,
        CommitManager,
        WorkflowUtils
    )
    WORKFLOW_AVAILABLE = True
    
    # Only define workflow functions when imports are successful
    def run_workflow_step(workflow_step, schema_name, **kwargs):
        """Run a specific workflow step."""
        temp_dir = f"schemas/{schema_name}/temp"
        
        if workflow_step == "extract":
            return _run_extract_step(schema_name, temp_dir, **kwargs)
        elif workflow_step == "review":
            return _run_review_step(schema_name, temp_dir)
        elif workflow_step == "generate":
            return _run_generate_step(schema_name, temp_dir)
        elif workflow_step == "commit":
            return _run_commit_step(schema_name, temp_dir)
        elif workflow_step == "status":
            return _run_status_step(schema_name, temp_dir)
        elif workflow_step == "clean":
            return _run_clean_step(schema_name, temp_dir)
        else:
            print(f"❌ ERROR: Unknown workflow step: {workflow_step}")
            return False


    def _run_extract_step(schema_name, temp_dir, src, database, db_base=None, db_prefix=None):
        """Run the extract workflow step."""
        
        # Load connection config
        from scripts.utils.connection import load_connection_config
        config = load_connection_config(is_ci=False, connection_name=src)
        
        # Create safe extractor
        extractor = SafeSchemaExtractor(schema_name, database, config)
        
        # Extract to temp directory
        success = extractor.extract_safe(db_base=db_base, db_prefix=db_prefix)
        if not success:
            return False
        
        print(f"\n🚀 NEXT STEP")
        print(f"Command: python scripts/export_schema.py --workflow review --schema {schema_name}")
        print(f"Purpose: Review templating suggestions and make decisions")
        
        return True


    def _run_review_step(schema_name, temp_dir):
        """Run the review workflow step."""
        
        # Create interactive reviewer
        reviewer = InteractiveReviewer(temp_dir)
        
        # Start review process
        success = reviewer.start_review(schema_name)
        if not success:
            return False
        
        return True


    def _run_generate_step(schema_name, temp_dir):
        """Run the generate workflow step."""
        
        # Create final generator
        generator = FinalVersionGenerator(temp_dir)
        
        # Generate final version
        success = generator.generate_final_version(schema_name)
        if not success:
            return False
        
        return True


    def _run_commit_step(schema_name, temp_dir):
        """Run the commit workflow step."""
        
        # Create commit manager
        committer = CommitManager(temp_dir)
        
        # Commit schema
        success = committer.commit_schema(schema_name)
        if not success:
            return False
        
        return True


    def _run_status_step(schema_name, temp_dir):
        """Run the status workflow step."""
        
        print(f"📋 WORKFLOW STATUS: {schema_name} schema")
        
        if not os.path.exists(temp_dir):
            print(f"❌ No workflow found for {schema_name}")
            print(f"💡 Start workflow: python scripts/export_schema.py --workflow extract --schema {schema_name}")
            
            # Show status summary
            print(f"\n📋 STATUS SUMMARY")
            print(f"Status: No active workflow")
            print(f"Schema: {schema_name}")
            print(f"State: No temporary files or workflow state found")
            print(f"Result: Ready to start new workflow")
            
            # Show next step
            print(f"\n🚀 NEXT STEP")
            print(f"Command: python scripts/export_schema.py --workflow extract --schema {schema_name}")
            print(f"Purpose: Start new schema extraction workflow")
            
            return True
        
        # Get current state and show concise status
        committer = CommitManager(temp_dir)
        current_state = committer.state_manager.get_current_state()
        
        # Show status summary
        print(f"\n📋 STATUS SUMMARY")
        print(f"Status: Workflow active")
        print(f"Schema: {schema_name}")
        print(f"Current State: {current_state}")
        
        # Show next step based on current state
        if current_state == "EXTRACTION_COMPLETE":
            print(f"Next Step: Review templating suggestions")
        elif current_state == "REVIEW_COMPLETE":
            print(f"Next Step: Generate final version")
        elif current_state == "FINAL_GENERATED":
            print(f"Next Step: Commit to main directory")
        elif current_state == "COMMITTED":
            print(f"Next Step: Workflow complete")
        
        # Show next step command
        if current_state == "EXTRACTION_COMPLETE":
            print(f"\n🚀 NEXT STEP")
            print(f"Command: python scripts/export_schema.py --workflow review --schema {schema_name}")
            print(f"Purpose: Review templating suggestions")
        elif current_state == "REVIEW_COMPLETE":
            print(f"\n🚀 NEXT STEP")
            print(f"Command: python scripts/export_schema.py --workflow generate --schema {schema_name}")
            print(f"Purpose: Generate final version based on review decisions")
        elif current_state == "FINAL_GENERATED":
            print(f"\n🚀 NEXT STEP")
            print(f"Command: python scripts/export_schema.py --workflow commit --schema {schema_name}")
            print(f"Purpose: Commit final version to main schema directory")
        elif current_state == "COMMITTED":
            print(f"\n🎉 WORKFLOW COMPLETE")
            print(f"Schema '{schema_name}' has been successfully processed!")
            print(f"💡 Ready for deployment using schemachange")
        
        return True


    def _run_clean_step(schema_name, temp_dir):
        """Run the clean workflow step."""
        
        # Clean temp files
        WorkflowUtils.clean_temp_files(schema_name)
        
        # Show clean summary
        print(f"\n📋 CLEAN SUMMARY")
        print(f"Status: Temporary files cleaned")
        print(f"Schema: {schema_name}")
        print(f"Action: All temp files and workflow state removed")
        print(f"Result: Ready for fresh workflow start")
        
        # Show next step
        print(f"\n🚀 NEXT STEP")
        print(f"Command: python scripts/export_schema.py --workflow extract --schema {schema_name}")
        print(f"Purpose: Start fresh schema extraction workflow")
        
        return True

except ImportError as e:
    WORKFLOW_AVAILABLE = False
    print(f"⚠️  Workflow import warning: {e}")
    
    # Define stub functions when imports fail
    def run_workflow_step(workflow_step, schema_name, **kwargs):
        """Stub function when workflow modules are not available."""
        print("❌ ERROR: Workflow modules not available")
        print("💡 Make sure utils/workflow modules are properly installed")
        return False


def main():
    """Main workflow orchestrator for Snowflake schema management."""
    ap = argparse.ArgumentParser(
        description="Workflow-based Snowflake schema extraction and management",
        epilog="""
Examples:
  # Start workflow: Extract schema
  python scripts/export_schema.py --workflow extract --schema ReportingApps --src SRC_REPORTINGAPPS --database PLATFORM_SIT
  
  # Continue workflow: Review templating suggestions
  python scripts/export_schema.py --workflow review --schema ReportingApps
  
  # Continue workflow: Generate final version
  python scripts/export_schema.py --workflow generate --schema ReportingApps
  
  # Complete workflow: Commit to main directory
  python scripts/export_schema.py --workflow commit --schema ReportingApps
  
  # Check workflow status
  python scripts/export_schema.py --workflow status --schema ReportingApps
  
  # Clean temp files and start over
  python scripts/export_schema.py --workflow clean --schema ReportingApps
        """
    )
    
    # Workflow arguments
    ap.add_argument("--workflow", 
                   choices=["extract", "review", "generate", "commit", "status", "clean"], 
                   required=True,
                   help="Workflow step to execute")
    
    # Schema argument (required for all workflow steps)
    ap.add_argument("--schema", 
                   required=True,
                   help="Schema name for workflow operations")
    
    # Connection arguments (required only for extract step)
    ap.add_argument("--src", 
                   help="Source connection name (required for extract step)")
    ap.add_argument("--database", 
                   help="Source database name (required for extract step)")
    ap.add_argument("--db-base", 
                   help="Database base type (ALTO, PLATFORM, etc.)")
    ap.add_argument("--db-prefix", 
                   help="Database prefix for target environment")
    
    args = ap.parse_args()
    
    # Validate workflow prerequisites
    if args.workflow == "extract":
        if not args.src or not args.database:
            print("❌ ERROR: --src and --database are required for extract workflow step")
            print("💡 Example: python scripts/export_schema.py --workflow extract --schema ReportingApps --src SRC_REPORTINGAPPS --database PLATFORM_SIT")
            return 1
    
    # Run workflow step
    try:
        if args.workflow == "extract":
            success = run_workflow_step("extract", args.schema, 
                                     src=args.src, 
                                     database=args.database,
                                     db_base=args.db_base,
                                     db_prefix=args.db_prefix)
        else:
            success = run_workflow_step(args.workflow, args.schema)
        
        return 0 if success else 1
        
    except Exception as e:
        print(f"❌ Workflow error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
