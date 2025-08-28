"""
Analyzes DDL content for templating opportunities and generates analysis reports.
"""

import re
import os
from pathlib import Path
from datetime import datetime


class TemplatingAnalyzer:
    """Analyzes DDL for safe templating opportunities."""
    
    def __init__(self, temp_dir):
        self.temp_dir = Path(temp_dir)
        self.raw_dir = self.temp_dir / "raw"
        self.analysis_dir = self.temp_dir / "analysis"
        self.suggested_dir = self.temp_dir / "suggested"
    
    def analyze_schema_files(self, db_base=None):
        """Analyze all SQL files in the raw directory."""
        
        if not self.raw_dir.exists():
            raise Exception(f"Raw directory not found: {self.raw_dir}")
        
        analysis_results = {
            'schema_name': self.temp_dir.parent.name,
            'analyzed_at': datetime.now().isoformat(),
            'files_analyzed': [],
            'templating_suggestions': [],
            'cross_database_refs': [],
            'warnings': [],
            'summary': {}
        }
        
        # Analyze each SQL file
        for sql_file in self.raw_dir.glob("*.sql"):
            file_analysis = self._analyze_sql_file(sql_file, db_base)
            analysis_results['files_analyzed'].append(file_analysis)
            
            # Aggregate suggestions
            analysis_results['templating_suggestions'].extend(file_analysis['suggestions'])
            analysis_results['cross_database_refs'].extend(file_analysis['cross_db_refs'])
            analysis_results['warnings'].extend(file_analysis['warnings'])
        
        # Generate summary
        analysis_results['summary'] = self._generate_summary(analysis_results)
        
        # Write analysis report
        self._write_analysis_report(analysis_results)
        
        # Generate suggested templated versions
        self._generate_suggested_versions(analysis_results)
        
        return analysis_results
    
    def _analyze_sql_file(self, sql_file, db_base):
        """Analyze a single SQL file for templating opportunities."""
        
        with open(sql_file, 'r') as f:
            content = f.read()
        
        file_analysis = {
            'file_name': sql_file.name,
            'file_size': len(content),
            'suggestions': [],
            'cross_db_refs': [],
            'warnings': [],
            'risk_score': 0
        }
        
        # Find database references that could be templated
        if db_base:
            # Look for patterns like PLATFORM_SIT, ALTO_SIT, etc.
            db_pattern = rf'{re.escape(db_base)}_[A-Z]+'
            matches = re.finditer(db_pattern, content, re.IGNORECASE)
            
            for match in matches:
                suggestion = self._analyze_database_reference(
                    match.group(), match.start(), match.end(), content, db_base
                )
                if suggestion:
                    file_analysis['suggestions'].append(suggestion)
        
        # Find cross-database references
        cross_db_refs = self._find_cross_database_references(content)
        file_analysis['cross_db_refs'] = cross_db_refs
        
        # Find potential warnings
        warnings = self._find_potential_warnings(content)
        file_analysis['warnings'] = warnings
        
        # Calculate risk score
        file_analysis['risk_score'] = self._calculate_risk_score(file_analysis)
        
        return file_analysis
    
    def _analyze_database_reference(self, match_text, start, end, content, db_base):
        """Analyze a specific database reference for templating safety."""
        
        # Get context around the match
        context_start = max(0, start - 50)
        context_end = min(len(content), end + 50)
        context = content[context_start:context_end]
        
        # Determine if this is safe to template
        is_safe, reason = self._is_safe_to_template(match_text, context, content, start, end)
        
        suggestion = {
            'type': 'database_reference',
            'original': match_text,
            'suggested': f"{{{{DB_BASE}}}}_{{{{ENV}}}}",
            'context': context.strip(),
            'position': (start, end),
            'is_safe': is_safe,
            'reason': reason,
            'risk_level': 'LOW' if is_safe else 'MEDIUM'
        }
        
        return suggestion
    
    def _is_safe_to_template(self, match_text, context, full_content, start, end):
        """Determine if a database reference is safe to template."""
        
        # Check if this is in a FROM/JOIN clause (safe)
        from_join_patterns = [
            r'FROM\s+' + re.escape(match_text),
            r'JOIN\s+' + re.escape(match_text),
            r'UPDATE\s+' + re.escape(match_text),
            r'DELETE\s+FROM\s+' + re.escape(match_text)
        ]
        
        for pattern in from_join_patterns:
            if re.search(pattern, context, re.IGNORECASE):
                return True, "Database reference in FROM/JOIN clause - safe to template"
        
        # Check if this is in a table/column name (dangerous)
        table_column_patterns = [
            r'CREATE\s+TABLE\s+' + re.escape(match_text),
            r'CREATE\s+VIEW\s+' + re.escape(match_text),
            r'ALTER\s+TABLE\s+' + re.escape(match_text),
            r'DROP\s+TABLE\s+' + re.escape(match_text)
        ]
        
        for pattern in table_column_patterns:
            if re.search(pattern, context, re.IGNORECASE):
                return False, "Database reference in table/view name - do not template"
        
        # Check if this is in a string literal (dangerous)
        if self._is_in_string_literal(full_content, start, end):
            return False, "Database reference in string literal - do not template"
        
        # Default to unsafe if we're not sure
        return False, "Unclear context - review manually"
    
    def _is_in_string_literal(self, content, start, end):
        """Check if position is inside a string literal."""
        # Simple check for quotes around the position
        before = content[:start]
        after = content[end:]
        
        # Count single and double quotes before the position
        single_quotes_before = before.count("'") - before.count("\\'")
        double_quotes_before = before.count('"') - before.count('\\"')
        
        # Count single and double quotes after the position
        single_quotes_after = after.count("'") - after.count("\\'")
        double_quotes_after = after.count('"') - after.count('\\"')
        
        # If odd number of quotes before and after, we're inside a string
        return (single_quotes_before % 2 == 1) or (double_quotes_before % 2 == 1)
    
    def _find_cross_database_references(self, content):
        """Find references to other databases."""
        # Look for patterns like OTHER_DB.schema.table
        cross_db_pattern = r'([A-Z_]+)\.([A-Z_]+)\.([A-Z_]+)'
        matches = re.finditer(cross_db_pattern, content)
        
        cross_db_refs = []
        for match in matches:
            cross_db_refs.append({
                'database': match.group(1),
                'schema': match.group(2),
                'table': match.group(3),
                'full_reference': match.group(0),
                'context': self._get_context_around_match(content, match.start(), match.end())
            })
        
        return cross_db_refs
    
    def _find_potential_warnings(self, content):
        """Find potential issues in the DDL."""
        warnings = []
        
        # Check for hardcoded environment references
        env_patterns = ['SIT', 'DEV', 'QA', 'UAT', 'PROD']
        for env in env_patterns:
            if env in content:
                warnings.append({
                    'type': 'hardcoded_environment',
                    'value': env,
                    'message': f"Hardcoded environment reference: {env}",
                    'risk_level': 'MEDIUM'
                })
        
        # Check for potential SQL injection patterns
        sql_injection_patterns = [
            r'EXEC\s*\(',
            r'EXECUTE\s*\(',
            r'EXECUTE\s+IMMEDIATE'
        ]
        
        for pattern in sql_injection_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                warnings.append({
                    'type': 'dynamic_sql',
                    'pattern': pattern,
                    'message': f"Dynamic SQL execution detected: {pattern}",
                    'risk_level': 'HIGH'
                })
        
        return warnings
    
    def _get_context_around_match(self, content, start, end, context_size=50):
        """Get context around a match position."""
        context_start = max(0, start - context_size)
        context_end = min(len(content), end + context_size)
        return content[context_start:context_end].strip()
    
    def _calculate_risk_score(self, file_analysis):
        """Calculate overall risk score for a file."""
        risk_score = 0
        
        # High risk warnings
        high_risk_warnings = [w for w in file_analysis['warnings'] if w['risk_level'] == 'HIGH']
        risk_score += len(high_risk_warnings) * 10
        
        # Medium risk warnings
        medium_risk_warnings = [w for w in file_analysis['warnings'] if w['risk_level'] == 'MEDIUM']
        risk_score += len(medium_risk_warnings) * 5
        
        # Unsafe templating suggestions
        unsafe_suggestions = [s for s in file_analysis['suggestions'] if not s['is_safe']]
        risk_score += len(unsafe_suggestions) * 3
        
        return risk_score
    
    def _generate_summary(self, analysis_results):
        """Generate summary statistics."""
        total_suggestions = len(analysis_results['templating_suggestions'])
        safe_suggestions = len([s for s in analysis_results['templating_suggestions'] if s['is_safe']])
        unsafe_suggestions = total_suggestions - safe_suggestions
        
        total_warnings = len(analysis_results['warnings'])
        high_risk_warnings = len([w for w in analysis_results['warnings'] if w['risk_level'] == 'HIGH'])
        
        return {
            'total_files': len(analysis_results['files_analyzed']),
            'total_suggestions': total_suggestions,
            'safe_suggestions': safe_suggestions,
            'unsafe_suggestions': unsafe_suggestions,
            'total_warnings': total_warnings,
            'high_risk_warnings': high_risk_warnings,
            'overall_risk': 'LOW' if total_warnings == 0 else 'MEDIUM' if high_risk_warnings == 0 else 'HIGH'
        }
    
    def _write_analysis_report(self, analysis_results):
        """Write the analysis report to file."""
        self.analysis_dir.mkdir(parents=True, exist_ok=True)
        
        report_file = self.analysis_dir / "templating_analysis.md"
        
        with open(report_file, 'w') as f:
            f.write(self._generate_markdown_report(analysis_results))
    
    def _generate_markdown_report(self, analysis_results):
        """Generate markdown format analysis report."""
        
        report = f"""# Templating Analysis Report

## Schema: {analysis_results['schema_name']}
**Analyzed at:** {analysis_results['analyzed_at']}

## Summary
- **Total Files:** {analysis_results['summary']['total_files']}
- **Total Suggestions:** {analysis_results['summary']['total_suggestions']}
- **Safe to Template:** {analysis_results['summary']['safe_suggestions']}
- **Needs Review:** {analysis_results['summary']['unsafe_suggestions']}
- **Total Warnings:** {analysis_results['summary']['total_warnings']}
- **Overall Risk:** {analysis_results['summary']['overall_risk']}

## Templating Suggestions

"""
        
        # Group suggestions by safety
        safe_suggestions = [s for s in analysis_results['templating_suggestions'] if s['is_safe']]
        unsafe_suggestions = [s for s in analysis_results['templating_suggestions'] if not s['is_safe']]
        
        if safe_suggestions:
            report += "### ✅ Safe to Template (High Confidence)\n\n"
            for i, suggestion in enumerate(safe_suggestions, 1):
                report += f"{i}. **{suggestion['original']}** → **{suggestion['suggested']}**\n"
                report += f"   - Context: `{suggestion['context']}`\n"
                report += f"   - Reason: {suggestion['reason']}\n\n"
        
        if unsafe_suggestions:
            report += "### ⚠️ Needs Manual Review (Medium Confidence)\n\n"
            for i, suggestion in enumerate(unsafe_suggestions, 1):
                report += f"{i}. **{suggestion['original']}** → **{suggestion['suggested']}**\n"
                report += f"   - Context: `{suggestion['context']}`\n"
                report += f"   - Reason: {suggestion['reason']}\n"
                report += f"   - Risk: {suggestion['risk_level']}\n\n"
        
        # Cross-database references
        if analysis_results['cross_database_refs']:
            report += "## 🔗 Cross-Database Dependencies\n\n"
            report += "**These references should NOT be templated:**\n\n"
            for ref in analysis_results['cross_database_refs']:
                report += f"- **{ref['full_reference']}**\n"
                report += f"  - Database: {ref['database']}\n"
                report += f"  - Schema: {ref['schema']}\n"
                report += f"  - Table: {ref['table']}\n"
                report += f"  - Context: `{ref['context']}`\n\n"
        
        # Warnings
        if analysis_results['warnings']:
            report += "## ⚠️ Warnings and Issues\n\n"
            for warning in analysis_results['warnings']:
                report += f"- **{warning['type'].replace('_', ' ').title()}**\n"
                report += f"  - {warning['message']}\n"
                report += f"  - Risk Level: {warning['risk_level']}\n\n"
        
        # Recommendations
        report += "## 💡 Recommendations\n\n"
        report += "1. **Start with raw DDL** - Use the extracted files as-is initially\n"
        report += "2. **Apply only 'Safe to Template' changes** - These are low-risk\n"
        report += "3. **Manually review 'Needs Review' items** - Understand the context\n"
        report += "4. **Never template cross-database references** - These must remain unchanged\n"
        report += "5. **Test in target environment** - Verify changes work as expected\n"
        report += "6. **Document your decisions** - Keep track of what was templated and why\n\n"
        
        report += "## 🚀 Next Steps\n\n"
        report += "1. Review this analysis report\n"
        report += "2. Run interactive review: `python scripts/review_templating.py --schema {analysis_results['schema_name']}`\n"
        report += "3. Make templating decisions based on the analysis\n"
        report += "4. Generate final version: `python scripts/generate_final.py --schema {analysis_results['schema_name']}`\n"
        
        return report
    
    def _generate_suggested_versions(self, analysis_results):
        """Generate suggested templated versions of the files."""
        self.suggested_dir.mkdir(parents=True, exist_ok=True)
        
        for file_analysis in analysis_results['files_analyzed']:
            if not file_analysis['suggestions']:
                continue
            
            # Load original file
            original_file = self.raw_dir / file_analysis['file_name']
            with open(original_file, 'r') as f:
                content = f.read()
            
            # Apply safe suggestions only
            suggested_content = content
            applied_suggestions = []
            
            for suggestion in file_analysis['suggestions']:
                if suggestion['is_safe']:
                    suggested_content = suggested_content.replace(
                        suggestion['original'], 
                        suggestion['suggested']
                    )
                    applied_suggestions.append(suggestion)
            
            # Write suggested version
            suggested_file = self.suggested_dir / file_analysis['file_name']
            with open(suggested_file, 'w') as f:
                f.write(f"-- SUGGESTED TEMPLATING (REVIEW CAREFULLY):\n")
                f.write(f"-- This file contains suggested changes for multi-environment deployment\n")
                f.write(f"-- ⚠️  MANUAL REVIEW REQUIRED before using\n")
                f.write(f"-- Applied {len(applied_suggestions)} safe suggestions\n\n")
                f.write(suggested_content)
            
            print(f"💡 Suggested version generated: {suggested_file}")
            print(f"   Applied {len(applied_suggestions)} safe suggestions")
