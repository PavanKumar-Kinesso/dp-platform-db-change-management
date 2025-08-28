# Snowflake Database Change Management

A comprehensive database change management system for Snowflake using [schemachange](https://github.com/Snowflake-Labs/schemachange). This repository provides tools for version-controlled database schema management, automated deployments, and schema comparisons.

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Configuration](#configuration)
- [Usage](#usage)
- [CI/CD Workflows](#cicd-workflows)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

## Overview

This project implements a GitOps approach to Snowflake database change management, ensuring:
- Version control for all database objects
- Automated validation and deployment
- Environment-specific configurations
- Comprehensive change tracking
- Schema comparison capabilities

## Features

- **Workflow-Based Schema Management**: Safe, interactive workflow for schema extraction and templating
- **Smart Templating Analysis**: Intelligent analysis of environment-specific references with human review
- **Schema Extraction**: Export existing Snowflake schemas to version-controlled SQL files
- **Schema Comparison**: Compare current database state with proposed changes
- **Automated Deployment**: Deploy changes using schemachange with full change tracking
- **Environment Management**: Support for multiple environments (SIT, QA, UAT, PROD)
- **CI/CD Integration**: GitHub Actions workflows for validation and deployment
- **Role-Based Access Control**: Template-based grant management
- **Schema Organization**: Modular schema management with separate folders

## Project Structure

```
.
├── scripts/                        # Python automation scripts
│   ├── apply_schema.py            # Deploy schemas using schemachange
│   ├── compare_schema.py          # Compare schemas with target
│   ├── export_schema.py           # Workflow-based schema extraction and management
│   └── utils/                     # Utility modules
│       ├── __init__.py
│       ├── connection.py          # Connection management
│       ├── extraction.py          # Schema extraction utilities
│       ├── schema_config.py       # Schema configuration
│       └── workflow/              # Workflow management system
│           ├── __init__.py
│           ├── state_manager.py   # Workflow state management
│           ├── safe_extractor.py  # Safe schema extraction
│           ├── templating_analyzer.py # Templating analysis
│           ├── interactive_reviewer.py # Interactive review
│           ├── final_generator.py # Final version generation
│           ├── commit_manager.py  # Commit management
│           └── workflow_utils.py  # Workflow utilities
├── schemas/                       # Schema definitions
│   ├── Monitoring/                # Monitoring schema
│   ├── ReportingApps/             # Reporting schema
│   └── DATA_AMS/                  # Data AMS schema
├── .github/workflows/             # CI/CD pipelines
│   ├── validate-schemas.yml       # PR validation
│   ├── deploy-schemas.yml         # Deployment automation
│   └── extract-schemas.yml        # Schema extraction
├── connections.toml               # Connection configuration
├── schemachange-config.yml        # Schemachange configuration
├── requirements.txt               # Python dependencies
└── README.md                      # This file
```

## Getting Started

### Prerequisites

- Python 3.8+
- Snowflake account with appropriate permissions
- Git

### Installation

1. Clone the repository:
```bash
git clone https://github.com/your-org/dp-platform-db-change-management.git
cd dp-platform-db-change-management
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure connections:
```bash
# Edit connections.toml with your Snowflake credentials
vim connections.toml
```

## Configuration

### connections.toml

The connections.toml file supports schema-specific connections:

```toml
# Source connections for different schemas
[SRC_DATA_AMS]
account = "KINESSO-KINESSO"
user = "YOUR_USERNAME"
authenticator = "externalbrowser"
role = "ACCOUNTADMIN"
warehouse = "GR_DATA_TEAM_PROD"
database = "ALTO_{{ENV}}"
schema = "DATA_AMS"

[SRC_REPORTINGAPPS]
account = "KINESSO-KINESSO_REPORTING"
user = "YOUR_USERNAME"
authenticator = "externalbrowser"
role = "DATAADMIN"
warehouse = "WH_APP_UPLOAD_XS_{{ENV}}"
database = "PLATFORM_{{ENV}}"
schema = "REPORTINGAPPS"
```

### Environment Variables

Set these environment variables for CI/CD:

```bash
export SNOWFLAKE_ACCOUNT="your-account.snowflakecomputing.com"
export SNOWFLAKE_USER="your-username"
export SNOWFLAKE_PASSWORD="your-password"
export SNOWFLAKE_ROLE="your-role"
export SNOWFLAKE_WAREHOUSE="your-warehouse"
```

## Usage

### Workflow-Based Schema Management

The new workflow system provides a safe, interactive approach to schema extraction and templating:

#### **Step 1: Extract Schema**
```bash
python scripts/export_schema.py --workflow extract --schema ReportingApps --src SRC_REPORTINGAPPS --database PLATFORM_SIT
```
This extracts the schema to a temporary directory and analyzes potential templating opportunities.

#### **Step 2: Review Templating Suggestions**
```bash
python scripts/export_schema.py --workflow review --schema ReportingApps
```
Interactive review of templating suggestions with accept/reject decisions.

#### **Step 3: Generate Final Version**
```bash
python scripts/export_schema.py --workflow generate --schema ReportingApps
```
Creates the final version based on your review decisions.

#### **Step 4: Commit to Main Directory**
```bash
python scripts/export_schema.py --workflow commit --schema ReportingApps
```
Moves approved files to the main schema directory.

#### **Additional Workflow Commands**
```bash
# Check workflow status
python scripts/export_schema.py --workflow status --schema ReportingApps

# Clean temp files and start over
python scripts/export_schema.py --workflow clean --schema ReportingApps
```

### Legacy Schema Operations

For backward compatibility, you can still use the direct extraction method:

```bash
python scripts/export_schema.py \
  --database PLATFORM_SIT \
  --schemas MONITORING REPORTINGAPPS DATA_AMS \
  --db-prefix TEST
```

### 2. Compare Schemas

Compare local schema files with target environment:

```bash
python scripts/compare_schema.py \
  --target-env SIT \
  --schema MONITORING \
  --db-prefix TEST
```

### 3. Deploy Schemas

Deploy schemas to target environment:

```bash
# Dry run (validation only)
python scripts/apply_schema.py \
  --target SIT \
  --schema-folder Monitoring \
  --db-prefix TEST \
  --dry-run

# Actual deployment
python scripts/apply_schema.py \
  --target SIT \
  --schema-folder Monitoring \
  --db-prefix TEST
```

### 4. Deploy All Schemas

Deploy all schemas in the repository:

```bash
for schema in Monitoring ReportingApps DATA_AMS; do
  python scripts/apply_schema.py \
    --target SIT \
    --schema-folder $schema \
    --db-prefix TEST
done
```

## CI/CD Workflows

### Pull Request Validation

Automatically validates schema changes on pull requests:
- Syntax validation
- Schema comparison
- Dry-run deployment test

### Automated Deployment

Deploys schemas when changes are merged to main:
- Supports manual triggers with environment selection
- Deploys specific schemas or all schemas
- Full change tracking with schemachange

### Schema Extraction

Manual workflow to extract schemas and create a pull request:
- Extract from any source database
- Automatically creates PR with changes
- Useful for migrating existing schemas

## Best Practices

### 1. Workflow-Based Development

- **Use the workflow system** for all new schema extractions
- **Review templating suggestions** before accepting them
- **Clean temp files** when starting over
- **Follow the workflow sequence**: extract → review → generate → commit

### 2. Naming Conventions

- **SQL Files**: Use schemachange naming convention `V<version>__<description>.sql`
- **Schemas**: Use UPPERCASE names (e.g., `MONITORING`, `REPORTINGAPPS`, `DATA_AMS`)
- **Roles**: Use environment-prefixed roles (e.g., `SIT_DATA_ENGINEER`)

### 3. Version Control

- Always use incremental version numbers
- Never modify existing migration files
- Create new files for changes
- Use descriptive file names

### 3. Environment Management

- Use template variables for environment-specific values:
  - `{{ ENV }}` - Environment name (DEV, SIT, etc.)
  - `{{ DB_PREFIX }}` - Database prefix (TEST, PROD, etc.)
  - `{{ DB_BASE }}` - Base database name (PLATFORM, ALTO, etc.)

### 4. Testing

- Always run with `--dry-run` first
- Use comparison tool before deployment
- Test in lower environments first
- Review change history table after deployment

### 5. Security

- Never commit credentials to version control
- Use environment variables or secrets management
- Implement least-privilege access
- Regularly rotate credentials

## Troubleshooting

### Common Issues

1. **Connection Errors**
   - Verify credentials in `connections.toml`
   - Check network connectivity
   - Ensure role has necessary permissions

2. **Schema Not Found**
   - Verify schema exists in source database
   - Check case sensitivity (use UPPERCASE)
   - Ensure proper database context

3. **Permission Denied**
   - Verify role has CREATE SCHEMA privileges
   - Check database-level grants
   - Ensure warehouse is running

4. **Schemachange Errors**
   - Check SQL syntax in migration files
   - Verify version numbers are sequential
   - Review schemachange logs with `--verbose`

### Debug Mode

Enable verbose logging:
```bash
python scripts/apply_schema.py \
  --target DEV \
  --schema-folder Monitoring \
  --verbose
```

### Change History

View deployment history:
```sql
SELECT * FROM SCHEMACHANGE.CHANGE_HISTORY
ORDER BY INSTALLED_ON DESC;
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run validation tests
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
- Create an issue in this repository
- Contact the platform team
- Check the [schemachange documentation](https://github.com/Snowflake-Labs/schemachange)