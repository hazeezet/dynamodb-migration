# DynamoDB Table Migration Tool

A simple tool for migrating data from one DynamoDB table to another with state management and rollback capabilities.

## Features

- **Table-to-Table Migration**: Migrate data between DynamoDB tables
- **State Management**: Resume interrupted migrations
- **Rollback Support**: Undo migrations if needed
- **Batch Processing**: Efficient handling of large datasets
- **Interactive CLI**: Easy-to-use command-line interface

## Prerequisites

- Python 3.7+
- AWS CLI configured
- Required packages: `boto3`

## Quick Start

1. **Run the migration tool**:
```bash
python index.py
```

2. **Choose your action**:
   - Start new migration (table to table)
   - Resume existing migration
   - Undo previous migration

3. **For new migration, provide**:
   - Source DynamoDB table name
   - Target DynamoDB table name
   - Column mappings (if needed)

## Usage Examples

### Basic Migration
```bash
python index.py
# Select: Start new migration
# Enter source table: users-old
# Enter target table: users-new
# Configure column mappings if needed
```

### Resume Migration
```bash
python index.py
# Select: Manage existing migrations
# Choose migration to resume
```

### Undo Migration
```bash
python index.py
# Select: Manage existing migrations
# Choose migration to undo
```

## Configuration

Default settings in `src/config.py`:
- Batch size: 25 items
- Max retries: 3
- DynamoDB capacity: 5 read/write units

## State Management

The tool tracks migration progress and allows you to:
- Resume interrupted migrations
- View migration status
- Rollback completed migrations
- Delete migration states

## Troubleshooting

**Common Issues:**
- AWS credentials: Run `aws configure`
- Permissions: Ensure DynamoDB access
- Table not found: Verify table names

**Debug mode:**
```bash
export MIGRATION_LOG_LEVEL=DEBUG
python index.py
```

**Check logs:**
```bash
tail -f logs/migration_*.log
```

---

Simple, reliable DynamoDB table migration. 🚀