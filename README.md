# DynamoDB Table Migration Tool

A powerful tool for migrating data from one DynamoDB table to another with advanced template transformations, state management, and rollback capabilities.

## Features

- **Table-to-Table Migration**: Migrate data between DynamoDB tables
- **Advanced Template System**: Transform data during migration with built-in functions
- **State Management**: Resume interrupted migrations
- **Rollback Support**: Undo migrations if needed
- **Batch Processing**: Efficient handling of large datasets
- **Interactive CLI**: Easy-to-use command-line interface

## Prerequisites

- Python 3.7+
- AWS CLI configured
- Required packages: `boto3`

```bash
pip install boto3
```

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
   - Column mappings with optional transformations

## Column Mapping & Transformations

The tool supports powerful template-based column mapping with transformations:

### Basic Mapping
```json
{
  "target_column": "{source_column}"
}
```

### String Transformations
```json
{
  "name_upper": "{name upper}",
  "email_lower": "{email lower}",
  "title_case": "{description title}",
  "clean_text": "{text strip}",
  "short_desc": "{description substring 0 50}",
  "replaced_text": "{text replace old new}",
  "padded_id": "{id pad_left 10 0}"
}
```

### Number Transformations
```json
{
  "price_with_tax": "{price multiply 1.1}",
  "age_next_year": "{age add 1}",
  "discounted_price": "{original_price subtract 10}",
  "half_value": "{value divide 2}",
  "rounded_price": "{price round_to 2}",
  "absolute_value": "{number abs_value}",
  "squared": "{value power 2}"
}
```

### Available String Operations
- `upper` - Convert to uppercase
- `lower` - Convert to lowercase  
- `title` - Convert to title case
- `strip` - Remove whitespace
- `replace old new` - Replace substring
- `split ,` - Split into array
- `substring 0 10` - Extract substring
- `pad_left 10 0` - Pad on left
- `pad_right 10 0` - Pad on right

### Available Number Operations
- `add 5` - Add number
- `subtract 3` - Subtract number
- `multiply 2` - Multiply by number
- `divide 4` - Divide by number
- `round_to 2` - Round to decimals
- `abs_value` - Absolute value
- `power 2` - Raise to power
- `sqrt` - Square root
- `floor` - Floor value
- `ceil` - Ceiling value
- `mod 3` - Modulo operation

## Usage Examples

### Basic Migration
```bash
python index.py
# Select: Start new migration
# Enter source table: users-old
# Enter target table: users-new
# Configure column mappings
```

### Real-world Example
Based on your migration state, here's how transformations work:

```json
{
  "mainId": "MIGRATION#2025",
  "sortId": "METADATA#20250824_135536#HELLO#{id}",
  "id": "{id upper}",
  "email_clean": "{email lower}",
  "full_name": "{firstName} {lastName}",
  "registration_year": "{year add 0}",
  "tuition_with_fee": "{tuitionFee add 20}",
  "grade_padded": "{grade pad_left 2 0}"
}
```

## Testing Transformations

Test the transformation system:

```bash
python tests/transformations.py
```

This will show you examples of all available transformations.

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

**Check logs:**
```bash
tail -f logs/migration_*.log
```

---

Simple, reliable DynamoDB table migration. 🚀