import boto3
import re
import sys
import json
import logging
import decimal
import numbers
from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError
from datetime import datetime

# Constants
STATE_FILE = 'migration_state.json'
UNDO_FILE = 'undo_state.json'

# Configure logging
logging.basicConfig(
    filename='migration.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

def load_state():
    try:
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {"migrations": []}
    except json.JSONDecodeError:
        logging.error("State file is corrupted.")
        print("State file is corrupted. Please fix or delete 'migration_state.json'.")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Error loading state: {e}")
        print(f"Error loading state: {e}")
        sys.exit(1)

def save_state(state):
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f, indent=4)
    except Exception as e:
        logging.error(f"Error saving state: {e}")
        print(f"Error saving state: {e}")
        sys.exit(1)

def load_undo_state():
    try:
        with open(UNDO_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {"undo_migrations": {}}
    except json.JSONDecodeError:
        logging.error("Undo file is corrupted.")
        print("Undo file is corrupted. Please fix or delete 'undo_state.json'.")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Error loading undo state: {e}")
        print(f"Error loading undo state: {e}")
        sys.exit(1)

def save_undo_state(undo_state):
    try:
        with open(UNDO_FILE, 'w') as f:
            json.dump(undo_state, f, indent=4)
    except Exception as e:
        logging.error(f"Error saving undo state: {e}")
        print(f"Error saving undo state: {e}")
        sys.exit(1)

def list_migrations(state):
    if not state['migrations']:
        print("No existing migration jobs found.")
        return

    print("\n=== Existing Migration Jobs ===")
    for idx, mig in enumerate(state['migrations'], start=1):
        print(f"{idx}. ID: {mig['id']} | Source: {mig['source_table']} | Target: {mig['target_table']} | Status: {mig['status']}")
    print()

def select_migration(state):
    if not state['migrations']:
        return None

    list_migrations(state)
    while True:
        choice = input("Select a migration job by number (or 'new' to create a new migration, 'undo' to undo last migration): ").strip()
        if choice.lower() == 'new':
            return None
        if choice.lower() == 'undo':
            return 'undo'
        if not choice.isdigit() or not (1 <= int(choice) <= len(state['migrations'])):
            print("Invalid selection. Please try again.")
            continue
        return state['migrations'][int(choice) - 1]

def edit_migration(migration):
    print("\n--- Edit Migration Job ---")
    print("Press Enter to keep the current value.\n")

    new_source = input(f"Source Table [{migration['source_table']}]: ").strip()
    if new_source:
        migration['source_table'] = new_source

    new_target = input(f"Target Table [{migration['target_table']}]: ").strip()
    if new_target:
        migration['target_table'] = new_target

    print("\n--- Edit Column Mappings ---")
    if migration['column_mappings']:
        for target, template in migration['column_mappings'].items():
            new_template = input(f"Mapping for '{target}' [{template}]: ").strip()
            if new_template:
                migration['column_mappings'][target] = new_template
    else:
        print("No existing column mappings.")

    while True:
        add_mapping = input("Do you want to add a new column mapping? (yes/no): ").strip().lower()
        if add_mapping == 'yes':
            mapping = input("Enter column mapping (format: target_column=template): ").strip()
            if '=' not in mapping:
                print("Invalid format. Please use 'target_column=template'.")
                continue
            target_column, template = mapping.split('=', 1)
            target_column = target_column.strip()
            template = template.strip()
            if target_column and template:
                migration['column_mappings'][target_column] = template
                print(f"Added mapping: {target_column} = {template}")
            else:
                print("Invalid mapping. Both target column and template are required.")
        elif add_mapping == 'no':
            break
        else:
            print("Please enter 'yes' or 'no'.")

    print("\nMigration job updated successfully.\n")

def get_user_input():
    try:
        source_table_name = input("Enter the source table name: ").strip()
        target_table_name = input("Enter the target table name: ").strip()

        if not source_table_name or not target_table_name:
            raise ValueError("Table names cannot be empty.")

        print("\n--- Column Mappings ---")
        
        # Add passthrough option
        mapping_type = input("Do you want to: \n"
                             "1. Copy all attributes directly (passthrough mode)\n"
                             "2. Define specific column mappings\n"
                             "Enter choice (1/2): ").strip()
        
        column_mappings = {}
        
        if mapping_type == "1":
            # Passthrough mode - use a special marker in the mappings
            column_mappings["__PASSTHROUGH__"] = "true"
            print("\nPassthrough mode selected. All source attributes will be copied to the target table.")
            
            # Allow for excluding certain columns
            exclude_columns = input("\nOptionally, enter column names to exclude (comma-separated, or leave empty): ").strip()
            if exclude_columns:
                exclude_list = [col.strip() for col in exclude_columns.split(",") if col.strip()]
                column_mappings["__EXCLUDE__"] = exclude_list
                print(f"The following columns will be excluded: {', '.join(exclude_list)}")
                
        elif mapping_type == "2":
            print("\nDefine your column mappings using the format:")
            print("target_column=prefix{source_column1}middle{source_column2}suffix")
            print("Use '{}' to insert source column values. You can add prefixes and suffixes as needed.")
            print("Enter 'done' when finished.\n")

            while True:
                mapping = input("Enter column mapping (or 'done' to finish): ").strip()
                if mapping.lower() == 'done':
                    break
                if '=' not in mapping:
                    print("Invalid format. Please use 'target_column=template'.")
                    continue
                target_column, template = mapping.split('=', 1)
                target_column = target_column.strip()
                template = template.strip()
                if not target_column or not template:
                    print("Invalid mapping. Target column and template cannot be empty.")
                    continue
                column_mappings[target_column] = template
        else:
            print("Invalid choice. Defaulting to specific column mappings.")
            # Repeat the specific mapping code here
            print("\nDefine your column mappings using the format:")
            print("target_column=prefix{source_column1}middle{source_column2}suffix")
            print("Use '{}' to insert source column values. You can add prefixes and suffixes as needed.")
            print("Enter 'done' when finished.\n")

            while True:
                mapping = input("Enter column mapping (or 'done' to finish): ").strip()
                if mapping.lower() == 'done':
                    break
                if '=' not in mapping:
                    print("Invalid format. Please use 'target_column=template'.")
                    continue
                target_column, template = mapping.split('=', 1)
                target_column = target_column.strip()
                template = template.strip()
                if not target_column or not template:
                    print("Invalid mapping. Target column and template cannot be empty.")
                    continue
                column_mappings[target_column] = template

        if not column_mappings:
            raise ValueError("You must define at least one column mapping.")

        return source_table_name, target_table_name, column_mappings

    except ValueError as ve:
        logging.error(f"Input Error: {ve}")
        print(f"Input Error: {ve}")
        sys.exit(1)

def check_and_create_target_table(source_table_name, target_table_name):
    """Check if target table exists and offer to create it with source table's settings if it doesn't."""
    try:
        dynamodb = boto3.resource('dynamodb')
        client = dynamodb.meta.client
        
        # Get source table description
        source_description = client.describe_table(TableName=source_table_name)['Table']
        
        # Ask for confirmation
        print(f"\nTarget table '{target_table_name}' does not exist.")
        confirm = input("Do you want to create it with the same settings as the source table? (yes/no): ").strip().lower()
        
        if confirm != 'yes':
            return False
            
        # Check billing mode FIRST - before working with GSIs
        billing_mode = 'PROVISIONED'
        provisioned_throughput = {
            'ReadCapacityUnits': source_description.get('ProvisionedThroughput', {}).get('ReadCapacityUnits', 5),
            'WriteCapacityUnits': source_description.get('ProvisionedThroughput', {}).get('WriteCapacityUnits', 5)
        }
        
        if source_description.get('BillingModeSummary', {}).get('BillingMode') == 'PAY_PER_REQUEST':
            billing_mode = 'PAY_PER_REQUEST'
            
        # Extract table configuration
        key_schema = source_description['KeySchema']
        
        # Collect all attribute names used in any key schema (primary + GSIs)
        key_attributes = set()
        
        # Add primary key attributes
        for key in key_schema:
            key_attributes.add(key['AttributeName'])
        
        # Prepare GSI configurations if present
        gsi_configs = []
        if 'GlobalSecondaryIndexes' in source_description:
            for gsi in source_description['GlobalSecondaryIndexes']:
                gsi_config = {
                    'IndexName': gsi['IndexName'],
                    'KeySchema': gsi['KeySchema'],
                    'Projection': gsi['Projection']
                }
                
                # Add GSI key attributes to our set
                for key in gsi['KeySchema']:
                    key_attributes.add(key['AttributeName'])
                
                # Add provisioning info for GSI ONLY if table is in PROVISIONED mode
                if billing_mode == 'PROVISIONED':
                    read_capacity = max(1, gsi.get('ProvisionedThroughput', {}).get('ReadCapacityUnits', 1))
                    write_capacity = max(1, gsi.get('ProvisionedThroughput', {}).get('WriteCapacityUnits', 1))
                    gsi_config['ProvisionedThroughput'] = {
                        'ReadCapacityUnits': read_capacity,
                        'WriteCapacityUnits': write_capacity
                    }
                
                gsi_configs.append(gsi_config)
        
        # Filter attribute definitions to include only the attributes used in keys
        attribute_definitions = []
        for attr_def in source_description['AttributeDefinitions']:
            if attr_def['AttributeName'] in key_attributes:
                attribute_definitions.append(attr_def)
        
        # Create table parameters
        create_params = {
            'TableName': target_table_name,
            'KeySchema': key_schema,
            'AttributeDefinitions': attribute_definitions,
            'BillingMode': billing_mode
        }
        
        # Add provisioned throughput if needed
        if billing_mode == 'PROVISIONED':
            create_params['ProvisionedThroughput'] = provisioned_throughput
        
        # Add GSIs if we have any
        if gsi_configs:
            create_params['GlobalSecondaryIndexes'] = gsi_configs
            print(f"Copying {len(gsi_configs)} Global Secondary Indexes from source table")

        # Create the table
        print(f"Creating target table '{target_table_name}'...")
        client.create_table(**create_params)
        
        # Wait for table to be created
        print("Waiting for table to be created (this may take a few minutes)...")
        waiter = client.get_waiter('table_exists')
        waiter.wait(TableName=target_table_name)
        print(f"Target table '{target_table_name}' created successfully.")
        return True
        
    except ClientError as e:
        logging.error(f"Error creating target table: {e}")
        print(f"Error creating target table: {e}")
        return False

def show_summary(source_table, target_table, column_mappings):
    try:
        print("\n=== Migration Summary ===")
        print(f"Source Table: {source_table}")
        print(f"Target Table: {target_table}\n")

        if column_mappings:
            print("Column Mappings:")
            for target, template in column_mappings.items():
                print(f"  {target} = {template}")

        confirm = input("\nDo you want to proceed with the migration? (yes/no): ").strip().lower()
        return confirm == 'yes'

    except Exception as e:
        logging.error(f"Summary Error: {e}")
        print(f"Summary Error: {e}")
        sys.exit(1)

def execute_batch_write(table_name, write_requests):
    dynamodb = boto3.client('dynamodb')
    try:
        response = dynamodb.batch_write_item(
            RequestItems={
                table_name: write_requests
            }
        )
        unprocessed = response.get('UnprocessedItems', {})
        if unprocessed.get(table_name):
            logging.warning(f"Unprocessed items detected. Retrying...")
            execute_batch_write(table_name, unprocessed[table_name])
    except ClientError as e:
        logging.error(f"Batch Write Error: {e}")
        print(f"Batch Write Error: {e}")
    except Exception as e:
        logging.error(f"Unexpected Batch Write Error: {e}")
        print(f"Unexpected Batch Write Error: {e}")

def get_table_key_schema(target_table, source_table):
    try:
        table_description = target_table.meta.client.describe_table(TableName=target_table.name)
        key_schema = table_description['Table']['KeySchema']
        keys = {}
        for key in key_schema:
            keys[key['KeyType']] = key['AttributeName']
        return keys
    except ClientError as e:
        if e.response['Error']['Code'] == "ResourceNotFoundException":
            if check_and_create_target_table(source_table.name, target_table.name):
                # Retry after creating the table
                return get_table_key_schema(target_table, source_table)
            else:
                logging.error("Target table doesn't exist and wasn't created")
                print("Migration cannot proceed as target table doesn't exist")
                sys.exit(1)
        else:
            logging.error(f"Error fetching key schema: {e}")
            print(f"Error fetching key schema: {e}")
            sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error fetching key schema: {e}")
        print(f"Unexpected error fetching key schema: {e}")
        sys.exit(1)

def convert_to_dynamodb_type(value):
    if isinstance(value, bool):
        return {'BOOL': value}
    elif isinstance(value, numbers.Number):
        return {'N': str(value)}
    elif isinstance(value, dict):
        return {'M': {k: convert_to_dynamodb_type(v) for k, v in value.items()}}
    elif isinstance(value, list):
        return {'L': [convert_to_dynamodb_type(elem) for elem in value]}
    elif value is None:
        return {'NULL': True}
    else:
        return {'S': value}

def migrate_data(state, migration):
    try:
        dynamodb = boto3.resource('dynamodb')
        source_table = dynamodb.Table(migration['source_table'])
        target_table = dynamodb.Table(migration['target_table'])
    except NoCredentialsError:
        logging.error("AWS credentials not found. Please configure your AWS credentials.")
        print("AWS credentials not found. Please configure your AWS credentials.")
        migration['status'] = 'error'
        save_state(state)
        sys.exit(1)
    except ClientError as e:
        logging.error(f"Failed to connect to DynamoDB: {e}")
        print(f"Failed to connect to DynamoDB: {e}")
        migration['status'] = 'error'
        save_state(state)
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        print(f"Unexpected error: {e}")
        migration['status'] = 'error'
        save_state(state)
        sys.exit(1)

    undo_state = load_undo_state()
    mig_id = migration['id']
    if mig_id not in undo_state['undo_migrations']:
        undo_state['undo_migrations'][mig_id] = {"DeleteRequest": {"Key": []}}
    undo_keys = undo_state['undo_migrations'][mig_id]['DeleteRequest']['Key']

    try:
        print("\nStarting migration...")
        key_schema = get_table_key_schema(target_table, source_table)
        migration['key_schema'] = key_schema
        save_state(state)
        paginator = dynamodb.meta.client.get_paginator('scan')
        scan_kwargs = {
            'TableName': migration['source_table']
        }
        if migration.get('last_evaluated_key'):
            scan_kwargs['ExclusiveStartKey'] = migration['last_evaluated_key']

        response_iterator = paginator.paginate(**scan_kwargs)
        total_items = migration.get('processed_items', 0)

        write_requests = []
        batch_size = 25

        for page in response_iterator:
            items = page.get('Items', [])
            for item in items:
                new_item = {}

                # Check if we're in passthrough mode
                if migration['column_mappings'].get('__PASSTHROUGH__') == 'true':
                    # Copy all attributes except those in exclude list
                    exclude_list = migration['column_mappings'].get('__EXCLUDE__', [])
                    for key, value in item.items():
                        if key not in exclude_list:
                            new_item[key] = value
                else:
                    # Original processing with specific mappings
                    for target_col, template in migration['column_mappings'].items():
                        try:
                            logging.info(f"Processing mapping: {target_col} = {template} (type: {type(template)})")
                            
                            # Check if template is a pure placeholder
                            if isinstance(template, str):
                                pure_placeholder_match = re.fullmatch(r'\{(\w+)\}', template)
                                if pure_placeholder_match:
                                    placeholder = pure_placeholder_match.group(1)
                                    value = item.get(placeholder, None)
                                    new_item[target_col] = value
                                    logging.info(f"Pure placeholder {target_col}: {value}")
                                    continue
                            
                            # Handle direct values (numbers, booleans, etc.)
                            if isinstance(template, (int, float, bool)):
                                new_item[target_col] = template
                                logging.info(f"Direct value {target_col}: {template}")
                                continue
                            
                            # Replace placeholders within the template string
                            result = apply_template(template, item)
                            new_item[target_col] = result
                            logging.info(f"Template result {target_col}: {result}")
                            
                        except Exception as e:
                            logging.error(f"Error processing column mapping {target_col}: {e}")
                            logging.error(f"Error traceback: {traceback.format_exc()}")
                            logging.error(f"Template: {template} (type: {type(template)})")
                            logging.error(f"Item: {item}")
                            raise e

                # Format the item for DynamoDB
                formatted_item = {}
                for k, v in new_item.items():
                    formatted_item[k] = convert_to_dynamodb_type(v)
                write_requests.append({
                    'PutRequest': {
                        'Item': formatted_item
                    }
                })

                # Build the key for undo operation
                key = {}
                for key_type, key_name in key_schema.items():
                    if key_name in formatted_item:
                        key[key_name] = formatted_item[key_name]
                    elif key_name in item:
                        key[key_name] = {
                            'S': str(item[key_name])
                        }
                    else:
                        key[key_name] = {'S': ''}
                
                # Append key to the DeleteRequest Key list
                undo_keys.append(key)

                if len(write_requests) == batch_size:
                    execute_batch_write(migration['target_table'], write_requests)
                    write_requests = []
                    total_items += batch_size
                    migration['processed_items'] = total_items
                    save_state(state)
                    print(f"Processed {total_items} items...")

            last_key = page.get('LastEvaluatedKey', None)
            if last_key:
                migration['last_evaluated_key'] = last_key
                save_state(state)
            else:
                migration['last_evaluated_key'] = None

        if write_requests:
            execute_batch_write(migration['target_table'], write_requests)
            total_items += len(write_requests)
            migration['processed_items'] = total_items
            migration['last_evaluated_key'] = None
            save_state(state)
            print(f"Processed {total_items} items...")

        migration['status'] = 'completed'
        migration['processed_items'] = total_items
        migration['last_evaluated_key'] = None
        save_state(state)

        save_undo_state(undo_state)

        print(f"\nMigration completed successfully. Total items migrated: {total_items}")
        logging.info(f"Migration '{mig_id}' completed successfully. Total items migrated: {total_items}")

    except EndpointConnectionError as e:
        logging.error(f"Connection Error: {e}")
        print(f"Connection Error: {e}")
        migration['status'] = 'error'
        save_state(state)
    except ClientError as e:
        logging.error(f"DynamoDB Client Error: {e}")
        print(f"DynamoDB Client Error: {e}")
        migration['status'] = 'error'
        save_state(state)
    except Exception as e:
        logging.error(f"Migration Error: {e}")
        print(f"Migration Error: {e}")
        migration['status'] = 'error'
        save_state(state)

def apply_template(template, item):
    try:
        placeholders = re.findall(r'\{(\w+)\}', template)
        for ph in placeholders:
            value = item.get(ph, None)
            if isinstance(value, (dict, list)):
                replacement = value  # Keep as dict or list
            elif value is None:
                # Replace None with 'null' for JSON compatibility
                replacement = 'null'
            elif isinstance(value, numbers.Number):
                # Preserve original data types (int, float, Decimal)
                replacement = str(value)
            else:
                # Preserve strings
                replacement = value.replace('"', '\\"')
            if isinstance(replacement, str) and replacement != 'null':
                # Ensure strings are properly quoted
                template = template.replace(f'{{{ph}}}', replacement)
            elif replacement == 'null':
                template = template.replace(f'{{{ph}}}', replacement)
            else:
                # For numbers and booleans, convert to string without quotes
                template = template.replace(f'{{{ph}}}', replacement)
        return template
    except Exception as e:
        logging.error(f"Template Processing Error: {e}")
        return ""

def undo_last_migration(state):
    undo_state = load_undo_state()
    print("\n=== Undo Migration ===")
    if not undo_state['undo_migrations']:
        print("No undo information available.")
        return

    print("\n=== Available Migrations to Undo ===")
    migration_ids = list(undo_state['undo_migrations'].keys())
    for idx, mig_id in enumerate(migration_ids, start=1):
        print(f"{idx}. ID: {mig_id}")
    print()

    while True:
        choice = input("Select a migration to undo by number (or 'cancel' to exit): ").strip()
        if choice.lower() == 'cancel':
            return
        if not choice.isdigit() or not (1 <= int(choice) <= len(migration_ids)):
            print("Invalid selection. Please try again.")
            continue
        selected_mig_id = migration_ids[int(choice) - 1]
        break

    migration = next((m for m in state['migrations'] if m['id'] == selected_mig_id), None)
    if not migration:
        print("Migration not found in state.")
        return

    target_table_name = migration['target_table']
    key_schema = migration.get('key_schema')
    if not key_schema:
        print("Key schema information missing. Cannot perform undo.")
        return

    try:
        dynamodb = boto3.client('dynamodb')
    except Exception as e:
        logging.error(f"Error initializing DynamoDB client: {e}")
        print(f"Error initializing DynamoDB client: {e}")
        return

    undo_data = undo_state['undo_migrations'].get(selected_mig_id, {})
    delete_request = undo_data.get('DeleteRequest', {})
    undo_keys = delete_request.get('Key', [])

    if not undo_keys:
        print("No undo items found for this migration.")
        return

    try:
        print("\nStarting undo operation...")
        batch_size = 25
        total_undo = len(undo_keys)
        processed_undo = 0

        for i in range(0, total_undo, batch_size):
            batch_keys = undo_keys[i:i + batch_size]
            write_requests = []
            for key in batch_keys:
                write_requests.append({
                    'DeleteRequest': {
                        'Key': key
                    }
                })

            response = dynamodb.batch_write_item(
                RequestItems={
                    target_table_name: write_requests
                }
            )
            unprocessed = response.get('UnprocessedItems', {})
            if unprocessed.get(target_table_name):
                logging.warning(f"Unprocessed items detected during undo. Retrying...")
                retry_write_requests = unprocessed[target_table_name]
                execute_batch_write(target_table_name, retry_write_requests)

            processed_undo += len(write_requests)
            print(f"Undid {processed_undo}/{total_undo} items...")

        print("\nUndo operation completed successfully.")
        logging.info(f"Undo operation for migration '{selected_mig_id}' completed successfully.")
        undo_state['undo_migrations'].pop(selected_mig_id, None)
        save_undo_state(undo_state)

        # Update migration status to 'undone'
        migration_status = next((m for m in state['migrations'] if m['id'] == selected_mig_id), None)
        if migration_status and migration_status['status'] == 'completed':
            migration_status['status'] = 'undone'
            migration_status['processed_items'] = 0
            save_state(state)
            print(f"Migration '{selected_mig_id}' status updated to 'undone'.")

    except ClientError as e:
        logging.error(f"Undo Error: {e}")
        print(f"Undo Error: {e}")
    except Exception as e:
        logging.error(f"Unexpected Undo Error: {e}")
        print(f"Unexpected Undo Error: {e}")

def create_migration_id():
    return f"migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

def main():
    state = load_state()

    print("=== DynamoDB Migration Tool ===")

    migration = select_migration(state)

    if migration == 'undo':
        undo_last_migration(state)
        sys.exit(0)

    if migration:
        print(f"\nSelected Migration ID: {migration['id']} | Status: {migration['status']}")
        if migration['status'] == 'completed':
            print("This migration has already been completed.")
            proceed = input("Do you want to delete this migration and start a new one? (yes/no): ").strip().lower()
            if proceed == 'yes':
                state['migrations'].remove(migration)
                save_state(state)
                print("Migration deleted. You can start a new migration now.")
            else:
                print("Exiting the migration tool.")
                sys.exit(0)
        elif migration['status'] in ['in_progress', 'error', 'undone']:
            print("This migration is incomplete.")
            action = input("Do you want to (c)ontinue, (e)dit, or (d)elete this migration? (c/e/d): ").strip().lower()
            if action == 'c':
                print("Continuing the selected migration...\n")
            elif action == 'e':
                edit_migration(migration)
                if not show_summary(migration['source_table'], migration['target_table'], migration['column_mappings']):
                    print("Migration cancelled.")
                    logging.info("Migration cancelled by user.")
                    sys.exit(0)
            elif action == 'd':
                state['migrations'].remove(migration)
                save_state(state)
                print("Migration deleted.")
                migration = None
            else:
                print("Invalid choice. Exiting.")
                sys.exit(0)
    else:
        print("\n--- New Migration ---")
        source_table, target_table, column_mappings = get_user_input()
        if not show_summary(source_table, target_table, column_mappings):
            print("Migration cancelled.")
            logging.info("Migration cancelled by user.")
            sys.exit(0)
        migration_id = create_migration_id()
        migration = {
            "id": migration_id,
            "source_table": source_table,
            "target_table": target_table,
            "column_mappings": column_mappings,
            "last_evaluated_key": None,
            "processed_items": 0,
            "status": "in_progress"
        }
        state['migrations'].append(migration)
        save_state(state)
        print(f"\nNew migration '{migration_id}' created and ready to start.\n")

    if migration and migration['status'] in ['in_progress', 'error', 'undone']:
        migrate_data(state, migration)
    else:
        print("No migration to perform.")

if __name__ == "__main__":
    main()