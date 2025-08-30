import sys
from src.state_manager import load_state, save_state
from src.user_interface import (
    select_migration,
    edit_migration,
    get_user_input,
    show_summary,
    create_migration_id,
)
from src.migration_engine import migrate_data
from src.undo_operations import undo_last_migration
from src.utils.logger import get_logger

logger = get_logger()


def main():
    """Main entry point for the DynamoDB migration tool."""
    state = load_state()

    print("=== DynamoDB Migration Tool ===")

    migration = select_migration(state)

    if migration == "undo":

        undo_last_migration(state)

        sys.exit(0)

    if migration:

        print(
            f"\nSelected Migration ID: {migration['id']} | Status: {migration['status']}"
        )

        if migration["status"] == "completed":

            print("This migration has already been completed.")

            proceed = (
                input(
                    "Do you want to delete this migration and start a new one? (yes/no): "
                )
                .strip()
                .lower()
            )

            if proceed == "yes":

                state["migrations"].remove(migration)

                save_state(state)

                print("Migration deleted. You can start a new migration now.")

            else:

                print("Exiting the migration tool.")

                sys.exit(0)

        elif migration["status"] in ["in_progress", "error", "undone"]:

            print("This migration is incomplete.")

            action = (
                input(
                    "Do you want to (c)ontinue, (e)dit, or (d)elete this migration? (c/e/d): "
                )
                .strip()
                .lower()
            )

            if action == "c":

                print("Continuing the selected migration...\n")

            elif action == "e":

                edit_migration(migration)

                if not show_summary(
                    migration["source_table"],
                    migration["target_table"],
                    migration["column_mappings"],
                ):

                    print("Migration cancelled.")

                    logger.info("Migration cancelled by user.")

                    sys.exit(0)

            elif action == "d":

                state["migrations"].remove(migration)

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

            logger.info("Migration cancelled by user.")

            sys.exit(0)

        migration_id = create_migration_id()

        migration = {
            "id": migration_id,
            "source_table": source_table,
            "target_table": target_table,
            "column_mappings": column_mappings,
            "last_evaluated_key": None,
            "processed_items": 0,
            "status": "in_progress",
        }

        state["migrations"].append(migration)

        save_state(state)

        print(f"\nNew migration '{migration_id}' created and ready to start.\n")

    if migration and migration["status"] in ["in_progress", "error", "undone"]:

        migrate_data(state, migration)

    else:

        print("No migration to perform.")


if __name__ == "__main__":
    main()
