import logging
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import os

# Configure logging
logging.basicConfig(level=logging.INFO)

def sync_views(dataset_id, project_id, sql_dir):
    """
    Ensure every .sql file has a view and every view has a .sql file.

    :param dataset_id: The ID of the dataset to be synced.
    :param project_id: The ID of the GCP project.
    :param sql_dir: The directory containing the SQL files for the dataset's views.
    """

    logging.info(f"Syncing views for dataset '{dataset_id}' in project '{project_id}' using SQL files from {sql_dir}")

    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)
    logging.info("BigQuery client initialized")

    # Get the dataset
    dataset_ref = client.dataset(dataset_id)
    try:
        dataset = client.get_dataset(dataset_ref)
        logging.info(f"Dataset {dataset_id} found")
    except NotFound:
        logging.error(f"Dataset {dataset_id} does not exist")
        return

    # Get a list of views in the dataset
    views = [table for table in client.list_tables(dataset) if table.table_type == 'VIEW']
    logging.info(f"Found {len(views)} views in dataset '{dataset_id}'")

    # Create a list of view names
    view_names = [view.table_id for view in views]

    # Iterate through the SQL files in the given directory and create a list of view names from SQL files
    sql_files = []
    for root, dirs, files in os.walk(sql_dir):
        for file in files:
            if file.endswith('.sql'):
                sql_file = os.path.join(root, file)
                view_name = os.path.splitext(os.path.basename(sql_file))[0]
                sql_files.append(view_name)

                if view_name not in view_names:
                    logging.warning(f".sql file {file} does not have a corresponding view in dataset '{dataset_id}'")

    logging.info(f"Found {len(sql_files)} .sql files in directory '{sql_dir}'")

    # Check if views have a corresponding .sql file, if not delete them
    for view in views:
        if view.table_id not in sql_files:
            view_ref = dataset_ref.table(view.table_id)
            try:
                client.delete_table(view_ref, not_found_ok=True)
                logging.info(f"View '{view.table_id}' does not have a corresponding .sql file and was deleted")
            except Exception as e:
                logging.error(f"Error deleting view '{view.table_id}': {e}")

    logging.info("View syncing complete")
