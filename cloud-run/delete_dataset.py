import logging
from google.cloud import bigquery

# Configure logging
logging.basicConfig(level=logging.INFO)

def delete_removed_datasets(project_id, local_datasets):
    """
    Delete datasets in BigQuery that are not present in the local 'sql' folder.

    :param project_id: The ID of the GCP project.
    :param local_datasets: A list of local dataset folder names.
    """

    logging.info(f"Starting to delete removed datasets in project '{project_id}'")

    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)
    logging.info("BigQuery client initialized")

    # Get a list of remote datasets in the project
    remote_datasets = list(client.list_datasets())
    logging.info(f"Found {len(remote_datasets)} datasets in project '{project_id}'")

    # Iterate through remote datasets and delete those not in the local_datasets list
    for remote_dataset in remote_datasets:
        if remote_dataset.dataset_id not in local_datasets:
            logging.info(f"Dataset '{remote_dataset.dataset_id}' not in local datasets, deleting it")
            dataset_ref = client.dataset(remote_dataset.dataset_id)
            try:
                client.delete_dataset(dataset_ref, delete_contents=True, not_found_ok=True)
                logging.info(f"Dataset '{remote_dataset.dataset_id}' deleted")
            except Exception as e:
                logging.error(f"Error deleting dataset '{remote_dataset.dataset_id}': {e}")
        else:
            logging.info(f"Dataset '{remote_dataset.dataset_id}' found in local datasets, skipping deletion")

    logging.info("Deletion of removed datasets complete")
