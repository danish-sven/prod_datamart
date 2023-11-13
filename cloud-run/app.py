import os
from flask import Flask
from setup_dataset import setup_master_dataset
from delete_dataset import delete_removed_datasets
from views_check import sync_views
from authorise_views import apply_permissions
import logging
from google.cloud import bigquery

# Configure logging #
logging.basicConfig(level=logging.INFO)

app = Flask(__name__)

project_id = '' ## DEFINE PROJECT ID HERE
sql_base_dir = 'cloud-run/sql'
client = bigquery.Client(project=project_id)

def main_process(): 
    logging.info("Starting main process")
    local_datasets = []

    # Iterate through directories in the 'sql' folder
    for dataset_folder in os.listdir(sql_base_dir):
        dataset_path = os.path.join(sql_base_dir, dataset_folder)
        if os.path.isdir(dataset_path):
            logging.info(f"Processing dataset folder: {dataset_folder}")
            local_datasets.append(dataset_folder)
            setup_master_dataset(dataset_folder, project_id, dataset_path)
            sync_views(dataset_folder, project_id, dataset_path)
            
            # Apply permissions for each view in the dataset
            dataset_ref = bigquery.DatasetReference(project_id, dataset_folder)
            for table in client.list_tables(dataset_ref):
                if table.table_type == 'VIEW':
                    apply_permissions(client, project_id, dataset_folder, table.table_id)

        else:
            logging.info(f"Skipping non-directory: {dataset_folder}")

    # Delete remote datasets not present in the 'sql' folder
    logging.info("Deleting removed datasets")
    delete_removed_datasets(project_id, local_datasets)
    
    logging.info("Main process completed")
    return 'Datamart Updated'

@app.route('/main', methods=['POST'])
def main():
    logging.info("Received request to '/main' endpoint")
    return main_process()

if __name__ == "__main__":
    logging.info("Running app directly")
    main_process()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
