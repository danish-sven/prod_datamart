# BigQuery Datasets and Views Manager

This repository helps you manage datasets and views in Google BigQuery by maintaining a local folder structure containing SQL files. The Python script provided in the repository creates, updates, or deletes datasets and views in BigQuery based on the local folder structure.

## How It Works

- The script reads the folder structure under `/sql` and processes each dataset folder.
- For each dataset folder, it checks if the corresponding dataset exists in BigQuery. If not, it creates a new dataset.
- For each `.sql` file within a dataset folder, the script checks if the corresponding view exists in the dataset. If not, it creates a new view. If the view exists, it updates the view with the contents of the `.sql` file.
- If a dataset folder is removed from the `/sql` directory and the script is run, the script will delete the corresponding dataset and its views from BigQuery.
- if a `.sql` file is deleted from a dataset folder, the script will delete the view in BigQuery.


## Folder Structure

The local folder structure should follow this pattern:
```bash
/cloud-run
   /sql
      ├── vehicle
      │   └── master_vehicle.sql
      ├── battery
      │   └── master_battery.sql
      ├── dataset3
      │   ├── view1.sql
      │   └── view2.sql
      └── etc... 
```

Where each subfolder under `/sql` represents a dataset in BigQuery, and each `.sql` file within a dataset's folder represents a view in the corresponding dataset.

The final structure in BigQuery will then appear as:
```bash
central-ops-datamart-4fe3 (project)
   ├── vehicle (dataset)
   │   └── master_vehicle (view)
   ├── battery (dataset)
   │   └── master_battery (view)
   ├── dataset3
   │   ├── view1
   │   └── view2
   └── etc... 
```

## How to Add or Modify Datasets and Views

1. Make sure you have sufficient permissions to create, modify and delete datasets and views in BigQuery.

2. If not already installed, install [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git) and [Python](https://www.python.org/downloads/).

3. Clone the repository by navigating to your desired folder and typing the following into terminal: \
```git clone https://github.com/ridezoomo/ops-gcp-datamart.git```

4. Set up Google Cloud SDK and authenticate with your Google Cloud Platform account:
- [Install Google Cloud SDK](https://cloud.google.com/sdk/docs/install)
- [Initialize Google Cloud SDK](https://cloud.google.com/sdk/docs/initializing)
- [Authenticate your account](https://cloud.google.com/sdk/gcloud/reference/auth/login)

5. In this repository, create a new folder inside the sql folder for each dataset you want to create in BigQuery. The folder's name should be the desired dataset ID.

#### NB: What you name the folder will be what the dataset is named in BigQuery. Use these guidelines for naming the folders:
- Keep it lower case 
- as few words as possible 
- no spaces 
- no numbers 
- no punctuation other than underscores `_`

```bash
/cloud-run
   /sql
      ├── vehicle (new folder)
      ├── other_folder
      └── another_folder
```

6. Inside each dataset folder, create a .sql file for each view you want to create or update. The SQL file's name (without the .sql extension) will be the view's ID in BigQuery. If the .sql file already exists and you wish to update it, simply paste your new code in the existing view to replace the old.

```bash
/cloud-run
   /sql
      ├── vehicle
      │   ├── master_vehicle.sql (new file)
      │   └── other_vehicle_view.sql (new file)
      ├── other_folder
      └── another_folder
```

7. Stage and commit your changes. Ie, in your terminal, enter the following commands: \
```git add .``` \
```git commit -m "Insert comment here describing changes made"```  

8. Finally, push the changes to the repo in Git. This will trigger the listener in cloudbuild which will run the functions and update the GCP project. In your terminal, copy and paste the following code: \
```git push origin main```

## Repo Overview

- `README.md`: This file, containing instructions and explanations.
- `Dockerfile`: The Dockerfile which handles the environment variables to ensure anyone can run this script.
- `/cloud-run`: The folder containing all python scripts. You won't need to make any changes in here.
- `/sql`: The folder containing all dataset folders and their corresponding SQL files for views. This folder structure will be replicated in BigQuery under the chosen GCP project.
- `app.py`: This Python script defines the specific GCP project for which to run the following funtions:
- `setup_dataset.py`: This Python script defines a function which creates a given dataset and included views in BigQuery.
- `delete_dataset.py`: This Python script defines a function which scans the local folder structure and deletes any datasets in BigQuery which do not match.
- `views_check.py`: This Python script defines a function which scans the views in BigQuery and the local .sql files and ensures they are a 1-1 match.
- `cloudbuild.yaml`: The cloudbuild setup instructions.
- `requirements.txt`: The Python libraries required.

## Troubleshooting

In case of any errors or issues, you can check the logs in the Google Cloud Console's Cloud Functions section for the triggered function. This will help you identify the cause of the problem and take appropriate action.