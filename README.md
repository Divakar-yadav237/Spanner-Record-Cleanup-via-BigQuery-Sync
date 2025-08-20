
# Spanner Record Cleanup via BigQuery Sync

This Cloud Function identifies and deletes outdated records from Google Cloud Spanner by comparing them with a reference dataset in BigQuery. 
It ensures that Spanner only retains records that are still present in BigQuery, helping maintain data consistency across systems.

## GCP Services Used

1. BigQuery Client API  
2. Spanner Client API  
3. Cloud Run (with Python Function Framework)

## Prerequisites

Before deploying or running this function, ensure the following:

Google Cloud SDK is installed and authenticated.
You have access to:
1. A BigQuery dataset with the reference table.
2. A Cloud Spanner instance with main_table and STG_table.
3. Python 3.9+ environment.
4. Required Python packages are mentioned in requirements.txt file.


## Setup & Deployment
1. Clone the repository: **https://github.com/Divakar-yadav237/Spanner-Record-Cleanup-via-BigQuery-Sync.git**

2. Deploy to Google Cloud Functions: **As mentioned in below [1].**


Links:

[1] gcloud beta run deploy Cleanup_spanner_records \
      --source . \
      --function sync_deletions \
      --base-image python312 \
      --region us-central1 \
      --allow-unauthenticated \
      --update-env-vars PROJECT_ID="us",INSTANCE_ID="dev",DATABASE_ID="developer"
