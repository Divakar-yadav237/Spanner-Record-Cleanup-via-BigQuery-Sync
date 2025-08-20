"""function to delete records in spanner"""
import functions_framework
from google.cloud import bigquery, spanner

# Initialize BigQuery and Spanner clients
bq_client = bigquery.Client()
spanner_client = spanner.Client()
instance = spanner_client.instance('dev')
database = instance.database('developer')

# Fetch all IDs from BigQuery
def get_all_records_from_bigquery():
    query = """SELECT DISTINCT Column from BQ table;"""
    query_job = bq_client.query(query)
    bq_records= {row['Column'] for row in query_job.result()}  # Return as a set of IDs
    print(f"Fetched {len(bq_records)} records from BigQuery")
    return bq_records

# Fetch all IDs from Spanner
def get_all_records_from_spanner_excluding_latest():
    # Initialize the lists to store the Column from both tables
    spanner_ids_list = []
    stg_spanner_ids_list = []

    # Query for table1 (Main Table)
    with database.snapshot() as snapshot:
        query_table1 = """
        select Column from main_table
        """
        result_table1 = snapshot.execute_sql(query_table1)
        for row in result_table1:
            spanner_ids_list.append(row[0])  # Add each ID to the list directly
        print(f"Fetched {len(spanner_ids_list)} records from Column (Main Table)")

    # Query for stg_table (Staging Table)
    with database.snapshot() as snapshot:
        query_stg_table = """
        select Column from STG_table
        """
        result_stg_table = snapshot.execute_sql(query_stg_table)
        for row in result_stg_table:
            stg_spanner_ids_list.append(row[0])  # Add each ID to the list directly
        print(f"Fetched {len(stg_spanner_ids_list)} records from STG_Table (Staging Table)")

    # Combine both lists into a set to remove duplicates
    spanner_ids = set(spanner_ids_list) | set(stg_spanner_ids_list)  # Using union (|) to combine sets

    print(f"Total combined Spanner records (Column + STG_Column): {len(spanner_ids)}")
    return spanner_ids

# Find records in Spanner that are missing in BigQuery
def find_deleted_records_in_spanner(bq_ids, spanner_ids):
    deleted_records = spanner_ids - bq_ids  # Set difference (records in Spanner but not in BigQuery)
    print(f"Found {len(deleted_records)} deleted records in Spanner that are not in BigQuery.")
    return deleted_records

# Preview records to be deleted from Spanner
def preview_deleted_records(deleted_records):
    print(f"\n{len(deleted_records)} records are missing in BigQuery and will be deleted from Spanner:")
    for Column in deleted_records:
        print(f"Record ID: {Column}")

# Delete records from Spanner
def delete_records_from_spanner(record_ids_to_delete):
    with database.batch() as batch:
        for Column in record_ids_to_delete:
            batch.delete(
                table='Column',
                keyset=spanner.KeySet(keys=[(Column)]),  # Assuming 'id' is the primary key
            )
            # Deleting from stg_table (Staging Table)
            batch.delete(
                table='STG_Column',
                keyset=spanner.KeySet(keys=[(Column)]),  # Assuming 'id' is the primary key
            )
    print(f"\nDeleted {len(record_ids_to_delete)} records from Spanner.")

# Main function to sync deletions from Spanner to BigQuery
# Cloud Function HTTP handler
@functions_framework.http
def sync_deletions(request):
    # Step 1: Fetch records from BigQuery excluding the latest recordstamp
    bq_ids = get_all_records_from_bigquery()
    print(f"BigQuery total records: {len(bq_ids)}")

    # Step 2: Fetch records from both Spanner tables, excluding the latest ones based on recordstamp
    spanner_ids= get_all_records_from_spanner_excluding_latest()
    print(f"Spanner total records: {len(spanner_ids)}")

    # Step 3: Find records in Spanner that are not in BigQuery
    deleted_records_in_spanner = find_deleted_records_in_spanner(bq_ids, spanner_ids)

    # Step 4: Preview records that will be deleted
    if deleted_records_in_spanner:
        preview_deleted_records(deleted_records_in_spanner)

        # Step 5: Ask the user if they want to proceed with deletion
        confirm_deletion = input("\nDo you want to delete these records from Spanner? (yes/no): ").strip().lower()

        if confirm_deletion == "yes":
            # Step 6: Delete the missing records from both Spanner tables if confirmed
            delete_records_from_spanner(deleted_records_in_spanner)
        else:
            print("Deletion canceled. No records were deleted.")
    else:
        print("No records to delete in Spanner.")
sync_deletions("request")
