# Spanner-Record-Cleanup-via-BigQuery-Sync
This Cloud Function identifies and deletes outdated records from Google Cloud Spanner by comparing them with a reference dataset in BigQuery.  It ensures that Spanner only retains records that are still present in BigQuery, helping maintain data consistency across systems.
