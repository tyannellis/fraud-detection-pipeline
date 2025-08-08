from google.cloud import bigquery
from datetime import datetime, timezone
import logging
from google.cloud import storage
import time


def setup_logger():
    # adding logging 
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    return(logging.getLogger(__name__))


def wait_for_gcs_folder(bucket_name, prefix, timeout_seconds=500, check_interval=30):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    start_time = time.time()

    while time.time() - start_time < timeout_seconds:
        blobs = list(bucket.list_blobs(prefix=prefix))
        if blobs:
            print(f"GCS folder found: {prefix}")
            return True
        print(f"Waiting for folder: {prefix}")
        time.sleep(check_interval)

    raise TimeoutError(f"GCS folder not found after {timeout_seconds} seconds.")

def load_to_bigquery(logger):
    client = bigquery.Client()
    
    # GCS file path
    current_time = datetime.now(timezone.utc)
    year = current_time.strftime("%Y") 
    month = current_time.strftime("%m")
    day = current_time.strftime("%d")

    gcs_uri = f"gs://fraud-detection-data-001/transactions/year={year}/month={month}/day={day}/*.parquet"
    
    # BigQuery table reference
    table_id = "sylvan-epoch-466712-v1.raw_data.raw_transactions"

    wait_for_gcs_folder(
        bucket_name="fraud-detection-data-001",
        prefix=f"transactions/year={year}/month={month}/day={day}/",
        timeout_seconds=300,
        check_interval=10
    )

    logger.info('folder created..proceed with big query load')

    # Load config
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET, 
        autodetect=True
    )

    # Load data from GCS
    load_job = client.load_table_from_uri(
        gcs_uri,
        table_id,
        job_config=job_config
    )

    load_job.result()  # Wait for job to finish

    logger.info(f"✅ Loaded {load_job.output_rows} rows into {table_id}.")

def main():
    logger = setup_logger()
    load_to_bigquery(logger)


if __name__ == "__main__":
    main()