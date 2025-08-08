
from google.cloud import bigquery
from datetime import datetime, timezone
import logging

def setup_logger():
    # adding logging 
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    return(logging.getLogger(__name__))


def table_create(logger):
    client = bigquery.Client()
    
    # GCS file path
    current_time = datetime.now(timezone.utc)
    year = current_time.strftime("%Y") 
    month = current_time.strftime("%m")
    day = current_time.strftime("%d")

    gcs_uri = f"gs://fraud-detection-data-001/transactions/year={year}/month={month}/day={day}/*.parquet"

    # BigQuery table create statement
    create_table_statement = '''CREATE TABLE IF NOT EXISTS sylvan-epoch-466712-v1.raw_data.raw_transactions (
    transaction_id STRING,
    user_id STRING,
    user_name STRING,
    transaction_amount FLOAT64,
    transaction_timestamp TIMESTAMP,
    transaction_date DATE,
    merchant STRING,
    credit_card_last_4_digits STRING,
    fraud_score FLOAT64,
    fraud_flag BOOL
    )
    PARTITION BY transaction_date
    CLUSTER BY user_id, fraud_flag;
    '''

    # Create the table
    query_job = client.query(create_table_statement)
    query_job.result()  # Wait for job to finish
    logger.info("✅ Table created (or already exists)")


def main():
    logger = setup_logger()
    table_create(logger)

if __name__ == '__main__':
    main()