# import libraries
from confluent_kafka import Consumer
import  json
import logging 
import time
import argparse
from datetime import datetime, timezone
import pandas as pd
import os
from google.cloud import storage



def setup_logger():
    # adding logging 
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    return(logging.getLogger(__name__))


def clean_dataframe(df):
    # Convert data types
    df['transaction_timestamp'] = pd.to_datetime(df['transaction_timestamp'], errors='coerce')
    df = df[df['transaction_timestamp'].notna()]
    df["transaction_timestamp"] = df["transaction_timestamp"].dt.round("us")

    df['transaction_timestamp'] = df['transaction_timestamp'].astype('datetime64[ms]')
    df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce').dt.date
    df[['transaction_id','user_id','card_last_4_digits']] = df[['transaction_id','user_id','card_last_4_digits']].astype(str)
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    df = df[df['amount'].notna()]

    # Drop rows missing critical values
    required_cols = ['user_id', 'transaction_id', 'amount']
    df= df.dropna(subset=required_cols)


    # rename columns 
    df = df.rename(columns={"amount": "transaction_amount",'card_last_4_digits': 'credit_card_last_4_digits'})


    return df

def schema_check(df):
    expected_columns = ['transaction_id', 'user_id', 'user_name','transaction_amount',
                        'transaction_timestamp','transaction_date','merchant','credit_card_last_4_digits',
                        'fraud_score','fraud_flag']
    
    incoming_columns  = df.columns.tolist()

    missing_columns = set(expected_columns) - set(incoming_columns) 
    extra_columns = set(expected_columns) - set(incoming_columns)  

    try:
        if len(missing_columns) == 0 or len(extra_columns):
            logging.info('Dataframe matches GCP')
    except ValueError as e:
        logging.error(f'Schema mismatch  {e}')


    return df

def read_to_gsc(df):
    # gettting date variables
    current_time = datetime.now(timezone.utc)
    year = current_time.strftime("%Y") 
    month = current_time.strftime("%m")
    day = current_time.strftime("%d")
    formatted_time = current_time.strftime("%Y-%m-%dT%H-%M-%SZ")
    
    #creating File and Path
    file_name = f"transactions/year={year}/month={month}/day={day}/transactions-{formatted_time}.parquet"
    folder_path = os.path.dirname(file_name)

    os.makedirs(folder_path, exist_ok=True)

    df.to_parquet(file_name, engine="pyarrow")
        
    return (file_name)

def upload_to_gcs(local_path, bucket_name,gcs_path,logger):
    max_retry = 3
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    for attempt in range (max_retry): 
        try:
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(local_path)
            logger.info(f"File {local_path} uploaded to GSC")
            os.remove(local_path)
            break
        except:
            if attempt < max_retry-1:
                logger.info(f"File {local_path} not uploaded retrying...now")
            else:
                logger.error(f"File not uploaded after max retries.. keeping locally")


def consume_messages(logger,topic):
    

    consumer_config = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'transaction-consumer-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False

    }

    

    consumer = Consumer(consumer_config)
    topic_name = topic
    consumer.subscribe([topic_name])
    logger.info(f"Subscribed to topic: {topic_name}")

    max_attempts = 3
    buffer = []
    batch_size = 1000
    last_flush_time = time.time()
    flush_interval  =  60
 

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                        continue
            for  attempt in range (max_attempts):
                try:
                    value = json.loads(message.value().decode('utf-8'))
                    buffer.append(value)

                    logger.info(f"Consumed message: {value}")

                    
                    if len(buffer) >= batch_size or (time.time() - last_flush_time >= flush_interval and len(buffer) > 750):
                        df = pd.DataFrame(buffer)
                        df = clean_dataframe(df)
                        df = schema_check(df)
                        file_path = read_to_gsc(df)
                        upload_to_gcs(file_path,"fraud-detection-data-001", file_path, logger)
                        consumer.commit(asynchronous=False)
                        buffer.clear()
                        last_flush_time = time.time()
                    
                    
                except Exception as e:
                    if attempt < max_attempts -1 : 
                        logger.warning(f"Failed to read {e} on attempt {attempt -1 } retrying...")
                        time.sleep(5)
                    else:
                        logger.error (f"Max Retries Hit Dropping message {e}")

    except KeyboardInterrupt:
        logger.info("Consumer interrupted. Closing now...")

    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")



def main():
    logger = setup_logger()
    # create CLI Arguments
    parser = argparse.ArgumentParser(description="Kafka Producer Script")
    parser.add_argument("--topic",default='transactions', help= "kafka topic name")
    args = parser.parse_args()
    topic = args.topic
    consume_messages(logger,topic)

if __name__ == "__main__":
    main()
