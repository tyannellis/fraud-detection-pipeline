# ingestion/generate_transactions.py

from faker import Faker
import uuid
import random
from datetime import datetime
import json
import time
import logging  
import argparse
from functools import partial
from confluent_kafka import Producer

#Initialize Faker instance
fake = Faker()

#adding logging
def setup_logger(log_to_file=False):
    handlers = [logging.StreamHandler()]
    
    if log_to_file:
        handlers.append(logging.FileHandler("transaction_stream.log"))

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=handlers,
        force = True
    )

    return logging.getLogger(__name__)

# Step 1: create distict user group 
def create_users(user_count,logger):
    user_list = []
    for num in range (user_count):
        user =  {'user_id':str(uuid.uuid4()),
            'user_name': fake.name()}
        user_list.append(user)
    logger.info(f"Created {user_count} users")

    return user_list


# Step 2: Define a function that will return a fake transaction dictionary
def generate_transaction(user_list):
    user = random.choice(user_list)  # randomly select an existing user

    # timstamp from last 6 months 
    timestamp = fake.date_time_between(start_date='-6M', end_date='now')
           
    # randomly assigned fraud score 
    fraud_score = round(random.uniform(0, 1), 4)
            
    transaction = {'transaction_id':str(uuid.uuid4()),
                    'user_id':user['user_id'],
                    'amount':round(random.uniform(0.01, 5000.00),2),
                    'transaction_timestamp': timestamp.isoformat(),  
                    'transaction_date': timestamp.date().isoformat(), 
                    'merchant': fake.company(),
                    'user_name': user['user_name'],
                    'card_last_4_digits': random.randint(1000, 9999),
                    "fraud_score": fraud_score,
                    "fraud_flag": True if fraud_score > 0.995 else False}
            
                
    return transaction
# Step 3: Set up logging



# Step 4: print Kafka message delivery status
def delivery_report(err, msg,logger):
    if err is not None:
        logger.error(f'❌ Delivery failed: {err}')
    else:
        logger.info(f'✅ Sent to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')


# Step 5: send messages to kafka topic

def stream_forever(topic, delay_seconds,user_count,logger):

    logger.info("🚀 Starting transaction stream...")
    users = create_users(user_count=user_count, logger=logger)
    # Kafka config
    conf = {'bootstrap.servers': 'kafka:9092'}
    producer = Producer(conf)

    max_attempts = 3
    retry_delay = 5
    try:
        while True:
                transaction = generate_transaction(users)
                for attempt in range(max_attempts):
                    try:
                            producer.produce(
                                topic,
                                key=transaction['user_id'],
                                value=json.dumps(transaction),
                                callback=partial(delivery_report, logger=logger)
                            )
                            producer.poll(0)  # handle delivery callbacks
                            break
                    except Exception as e:
                        if attempt < max_attempts -1:
                            logger.info(f"Failed to send message: {e} at attempt number {attempt} retrying...")
                            time.sleep(retry_delay)
                            continue

                        else:
                            logger.error(f"Max retries hit, dropping message {e}")
                time.sleep(delay_seconds)  # simulate real-time frequency
    except KeyboardInterrupt:
        logger.info("🛑 Stream stopped by user.")
    finally:
        producer.flush()
        logger.info("Producer Closed")


# Step 6: 


def main():
    # create CLI Arguments
    parser = argparse.ArgumentParser(description="Kafka Producer Script")
    parser.add_argument("--log-to-file", action="store_true", help="Enable logging to a file")
    parser.add_argument("--topic",default='transactions', help= "kafka topic name")
    parser.add_argument("--user-count",type=int, default=100 , help= "kafka topic name")
    parser.add_argument("--delay-seconds", type = int, default=1, help = 'The delay in seconds between sending messages')
    args = parser.parse_args()

    logger = setup_logger(log_to_file=args.log_to_file)
    user_count = args.user_count
    delay_seconds = args.delay_seconds
    topic = args.topic
    
    stream_forever(topic,delay_seconds,user_count,logger)
    
if __name__ == "__main__":
    main()