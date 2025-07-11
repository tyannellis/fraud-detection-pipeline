# ingestion/generate_transactions.py

from faker import Faker
import uuid
import random
from datetime import datetime
import json
import time
from confluent_kafka import Producer

# Kafka config
conf = {'bootstrap.servers': 'kafka:9092'}
producer = Producer(conf)

#Initialize Faker instance
fake = Faker()

# Step 1: create distict user group 
def create_users(user_count):
    user_list = []
    for num in range (user_count):
        user =  {'user_id':str(uuid.uuid4()),
            'user_name': fake.name()}
        user_list.append(user)
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
                    'card_last_4_digits': round(random.uniform(1000, 9999)),
                    "fraud_score": fraud_score,
                    "fraud_flag": True if fraud_score > 0.995 else False}
            
                
    return transaction

# Step 3: print Kafka message delivery status
def delivery_report(err, msg):
    if err is not None:
        print(f'❌ Delivery failed: {err}')
    else:
        print(f'✅ Sent to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')


# Step 4: send messages to kafka topic

def stream_forever(topic='transactions', delay_seconds=1):
    print("🚀 Starting transaction stream...")
    users = create_users(1000)
    try:
        while True:
            transaction = generate_transaction(users)
            producer.produce(
                topic,
                key=transaction['user_id'],
                value=json.dumps(transaction),
                callback=delivery_report
            )
            producer.poll(0)  # handle delivery callbacks
            time.sleep(delay_seconds)  # simulate real-time frequency
    except KeyboardInterrupt:
        print("🛑 Stream stopped by user.")
    finally:
        producer.flush()


def main():
    

    stream_forever(topic='transactions', delay_seconds=1)
    
if __name__ == "__main__":
    main()