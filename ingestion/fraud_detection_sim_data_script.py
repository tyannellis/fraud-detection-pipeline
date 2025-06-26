# ingestion/generate_transactions.py

from faker import Faker
import uuid
import random
from datetime import datetime
import json

# Step 1: Initialize Faker instance
fake = Faker()

# Step 2: Define a function that will return a fake transaction dictionary
def transaction_sim():
    transaction_list = []
    for number in range (10):
        transaction ={'transaction_id':str(uuid.uuid4()),
                'user_id':str(uuid.uuid4()),
                'amount':round(random.uniform(0.01, 5000.00),2),
                'timestamp':datetime.now().isoformat(),
                'merchant': fake.company(),
                'user_name': fake.name(),
                'card_last_4_digits': round(random.uniform(1000, 9999)),
                 "is_fraud": random.choice([0, 0, 0, 1])}
        
        transaction_list.append(transaction)
    return(transaction_list)

def write_transaction_to_file(transactions):
    with open('/Users/gregellis/fraud-detection-pipeline/data_lake/transactions.json', 'w') as f:
        json.dump(transactions, f, indent=2)

def load_transactions_from_json():
    with open('/Users/gregellis/fraud-detection-pipeline/data_lake/transactions.json', 'r') as f:
        return json.load(f)
    
def main():
    transactions = transaction_sim()
    write_transaction_to_file(transactions)
    transaction_file = load_transactions_from_json()
    print(transaction_file[0])

if __name__ == "__main__":
    main()


