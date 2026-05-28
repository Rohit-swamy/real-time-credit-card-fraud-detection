from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Fixed card pool for historical behavior
cards = [111111, 222222, 333333]

while True:

    fraud_type = random.choices(
    ["normal", "rapid", "location", "spike"],
    weights=[70, 10, 10, 10],
    k=1
    )[0]

    card_id = random.choice(cards)

    # =====================================================
    # NORMAL TRANSACTION
    # =====================================================

    if fraud_type == "normal":

        transaction = {
            "card_id": card_id,
            "member_id": random.randint(1000, 9999),
            "amount": random.randint(1000, 5000),
            "postcode": 500001,
            "pos_id": random.randint(1, 50),
            "transaction_dt": str(datetime.now())
        }

    # =====================================================
    # RAPID TRANSACTION FRAUD
    # =====================================================

    elif fraud_type == "rapid":

        transaction = {
            "card_id": 111111,
            "member_id": 2001,
            "amount": random.randint(2000, 4000),
            "postcode": 500001,
            "pos_id": 10,
            "transaction_dt": str(datetime.now())
        }

    # =====================================================
    # LOCATION FRAUD
    # =====================================================

    elif fraud_type == "location":

        transaction = {
            "card_id": 222222,
            "member_id": 2002,
            "amount": random.randint(3000, 7000),
            "postcode": random.choices(
                [500001, 500002, 500003, 500004, 500005, 700001],
                weights=[25, 25, 20, 15, 10, 5],
                k=1
            )[0],
            "pos_id": 20,
            "transaction_dt": str(datetime.now())
        }

    # =====================================================
    # SPENDING SPIKE FRAUD
    # =====================================================

    else:

        transaction = {
            "card_id": 333333,
            "member_id": 2003,
            "amount": random.choice([2000, 2500, 3000, 90000]),
            "postcode": 500001,
            "pos_id": 30,
            "transaction_dt": str(datetime.now())
        }

    producer.send("transactions", transaction)

    print("Transaction Sent:", transaction)

    time.sleep(2)
