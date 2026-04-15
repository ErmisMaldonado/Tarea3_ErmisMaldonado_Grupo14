from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    data = {
        "user_id": random.randint(1, 100),
        "purchase": random.randint(10, 500)
    }

    producer.send('ecommerce', data)
    print("Enviado:", data)

    time.sleep(2)
