import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

topic = "user_interactions"

while True:
    event = {
        "user_id": random.randint(1, 5000),
        "item_id": random.randint(1, 1000),
        "rating": round(random.uniform(1.0, 5.0), 1),
        "timestamp": datetime.utcnow().isoformat()
    }

    producer.send(topic, event)
    print("Sent:", event)

    time.sleep(1)
