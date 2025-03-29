import json
import random
import time
from kafka import KafkaProducer

# Define at least five sensor IDs.
sensors = ["S101", "S102", "S103", "S104", "S105"]

# Initialize the Kafka producer.
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def generate_event():
    event = {
        "sensor_id": random.choice(sensors),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
        "vehicle_count": random.randint(0, 50),
        "average_speed": round(random.uniform(20.0, 100.0), 2),
        "congestion_level": random.choice(["LOW", "MEDIUM", "HIGH"]),
    }
    return event


if __name__ == "__main__":
    topic = "traffic_data"
    while True:
        event = generate_event()
        producer.send(topic, event)
        print("Sent event:", event)
        time.sleep(1)
