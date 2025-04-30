import json
import random
import time
from kafka import KafkaProducer
from prometheus_client import Counter, start_http_server


# Start an HTTP server to expose metrics on port 8000
start_http_server(8000)

# Define a Counter metric for vehicle counts, with labels for sensor_id and congestion_level
vehicle_counts = Counter(
    "vehicle_count_total",
    "Total number of vehicle count",
    ["sensor_id", "congestion_level"],
)

# Define at least five sensor IDs.
sensors = ["S101", "S102", "S103", "S104", "S105"]

# Initialize the Kafka producer.
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def generate_event(sensor_id, congestion_level):
    event = {
        "sensor_id": sensor_id,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
        "vehicle_count": random.randint(0, 50),
        "average_speed": round(random.uniform(20.0, 100.0), 2),
        "congestion_level": congestion_level,
    }
    return event


if __name__ == "__main__":
    topic = "traffic_data"
    while True:
        sensor_id = random.choice(sensors)  # type: ignore
        congestion_level = random.choice(["LOW", "MEDIUM", "HIGH"])  # type: ignore
        event = generate_event(sensor_id, congestion_level)
        producer.send(topic, event)
        # Increment the Prometheus counter
        vehicle_counts.labels(
            sensor_id=sensor_id, congestion_level=congestion_level
        ).inc()
        print("Sent event:", event)
        time.sleep(1)
