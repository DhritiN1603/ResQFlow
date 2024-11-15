import time
import json
import uuid
import random
from kafka import KafkaProducer

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'new_emergency'

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Emergency Types and Information
EMERGENCY_TYPES = ["medical", "fire", "police"]

def generate_random_location():
    """Generating a random location in Bangalore within a defined bounding box."""
    latitude = random.uniform(12.8, 13.1)
    longitude = random.uniform(77.5, 77.7)
    return {"lat": latitude, "lon": longitude}

def send_task(emergency_type):
    """Sending an emergency task to the Kafka queue."""
    task_id = str(uuid.uuid4())
    location = generate_random_location()
    task = {
        "task_id": task_id,
        "type": emergency_type,
        "location": location
    }
    producer.send(TOPIC_NAME, task)
    print(f"Task sent: {task}")

if __name__ == "__main__":
    # Send emergency tasks continuously until stopped manually
    while True:
        # Generating a random emergency
        emergency_type = random.choice(EMERGENCY_TYPES)
        send_task(emergency_type)
        
        # Waiting before sending the next task 
        time.sleep(5)

