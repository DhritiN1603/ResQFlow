from kafka import KafkaConsumer,KafkaProducer
import json
import redis
import time
import threading
import math

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'new_emergency'
HEARTBEAT_TOPIC = 'worker_heartbeat'
HEARTBEAT_INTERVAL = 5  # Time interval to send heartbeats (seconds)

# Initialize Kafka Consumer and Producer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    group_id='emergency_group',
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Worker ID for unique identification in heartbeats
WORKER_ID = "worker_1" 

# Heartbeat Status
status = "idle" 

# Unit locations of emergency units hardcoded
UNIT_LOCATIONS = {
    "medical": {"lat": 12.9716, "lon": 77.5946},
    "fire": {"lat": 12.9260, "lon": 77.6762},
    "police": {"lat": 12.9902, "lon": 77.5372}
}

AVERAGE_SPEED_KMH = {
    "medical": 50,
    "fire": 30,
    "police": 45
}

def send_heartbeat():
    """Continuously send heartbeat messages to the Kafka heartbeat topic."""
    global status
    while True:
        heartbeat_message = {
            "worker_id": WORKER_ID,
            "status": status,
            "timestamp": time.time()
        }
        producer.send(HEARTBEAT_TOPIC, heartbeat_message)
        print(f"Heartbeat sent: {heartbeat_message}")
        time.sleep(HEARTBEAT_INTERVAL) 

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate the Haversine distance between two latitude/longitude points in kilometers."""
    R = 6371  # Radius of Earth in kilometers
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c
    return distance

def calculate_estimated_time(emergency_type, location):
    """Calculate the estimated arrival time for an emergency task."""
    unit_location = UNIT_LOCATIONS[emergency_type]
    speed_kmh = AVERAGE_SPEED_KMH[emergency_type]

    # Haversine formula
    distance = haversine_distance(
        unit_location["lat"], unit_location["lon"],
        location["lat"], location["lon"]
    )
    
    time_hours = distance / speed_kmh
    time_minutes = time_hours * 60  
    return round(time_minutes, 2)

# Emergency type functions
def handle_medical(task_data):
    """Handle medical emergency tasks."""
    estimated_time = calculate_estimated_time("medical", task_data["location"])
    time.sleep(10)
    print(f"In medical: Successful task {task_data}, Estimated arrival time: {estimated_time} minutes")
    

def handle_fire(task_data):
    """Handle fire emergency tasks."""
    estimated_time = calculate_estimated_time("fire", task_data["location"])
    time.sleep(10)
    print(f"In fire: Successful task {task_data}, Estimated arrival time: {estimated_time} minutes")

def handle_police(task_data):
    """Handle police emergency tasks."""
    estimated_time = calculate_estimated_time("police", task_data["location"])
    time.sleep(10)
    print(f"In police: Successful task {task_data}, Estimated arrival time: {estimated_time} minutes")

def process_task(message):
    """Process the emergency task based on its type."""
    global status
    task_data = json.loads(message.value.decode('utf-8'))
    print(f"Received task: {task_data}")
    
    # Set status to working while processing
    status = "working"

    if task_data["type"] == "medical":
        handle_medical(task_data)
    elif task_data["type"] == "fire":
        handle_fire(task_data)
    elif task_data["type"] == "police":
        handle_police(task_data)
    else:
        print(f"Unknown task type: {task_data['type']}")

    #  after processing status is set back 
    status = "idle"

def main():
    heartbeat_thread = threading.Thread(target=send_heartbeat)
    heartbeat_thread.daemon = True
    heartbeat_thread.start()
    
    for message in consumer:
        process_task(message)

if __name__ == "__main__":
    main()

