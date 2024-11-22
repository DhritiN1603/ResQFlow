import json
import redis
import time
import uuid
import logging
import threading
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'new_emergency'
HEARTBEAT_TOPIC = 'worker_heartbeat'

REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0

HEARTBEAT_TIMEOUT = 10

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("yadtq.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

class YADTQ:
    def __init__(self, broker, backend):
        self.broker = broker
        self.backend = backend
        self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
        self.producer = KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        self.consumer = None
        self.worker_id = f"worker_{uuid.uuid4()}"
        self.task_count = 0
        self.status = "idle" 
        self.worker_heartbeats = {} 

    def send_task(self, task_type, task_data):
        task_id = str(uuid.uuid4())
        task = {"task_id": task_id, "type": task_type, "data": task_data, "status": "queued"}
        try:
            self.producer.send(TOPIC_NAME, value=task)
            self.producer.flush()
            self._store_result(task_id, {"status": "queued"})
            logger.info(f"Task sent: {task}")
            return task_id
        except KafkaError as e:
            logger.error(f"Failed to send task: {e}")
            raise

    def get_task_status(self, task_id):
        result = self.redis_client.get(f"task_result:{task_id}")
        if result:
            return json.loads(result)
        return {"status": "unknown"}

    def initialize_consumer(self, group_id):
        self.consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=self.broker,
            group_id=group_id,
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8"))
        )
        logger.info(f"Worker {self.worker_id} initialized for topic {TOPIC_NAME} in group {group_id}")

    def process_task(self, process_callback):
        for message in self.consumer:
            task = message.value
            task_id = task["task_id"]
            try:
                self._store_result(task_id, {"status": "processing"})
                logger.info(f"Task {task_id} status updated to 'processing'.")
                
                self.status = "active"
                result = process_callback(task)

                if result.get("error"):
                    self._store_result(task_id, {"status": "failed", "error": result["error"]})
                    logger.error(f"Task {task_id} failed with error: {result['error']}")
                else:
                    self._store_result(task_id, {"status": "success", "result": result})
                    logger.info(f"Task {task_id} processed successfully with result: {result}")
                self.consumer.commit()
                self.task_count += 1

            except Exception as e:
                error_message = f"System error while processing task {task_id}: {e}"
                self._store_result(task_id, {"status": "failed", "error": error_message})
                logger.error(error_message)
            finally:
                self.status = "idle"


    def send_heartbeat(self, interval=5):
        while True:
            heartbeat = {
                "worker_id": self.worker_id,
                "status": self.status,
                "task_count": self.task_count,
                "timestamp": time.time(),
            }
            try:
                self.producer.send(HEARTBEAT_TOPIC, heartbeat)
                self.producer.flush()
                logger.info(f"Heartbeat sent: {heartbeat}")
            except KafkaError as e:
                logger.error(f"Failed to send heartbeat: {e}")
            time.sleep(interval)

    def monitor_heartbeats(self):
        consumer = KafkaConsumer(
            HEARTBEAT_TOPIC,
            bootstrap_servers=self.broker,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest"
        )
        logger.info("Monitoring worker heartbeats...")
        for message in consumer:
            heartbeat = message.value
            worker_id = heartbeat["worker_id"]
            timestamp = heartbeat["timestamp"]
            self.worker_heartbeats[worker_id] = timestamp
            self._check_unresponsive_workers()

    def _check_unresponsive_workers(self):
        current_time = time.time()
        unresponsive_workers = [
            worker_id for worker_id, last_seen in self.worker_heartbeats.items()
            if current_time - last_seen > HEARTBEAT_TIMEOUT
        ]

        for worker_id in unresponsive_workers:
            logger.warning(f"Worker {worker_id} is unresponsive. Reprocessing its tasks.")
            self._reprocess_tasks(worker_id)
            del self.worker_heartbeats[worker_id]

    def _reprocess_tasks(self, worker_id):
        """Reprocess tasks that were assigned to a failed worker."""
        for task_key in self.redis_client.scan_iter(f"task_result:*"):
            task_result = json.loads(self.redis_client.get(task_key))
            if task_result.get("status") == "processing" and task_result.get("worker_id") == worker_id:
                task_id = task_key.split(":")[1]
                task_data = self.redis_client.get(f"task_data:{task_id}")
                self._store_result(task_id, {"status": "queued"})
                self.send_task(task_data["type"], task_data["data"])
                logger.info(f"Task {task_id} requeued for processing.")

    def monitor_task_status(self, task_id, callback=None):
        retries = 3
        while True:
            try:
                status = self.get_task_status(task_id)
                if callback:
                    callback(task_id, status)

                if status.get("status") in ["success", "failed"]:
                    break
            except Exception as e:
                retries -= 1
                if retries == 0:
                    print(f"Failed to monitor task {task_id} after retries. Giving up.")
                    break
                time.sleep(2)
            time.sleep(2)

    def monitor_heartbeats(self, callback=None):
        consumer = KafkaConsumer(
            HEARTBEAT_TOPIC,
            bootstrap_servers=self.broker,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
        )
        for message in consumer:
            heartbeat = message.value
            worker_id = heartbeat.get("worker_id")
            status = heartbeat.get("status")
            task_count = heartbeat.get("task_count")
            timestamp = heartbeat.get("timestamp")

            if callback:
                callback(worker_id, status, task_count, timestamp)

    def _store_result(self, task_id, result):
        self.redis_client.set(f"task_result:{task_id}", json.dumps(result))
