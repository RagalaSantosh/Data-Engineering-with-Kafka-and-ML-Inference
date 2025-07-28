import json
import time
import requests
import logging
from kafka import KafkaConsumer
from pymongo import MongoClient, errors
from datetime import datetime

# -------------------------------
# Configuration
# -------------------------------

KAFKA_BOOTSTRAP_SERVERS = '13.60.77.38:9093'  # Replace with your EC2 Kafka IP
TOPIC = 'test-topic'

MONGO_URI = 'mongodb://localhost:27017/'
DB_NAME = 'fraud_detection'
COLLECTION_NAME = 'predictions'

FASTAPI_ENDPOINT = 'http://127.0.0.1:8000/explain'
MAX_RETRIES = 3

# -------------------------------
# Setup Logging
# -------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaConsumer")

# -------------------------------
# Setup Kafka Consumer
# -------------------------------

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='fraud-detection-group'
)

# -------------------------------
# Setup MongoDB
# -------------------------------

mongo_client = MongoClient(MONGO_URI)
collection = mongo_client[DB_NAME][COLLECTION_NAME]

# Create unique index on transaction_id inside 'input'
try:
    collection.create_index("input.transaction_id", unique=True)
    logger.info("Created index on input.transaction_id")
except Exception as e:
    logger.warning("Index creation failed: %s", e)

# -------------------------------
# Main Loop
# -------------------------------

logger.info("Listening to Kafka topic '%s'...", TOPIC)

for message in consumer:
    print("message",message)
    transaction = message.value
    logger.info("Received transaction: %s", transaction)

    # Validate transaction_id presence
    if "transaction_id" not in transaction:
        logger.warning("Skipping message: missing transaction_id")
        continue

    # Retry calling the FastAPI endpoint
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(FASTAPI_ENDPOINT, json=transaction, timeout=5)

            if response.status_code == 200:
                result = response.json()

                # Construct MongoDB document
                record = {
                    "timestamp": datetime.utcnow(),
                    "input": transaction,
                    "prediction": result.get("prediction"),
                    "probability": result.get("probability"),
                    "shap_values": result.get("shap_values"),
                    "expected_value": result.get("expected_value")
                }

                try:
                    collection.insert_one(record)
                    logger.info("Saved prediction for transaction_id=%s", transaction["transaction_id"])
                except errors.DuplicateKeyError:
                    logger.warning("Duplicate transaction_id=%s. Skipping insert.", transaction["transaction_id"])

                # break  # Exit retry loop on success

            else:
                logger.error("Model API error (%s): %s", response.status_code, response.text)

        except requests.exceptions.RequestException as e:
            logger.error("Request error: %s (Attempt %d)", e, attempt + 1)
            time.sleep(2)  # Wait before retrying
