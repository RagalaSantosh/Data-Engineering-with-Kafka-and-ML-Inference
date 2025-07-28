import json
import time
import logging
from kafka import KafkaConsumer
from pymongo import MongoClient, errors
from datetime import datetime
import joblib
import pandas as pd
import numpy as np

# -------------------------------
# Configuration
# -------------------------------

KAFKA_BOOTSTRAP_SERVERS = '13.60.77.38:9093'
TOPIC = 'test-topic'

MONGO_URI = 'mongodb://localhost:27017/'
DB_NAME = 'anomaly_detection'
COLLECTION_NAME = 'predictions'

MODEL_PATH = './models/iso_forest_model.pkl'

# Define the expected columns
FEATURE_COLUMNS = ['Destination Port', 'Flow Duration', 'Total Fwd Packets',
                   'Total Backward Packets', 'Total Length of Fwd Packets',
                   'Total Length of Bwd Packets', 'Fwd Packet Length Max',
                   'Fwd Packet Length Min', 'Fwd Packet Length Mean',
                   'Fwd Packet Length Std', 'Bwd Packet Length Max',
                   'Bwd Packet Length Min', 'Bwd Packet Length Mean',
                   'Bwd Packet Length Std', 'Flow Bytes/s', 'Flow Packets/s',
                   'Flow IAT Mean', 'Flow IAT Std', 'Flow IAT Max', 'Flow IAT Min',
                   'Fwd IAT Total', 'Fwd IAT Mean', 'Fwd IAT Std', 'Fwd IAT Max',
                   'Fwd IAT Min', 'Bwd IAT Total', 'Bwd IAT Mean', 'Bwd IAT Std',
                   'Bwd IAT Max', 'Bwd IAT Min', 'Fwd PSH Flags', 'Bwd PSH Flags',
                   'Fwd URG Flags', 'Fwd Header Length', 'Bwd Header Length',
                   'Fwd Packets/s', 'Bwd Packets/s', 'Min Packet Length',
                   'Max Packet Length', 'Packet Length Mean', 'Packet Length Std',
                   'Packet Length Variance', 'FIN Flag Count', 'RST Flag Count',
                   'PSH Flag Count', 'ACK Flag Count', 'URG Flag Count', 'ECE Flag Count',
                   'Down/Up Ratio', 'Average Packet Size', 'Avg Bwd Segment Size',
                   'Subflow Fwd Bytes', 'Subflow Bwd Bytes', 'Init_Win_bytes_forward',
                   'Init_Win_bytes_backward', 'act_data_pkt_fwd', 'min_seg_size_forward',
                   'Active Mean', 'Active Std', 'Active Max', 'Active Min', 'Idle Mean',
                   'Idle Std', 'Idle Max', 'Idle Min']

# -------------------------------
# Setup Logging
# -------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaConsumer")

# -------------------------------
# Load the Isolation Forest model
# -------------------------------

iso_forest = joblib.load(MODEL_PATH)
logger.info("Isolation Forest model loaded from %s", MODEL_PATH)

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
    transaction = message.value
    logger.info("Received transaction: %s", transaction)

    if "transaction_id" not in transaction:
        logger.warning("Missing transaction_id. Skipping.")
        continue

    # Extract features from transaction
    try:
        feature_values = [transaction[col] for col in FEATURE_COLUMNS]
        X_input = pd.DataFrame([feature_values], columns=FEATURE_COLUMNS)
    except KeyError as e:
        logger.error("Missing expected feature: %s. Skipping.", str(e))
        continue

    # Perform prediction
    try:
        prediction = iso_forest.predict(X_input)[0]     # -1 = anomaly, 1 = normal
        score = iso_forest.decision_function(X_input)[0]

        result = {
            "timestamp": datetime.utcnow(),
            "input": transaction,
            "prediction": "Anomaly" if prediction == -1 else "Normal",
            "probability": float(score),  # anomaly score
            "shap_values": None,          # Optional: add SHAP if you have it
            "expected_value": None        # Optional
        }

        try:
            collection.insert_one(result)
            logger.info("Saved prediction for transaction_id=%s", transaction["transaction_id"])
        except errors.DuplicateKeyError:
            logger.warning("Duplicate transaction_id=%s. Skipping insert.", transaction["transaction_id"])

    except Exception as e:
        logger.error("Prediction failed: %s", str(e))
