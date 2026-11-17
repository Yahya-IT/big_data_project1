import json
import time
from kafka import KafkaProducer
import pandas as pd

# 1. Connect to the Kafka broker running in Docker
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
)

# 2. Load the 5,000-row CSV dataset (must be in the same folder as producer.py)
df = pd.read_csv('traffic_accidents_5000.csv')

# Replace pandas NaN with None so JSON outputs clean `null` values
df = df.where(pd.notnull(df), None)

print(f'Starting stream of {len(df)} records to raw-accidents...')

# 3. Stream row-by-row into Kafka
for index, row in df.iterrows():
  record = row.to_dict()
  producer.send('raw-accidents', value=record)
  print(f'Sent [{index + 1}]: {record}')
  time.sleep(0.5)

producer.flush()
producer.close()