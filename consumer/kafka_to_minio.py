import boto3
from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime
import os
import time
from dotenv import load_dotenv
from botocore.client import Config

# -----------------------------
# Load secrets from .env
# -----------------------------
load_dotenv()

# Kafka consumer settings
consumer = KafkaConsumer(
    'banking_server.public.customers',
    'banking_server.public.accounts',
    'banking_server.public.transactions',
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP"),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=os.getenv("KAFKA_GROUP"),
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)



# ...

# MinIO client
s3 = boto3.client(
    's3',
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
    config=Config(signature_version='s3v4')
)

bucket = os.getenv("MINIO_BUCKET")

# Create bucket if not exists
if bucket not in [b['Name'] for b in s3.list_buckets()['Buckets']]:
    s3.create_bucket(Bucket=bucket)

# Consume and write function
def write_to_minio(table_name, records):
    if not records:
        return
    df = pd.DataFrame(records)
    date_str = datetime.now().strftime('%Y-%m-%d')
    file_path = f'{table_name}_{date_str}.parquet'
    
    # Write parquet file locally
    df.to_parquet(file_path, engine='fastparquet', index=False)
    
    s3_key = f'{table_name}/date={date_str}/{table_name}_{datetime.now().strftime("%H%M%S%f")}.parquet'
    
    try:
        with open(file_path, 'rb') as data:
            s3.put_object(Body=data, Bucket=bucket, Key=s3_key)
        print(f'✅ Uploaded {len(records)} records to s3://{bucket}/{s3_key}')
    except Exception as e:
        print(f'❌ Failed to upload {s3_key}: {e}')
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)




# ... (rest of imports)

# Batch consume settings
batch_size = 50
flush_timeout = 10  # seconds
buffer = {
    'banking_server.public.customers': [],
    'banking_server.public.accounts': [],
    'banking_server.public.transactions': []
}
last_flush_time = {
    'banking_server.public.customers': time.time(),
    'banking_server.public.accounts': time.time(),
    'banking_server.public.transactions': time.time()
}

print("✅ Connected to Kafka. Listening for messages...")

try:
    while True:
        # Poll for new messages (wait up to 1 second)
        # returns a dict: {TopicPartition: [ConsumerRecord, ...]}
        msg_pack = consumer.poll(timeout_ms=1000)

        for tp, messages in msg_pack.items():
            for message in messages:
                topic = message.topic
                event = message.value
                payload = event.get("payload", {})
                record = payload.get("after")

                if record:
                    # Enrich record with metadata
                    source = payload.get("source", {})
                    record["lsn"] = source.get("lsn")
                    record["op"] = payload.get("op")
                    record["event_ts"] = payload.get("ts_ms")
                    
                    buffer[topic].append(record)
                    print(f"[{topic}] -> {record}")

        # Check conditions to flush
        current_time = time.time()
        for topic, records in buffer.items():
            # Flush if batch size reached OR timeout exceeded (and data exists)
            time_since_flush = current_time - last_flush_time[topic]
            
            if len(records) >= batch_size or (len(records) > 0 and time_since_flush >= flush_timeout):
                table_name = topic.split('.')[-1]
                write_to_minio(table_name, records)
                buffer[topic] = []
                last_flush_time[topic] = current_time

except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    consumer.close()