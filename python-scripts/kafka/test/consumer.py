from kafka import KafkaConsumer
import json
from datetime import datetime

# Initialize consumer with both brokers
consumer = KafkaConsumer(
    'abcd',
    bootstrap_servers=['kafka1:29092', 'kafka2:29093'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group'
)

try:
    for message in consumer:
        print(f"""
        Topic: {message.topic}
        Partition: {message.partition}
        Offset: {message.offset}
        Key: {message.key}
        Value: {message.value.decode('utf-8')}
        Timestamp: {datetime.fromtimestamp(message.timestamp/1000.0)}
        """)

except KeyboardInterrupt:
    print("Stopping the consumer...")
finally:
    consumer.close()