from kafka import KafkaProducer
import time
from datetime import datetime

# Initialize Kafka producer with multiple brokers for redundancy
producer = KafkaProducer(
    bootstrap_servers=['kafka1:29092', 'kafka2:29093'],
    acks='all'  # Wait for all replicas to acknowledge the message
)

def send_message(msg, topic='abcd'):
    try:
        future = producer.send(topic, value=msg.encode('utf-8'))
        # Wait for the message to be sent
        record_metadata = future.get(timeout=10)
        print(f"""
        Message: {msg}
        Topic: {record_metadata.topic}
        Partition: {record_metadata.partition}
        Offset: {record_metadata.offset}
        Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        """)
    except Exception as e:
        print(f"Error sending message: {str(e)}")

try:
    # Send 100 messages
    for i in range(100):
        message = f"Message {i}"
        send_message(message)
        time.sleep(1)  # Wait 1 second between messages

except KeyboardInterrupt:
    print("Stopping the producer...")
finally:
    # Ensure all messages are sent and clean up
    print("Flushing and closing producer...")
    producer.flush()
    producer.close()
