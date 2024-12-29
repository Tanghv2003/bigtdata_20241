from kafka import KafkaConsumer


consumer = KafkaConsumer('flights', bootstrap_servers='kafka:29092', auto_offset_reset='earliest')

for message in consumer:
    print(f"Received: {message.value.decode('utf-8')}")  
    # Dừng sau khi nhận 5 thông điệp
    if message.offset == 4:
        break

# Đóng kết nối consumer
consumer.close()
