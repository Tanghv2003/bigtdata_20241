from kafka import KafkaConsumer

# Khởi tạo Kafka consumer, kết nối tới broker Kafka và subscribe vào topic 'test_topic'
consumer = KafkaConsumer('abcd', bootstrap_servers='kafka:29092', auto_offset_reset='earliest')

# Lắng nghe và xử lý thông điệp từ topic
for message in consumer:
    print(f"Received: {message.value.decode('utf-8')}")  # Giải mã bytes thành chuỗi
    # Dừng sau khi nhận 5 thông điệp
    if message.offset == 4:
        break

# Đóng kết nối consumer
consumer.close()
