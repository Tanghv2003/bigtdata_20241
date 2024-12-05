from kafka import KafkaProducer
import time

# Khởi tạo Kafka producer, kết nối tới broker Kafka
producer = KafkaProducer(bootstrap_servers='kafka:29092')

# Gửi 5 thông điệp vào topic 'test_topic'
for i in range(100):
    message = f"Message {i}".encode('utf-8')  # Chuyển đổi chuỗi thành bytes
    producer.send('abcd', value=message)  # Gửi thông điệp vào topic
    print(f"Sent: Message {i}")
    time.sleep(1)  # Dừng 1 giây giữa các lần gửi

# Đảm bảo tất cả thông điệp đã được gửi
producer.flush()

# Đóng kết nối producer
producer.close()
