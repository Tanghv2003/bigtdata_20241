from kafka import KafkaConsumer
from hdfs import InsecureClient
import logging

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Khởi tạo HDFS client
hdfs_client = InsecureClient('http://namenode:9870', user='root')  # Cập nhật đúng địa chỉ NameNode

# Cấu hình Kafka consumer
consumer = KafkaConsumer('abcd', bootstrap_servers='kafka:29092', auto_offset_reset='earliest')

# Đường dẫn tới file trên HDFS
hdfs_file_path = '/user/kafka_data.txt'

# Kiểm tra nếu file chưa tồn tại trên HDFS, nếu không có thì tạo file rỗng
if not hdfs_client.status(hdfs_file_path, strict=False):
    logger.info(f"File {hdfs_file_path} does not exist. Creating new file...")
    with hdfs_client.write(hdfs_file_path, encoding='utf-8') as writer:
        writer.write('')  # Tạo file trống nếu chưa có

# Mở file trên HDFS để ghi dữ liệu (append mode)
with hdfs_client.write(hdfs_file_path, append=True, encoding='utf-8') as writer:
    try:
        logger.info(f"Started consuming messages from Kafka topic 'abcd'.")

        # Lắng nghe và xử lý thông điệp từ Kafka topic 'abcd'
        for message in consumer:
            # Giải mã thông điệp nhận được từ Kafka và ghi vào HDFS
            decoded_message = message.value.decode('utf-8')
            logger.info(f"Received message: {decoded_message}")
            
            # Ghi thông điệp vào file HDFS
            writer.write(decoded_message + '\n')  # Thêm dòng mới vào file trên HDFS
            
            logger.info(f"Message '{decoded_message}' written to HDFS.")

            # Dừng sau khi nhận 5 thông điệp (tùy chỉnh số lượng)
            if message.offset == 4:
                logger.info("Received 5 messages, stopping consumer.")
                break

    except KeyboardInterrupt:
        logger.info("Consuming interrupted by user.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        # Đảm bảo đóng kết nối consumer một cách an toàn
        logger.info("Closing Kafka consumer.")
        consumer.close()

consumer.close()
