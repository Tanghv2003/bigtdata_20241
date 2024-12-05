from confluent_kafka import Consumer
import json
import logging
from hdfs import InsecureClient
from config import Config

class DataConsumer:
    def __init__(self):
        self.consumer = Consumer({
            'bootstrap.servers': ','.join(Config.KAFKA_BOOTSTRAP_SERVERS),
            'group.id': 'airline-data-group',
            'auto.offset.reset': 'earliest',
            'max.partition.fetch.bytes': 5242880  # Match producer message size
        })
        self.consumer.subscribe([Config.KAFKA_TOPIC])
        
        self.hdfs_client = InsecureClient('http://localhost:9870', user='hduser')
    
    def consume_and_store(self):
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    logging.error(f"Consumer error: {msg.error()}")
                    continue

                chunk_data = json.loads(msg.value().decode('utf-8'))
                hdfs_path = f"{Config.HDFS_OUTPUT_PATH.rstrip('/')}/chunk_{chunk_data['chunk_number']}.csv"
                
                try:
                    # Kiểm tra xem file đã tồn tại chưa
                    if self.hdfs_client.status(hdfs_path, strict=False) is not None:
                        # Nếu file đã tồn tại, xóa nó trước khi ghi mới
                        self.hdfs_client.delete(hdfs_path)
                        logging.info(f"Deleted existing file: {hdfs_path}")
                    
                    # Ghi file mới
                    with self.hdfs_client.write(hdfs_path) as writer:
                        writer.write(chunk_data['chunk_data'].encode('utf-8'))
                    logging.info(f"Stored chunk {chunk_data['chunk_number']} in HDFS at {hdfs_path}")
                except Exception as e:
                    logging.error(f"HDFS write error for chunk {chunk_data['chunk_number']}: {e}")
        
        except Exception as e:
            logging.error(f"Error in consuming and storing chunks: {e}")
        finally:
            self.consumer.close()