from confluent_kafka import Producer
import json
import logging
from config import Config
from file_splitter import FileSplitter

class DataProducer:
    def __init__(self):
        self.producer = Producer({
            'bootstrap.servers': ','.join(Config.KAFKA_BOOTSTRAP_SERVERS),
            'message.max.bytes': 5242880,  # 5MB
            'queue.buffering.max.messages': 1000000,  # Increase queue size
            'queue.buffering.max.kbytes': 1048576,    # 1GB
            'compression.type': 'gzip'                # Add compression
        })
    
    def delivery_report(self, err, msg):
        if err is not None:
            logging.error(f'Message delivery failed: {err}')
        else:
            logging.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def produce_chunks(self, input_file):
        try:
            for chunk_number, chunk_data in FileSplitter.split_file(input_file):
                message = {
                    'chunk_number': chunk_number,
                    'chunk_data': chunk_data.decode('utf-8', errors='ignore'),
                    'total_size': len(chunk_data)
                }
                
                while True:
                    try:
                        self.producer.produce(
                            Config.KAFKA_TOPIC,
                            value=json.dumps(message).encode('utf-8'),
                            callback=self.delivery_report
                        )
                        # Poll more frequently to prevent queue buildup
                        self.producer.poll(0.1)
                        break
                    except BufferError:
                        # Queue is full, wait and retry
                        logging.warning("Queue full, waiting...")
                        self.producer.poll(1.0)
                
                logging.info(f"Sent chunk {chunk_number} to Kafka")
            
            # Ensure all messages are delivered
            remaining = self.producer.flush(timeout=30)
            if remaining > 0:
                logging.warning(f"{remaining} messages were not delivered")
            else:
                logging.info("All chunks sent to Kafka successfully")
        
        except Exception as e:
            logging.error(f"Error in producing chunks: {e}")