import logging
from config import Config
from kafka_producer import DataProducer
from kafka_consumer import DataConsumer
import multiprocessing

def setup_logging():
    """Configure logging for the application"""
    logging.basicConfig(
        filename=Config.LOG_FILE,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s: %(message)s'
    )

def run_producer():
    """Run Kafka Producer"""
    producer = DataProducer()
    producer.produce_chunks(Config.INPUT_FILE)

def run_consumer():
    """Run Kafka Consumer"""
    consumer = DataConsumer()
    consumer.consume_and_store()

def main():
    setup_logging()
    
    # Create multiprocessing for producer and consumer
    producer_process = multiprocessing.Process(target=run_producer)
    consumer_process = multiprocessing.Process(target=run_consumer)
    
    producer_process.start()
    consumer_process.start()
    
    producer_process.join()
    consumer_process.join()

if __name__ == '__main__':
    main()