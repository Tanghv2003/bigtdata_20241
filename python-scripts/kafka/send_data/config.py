import os

class Config:
    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS = ['kafka:29092']
    KAFKA_TOPIC = 'airline_data_topic'
    
    # File Configuration
    INPUT_FILE = '../..airline.csv.shuffle'
    CHUNK_SIZE = 1024 * 1024  # 1MB chunks
    
    # Hadoop Configuration
    HDFS_OUTPUT_PATH = '/user/hduser/data/airline/'
    
    # Logging
    LOG_FILE = 'data_pipeline.log'