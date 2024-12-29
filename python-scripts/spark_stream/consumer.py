import json
from kafka import KafkaConsumer

class FlightKafkaConsumer:
    def __init__(self, kafka_config, topic):
        # Initialize KafkaConsumer
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_config['bootstrap.servers'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),  # Deserialize JSON data
            auto_offset_reset='earliest',  # Start reading at the earliest message
            enable_auto_commit=True,  # Automatically commit offsets
            group_id='flight-consumer-group'  # Consumer group ID
        )
    
    def consume_flight_data(self):
        # Consume messages from Kafka topic
        print("Listening for flight data...")
        for message in self.consumer:
            flight_data = message.value
            print(f"Received flight data: {flight_data}")

def main():
    # Kafka configuration
    kafka_config = {
        'bootstrap.servers': 'kafka:29092',  # Address of Kafka broker
    }
    
    # Create a consumer instance
    consumer = FlightKafkaConsumer(kafka_config, 'flights')
    
    # Start consuming flight data
    consumer.consume_flight_data()

if __name__ == "__main__":
    main()