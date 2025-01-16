import requests
import pandas as pd
from datetime import datetime
import time
import json
from kafka import KafkaProducer

class FlightProcessor:
    def __init__(self, username=None, password=None):
        self.auth = (username, password) if username and password else None
        self.base_url = "https://opensky-network.org/api"
    
    def get_flights(self, begin_time, end_time):
        """
        Get flight data from OpenSky API (global flights)
        """
        endpoint = f"{self.base_url}/flights/all"
        params = {
            "begin": begin_time,
            "end": end_time
        }
        
        try:
            print(f"Querying OpenSky API for period: {datetime.fromtimestamp(begin_time)} to {datetime.fromtimestamp(end_time)}")
            response = requests.get(endpoint, params=params, auth=self.auth, timeout=30)
            response.raise_for_status()
            flights = response.json()
            return self.process_flights(flights)
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return pd.DataFrame()

    def process_flights(self, flights):
        """
        Process raw API data into DataFrame with required fields
        """
        processed_data = []
        for flight in flights:
            try:
                dep_time = datetime.fromtimestamp(flight.get('firstSeen', 0))
                arr_time = datetime.fromtimestamp(flight.get('lastSeen', 0))
                
                flight_data = {
                    'UniqueCarrier': flight.get('callsign', '').strip()[:3],
                    'Origin': flight.get('estDepartureAirport', ''),
                    'Dest': flight.get('estArrivalAirport', ''),
                    'Month': dep_time.month,
                    'DayofMonth': dep_time.day,
                    'FlightNum': flight.get('callsign', '').strip()[3:],
                    'CRSDepTime': dep_time.strftime('%H%M'),
                    'Distance': flight.get('estDepartureAirportHorizDistance', 0),
                    'CRSArrTime': arr_time.strftime('%H%M'),
                    'Diverted': 1 if flight.get('diverted', False) else 0,
                    'Cancelled': 0,
                    'RouteType': 'Domestic' if flight.get('estDepartureAirport', '')[:2] == flight.get('estArrivalAirport', '')[:2] else 'International'
                }
                processed_data.append(flight_data)
            except Exception as e:
                print(f"Error processing flight: {e}")
                continue
        return pd.DataFrame(processed_data)

class FlightKafkaProducer:
    def __init__(self, kafka_config, topic):
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_config['bootstrap.servers'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = topic
    
    def send_flight_data(self, flight_data):
        self.producer.send(self.topic, flight_data)
        time.sleep(2)
        self.producer.flush()
        print("Flight data sent to Kafka")

def main():
    processor = FlightProcessor()
    end_time = int(time.time())
    begin_time = end_time - 2 * 3600  # Get data for the last 2 hours
    
    print("\nFetching global flight data...")
    data = processor.get_flights(begin_time, end_time)
    
    if not data.empty:
        kafka_config = {
            'bootstrap.servers': 'kafka1:29092',
        }
        kafka_producer = FlightKafkaProducer(kafka_config, 'flights')
        
        for _, flight_data in data.iterrows():
            flight_json = flight_data.to_dict()
            kafka_producer.send_flight_data(flight_json)
            print(flight_json)
    
if __name__ == "__main__":
    main()