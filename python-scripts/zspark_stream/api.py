import requests
import pandas as pd
from datetime import datetime, timedelta
import time

class FlightProcessor:
    def __init__(self, username=None, password=None):
        self.auth = None
        if username and password:
            self.auth = (username, password)
        self.base_url = "https://opensky-network.org/api"
        self.data = None
    
    def validate_time_range(self, begin_time, end_time):
        """
        Validate and adjust time range if necessary
        Returns tuple of (begin_time, end_time) in Unix timestamp format
        """
        current_time = int(time.time())
        
        # Convert to int if passed as float
        begin_time = int(begin_time)
        end_time = int(end_time)
        
        # Ensure the time range is not in the future
        if end_time > current_time:
            end_time = current_time
            
        # Ensure begin_time is before end_time
        if begin_time >= end_time:
            begin_time = end_time - 3600  # Default to 1 hour before end_time
            
        # OpenSky API limitation: maximum time window is 7 days
        max_window = 7 * 24 * 3600  # 7 days in seconds
        if (end_time - begin_time) > max_window:
            begin_time = end_time - max_window
            
        return begin_time, end_time
        
    def get_flights(self, begin_time, end_time):
        """
        Get flight data from OpenSky API (global flights)
        """
        # Validate and adjust time range
        begin_time, end_time = self.validate_time_range(begin_time, end_time)
        
        endpoint = f"{self.base_url}/flights/all"
        params = {
            "begin": begin_time,
            "end": end_time
        }
        
        try:
            print(f"Querying OpenSky API for period: {datetime.fromtimestamp(begin_time)} to {datetime.fromtimestamp(end_time)}")
            response = requests.get(endpoint, params=params, auth=self.auth, timeout=30)
            
            if response.status_code == 429:
                print("Rate limit exceeded. Waiting 10 seconds before retry...")
                time.sleep(10)
                response = requests.get(endpoint, params=params, auth=self.auth, timeout=30)
                
            response.raise_for_status()
            flights = response.json()
            
            if not flights:
                print("No flights found for the specified period")
                return pd.DataFrame()
                
            return self.process_flights(flights)
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            print(f"Failed URL: {endpoint}?begin={begin_time}&end={end_time}")
            return pd.DataFrame()

    def process_flights(self, flights):
        """
        Process raw API data into DataFrame with required fields
        """
        if not flights:
            return pd.DataFrame()
            
        processed_data = []
        
        for flight in flights:
            try:
                dep_time = datetime.fromtimestamp(flight.get('firstSeen', 0))
                arr_time = datetime.fromtimestamp(flight.get('lastSeen', 0))
                
                dep_delay = 0
                if flight.get('scheduledDepartureTime'):
                    scheduled_dep = datetime.fromtimestamp(flight['scheduledDepartureTime'])
                    dep_delay = (dep_time - scheduled_dep).total_seconds() / 60
                
                origin = flight.get('estDepartureAirport', '')
                dest = flight.get('estArrivalAirport', '')
                
                flight_data = {
                    'UniqueCarrier': flight.get('callsign', '').strip()[:3],
                    'Origin': origin,
                    'Dest': dest,
                    'Month': dep_time.month,
                    'DayofMonth': dep_time.day,
                    'FlightNum': flight.get('callsign', '').strip()[3:],
                    'CRSDepTime': dep_time.strftime('%H%M'),
                    'DepDelay': dep_delay,
                    'Distance': flight.get('estDepartureAirportHorizDistance', 0),
                    'CRSArrTime': arr_time.strftime('%H%M'),
                    'Diverted': 1 if flight.get('diverted', False) else 0,
                    'Cancelled': 0, # mặc định trả về là 0
                    'RouteType': 'Domestic' if origin[:2] == dest[:2] else 'International'
                }
                processed_data.append(flight_data)
            except Exception as e:
                print(f"Error processing flight: {e}")
                continue
            
        self.data = pd.DataFrame(processed_data)
        return self.data
    
    def print_flights(self):
        """
        Print the flight data
        """
        if self.data is not None and not self.data.empty:
            print(self.data)
        else:
            print("No data available to display")
    
    def export_to_csv(self, filename):
        """
        Export data to CSV file
        """
        if self.data is not None and not self.data.empty:
            self.data.to_csv(filename, index=False)
            print(f"Data exported to {filename}")
            return True
        print("No data available to export")
        return False

def main():
    processor = FlightProcessor()
    end_time = int(time.time())
    begin_time = end_time - 2 * 3600  
    
    print("\nFetching global flight data...")
    data = processor.get_flights(begin_time, end_time)
    processor.print_flights()
    processor.export_to_csv('global_flights.csv')  # Save to CSV
    

if __name__ == "__main__":
    main()
