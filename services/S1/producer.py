from kafka import KafkaProducer, KafkaConsumer
import json
import time
from faker import Faker
import random
from datetime import datetime
import sys
import os

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from kafka_config.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS, CONSUMER_GROUPS

fake = Faker()

class DataService:
    def __init__(self):
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Initialize Kafka consumer for responses
        self.consumer = KafkaConsumer(
            KAFKA_TOPICS['data_responses'],
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id='s1_consumer_group',
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

    def generate_stock_data(self):
        """Generate stock data for PostgreSQL"""
        return {
            'symbol': fake.random_element(elements=('AAPL', 'GOOGL', 'MSFT', 'AMZN', 'META')),
            'price': round(random.uniform(100, 1000), 2),
            'volume': random.randint(1000, 1000000),
            'timestamp': datetime.now().isoformat()
        }

    def generate_news_data(self):
        """Generate news data for MongoDB"""
        return {
            'title': fake.sentence(),
            'content': fake.text(),
            'source': fake.company(),
            'published_at': datetime.now().isoformat(),
            'sentiment': random.choice(['positive', 'negative', 'neutral'])
        }

    def generate_market_event(self):
        """Generate market event data for Cassandra"""
        return {
            'event_id': fake.uuid4(),
            'event_type': random.choice(['price_alert', 'volume_spike', 'market_open', 'market_close']),
            'description': fake.sentence(),
            'severity': random.choice(['low', 'medium', 'high']),
            'timestamp': datetime.now().isoformat()
        }

    def request_data(self, data_type, query_params=None):
        """Request data from the databases"""
        request_id = fake.uuid4()
        request = {
            'request_id': request_id,
            'data_type': data_type,
            'query_params': query_params or {},
            'timestamp': datetime.now().isoformat()
        }
        self.producer.send(KAFKA_TOPICS['data_requests'], request)
        self.producer.flush()
        print(f"Sent data request: {request}")
        return request_id

    def send_data(self):
        """Send generated data to respective topics"""
        while True:
            try:
                # Send stock data
                stock_data = self.generate_stock_data()
                self.producer.send(KAFKA_TOPICS['stock_data'], stock_data)
                print(f"Sent stock data: {stock_data}")

                # Send news data
                news_data = self.generate_news_data()
                self.producer.send(KAFKA_TOPICS['news_data'], news_data)
                print(f"Sent news data: {news_data}")

                # Send market event
                market_event = self.generate_market_event()
                self.producer.send(KAFKA_TOPICS['market_events'], market_event)
                print(f"Sent market event: {market_event}")

                # Flush messages
                self.producer.flush()
                
                # Wait for 5 seconds before sending next batch
                time.sleep(5)

            except Exception as e:
                print(f"Error occurred: {e}")
                time.sleep(5)

    def process_responses(self):
        """Process responses from data requests"""
        try:
            for message in self.consumer:
                response = message.value
                print(f"Received response: {response}")
        except Exception as e:
            print(f"Error processing response: {e}")

if __name__ == "__main__":
    import threading
    
    service = DataService()
    
    # Start data generation in a separate thread
    producer_thread = threading.Thread(target=service.send_data)
    producer_thread.daemon = True
    producer_thread.start()
    
    # Start response processing in a separate thread
    consumer_thread = threading.Thread(target=service.process_responses)
    consumer_thread.daemon = True
    consumer_thread.start()
    
    # Example of requesting data
    while True:
        try:
            # Request stock data for a specific symbol
            service.request_data('stock_data', {'symbol': 'AAPL'})
            time.sleep(10)
        except KeyboardInterrupt:
            break 