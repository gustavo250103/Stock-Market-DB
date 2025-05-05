from kafka import KafkaConsumer, KafkaProducer
import json
import sys
import os
from datetime import datetime
import psycopg2
from pymongo import MongoClient
from cassandra.cluster import Cluster

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from kafka_config.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS, CONSUMER_GROUPS

class DataService:
    def __init__(self):
        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUPS['S2'],
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        # Initialize Kafka producer for responses
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Subscribe to topics
        self.consumer.subscribe([
            KAFKA_TOPICS['stock_data'],
            KAFKA_TOPICS['news_data'],
            KAFKA_TOPICS['market_events'],
            KAFKA_TOPICS['data_requests']
        ])

        # Initialize database connections
        self.setup_postgres()
        self.setup_mongodb()
        self.setup_cassandra()

    def setup_postgres(self):
        """Setup PostgreSQL connection"""
        self.pg_conn = psycopg2.connect(
            dbname="stock_market",
            user="postgres",
            password="postgres",
            host="localhost",
            port="5432"
        )
        self.pg_cursor = self.pg_conn.cursor()
        
        # Create table if not exists
        self.pg_cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_data (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(10),
                price DECIMAL(10,2),
                volume INTEGER,
                timestamp TIMESTAMP
            )
        """)
        self.pg_conn.commit()

    def setup_mongodb(self):
        """Setup MongoDB connection"""
        self.mongo_client = MongoClient('mongodb://localhost:27017/')
        self.mongo_db = self.mongo_client['stock_market']
        self.news_collection = self.mongo_db['news']

    def setup_cassandra(self):
        """Setup Cassandra connection"""
        self.cassandra_cluster = Cluster(['localhost'])
        self.cassandra_session = self.cassandra_cluster.connect('stock_market')
        
        # Create keyspace and table if not exists
        self.cassandra_session.execute("""
            CREATE KEYSPACE IF NOT EXISTS stock_market
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """)
        
        self.cassandra_session.execute("""
            CREATE TABLE IF NOT EXISTS market_events (
                event_id UUID PRIMARY KEY,
                event_type TEXT,
                description TEXT,
                severity TEXT,
                timestamp TIMESTAMP
            )
        """)

    def process_stock_data(self, data):
        """Store stock data in PostgreSQL"""
        self.pg_cursor.execute("""
            INSERT INTO stock_data (symbol, price, volume, timestamp)
            VALUES (%s, %s, %s, %s)
        """, (data['symbol'], data['price'], data['volume'], data['timestamp']))
        self.pg_conn.commit()

    def process_news_data(self, data):
        """Store news data in MongoDB"""
        self.news_collection.insert_one(data)

    def process_market_event(self, data):
        """Store market event in Cassandra"""
        self.cassandra_session.execute("""
            INSERT INTO market_events (event_id, event_type, description, severity, timestamp)
            VALUES (%s, %s, %s, %s, %s)
        """, (data['event_id'], data['event_type'], data['description'], 
              data['severity'], data['timestamp']))

    def handle_data_request(self, request):
        """Handle data retrieval requests"""
        try:
            data_type = request['data_type']
            query_params = request.get('query_params', {})
            response = {
                'request_id': request['request_id'],
                'data_type': data_type,
                'timestamp': datetime.now().isoformat()
            }

            if data_type == 'stock_data':
                # Query PostgreSQL
                self.pg_cursor.execute("""
                    SELECT * FROM stock_data 
                    WHERE symbol = %s 
                    ORDER BY timestamp DESC 
                    LIMIT 10
                """, (query_params.get('symbol'),))
                response['data'] = [dict(zip(['id', 'symbol', 'price', 'volume', 'timestamp'], row)) 
                                  for row in self.pg_cursor.fetchall()]

            elif data_type == 'news_data':
                # Query MongoDB
                query = {}
                if 'source' in query_params:
                    query['source'] = query_params['source']
                response['data'] = list(self.news_collection.find(query).limit(10))

            elif data_type == 'market_events':
                # Query Cassandra
                query = "SELECT * FROM market_events"
                if 'event_type' in query_params:
                    query += " WHERE event_type = %s"
                    rows = self.cassandra_session.execute(query, (query_params['event_type'],))
                else:
                    rows = self.cassandra_session.execute(query)
                response['data'] = [dict(row) for row in rows]

            # Send response
            self.producer.send(KAFKA_TOPICS['data_responses'], response)
            self.producer.flush()
            print(f"Sent response: {response}")

        except Exception as e:
            error_response = {
                'request_id': request['request_id'],
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
            self.producer.send(KAFKA_TOPICS['data_responses'], error_response)
            self.producer.flush()
            print(f"Error processing request: {e}")

    def consume_messages(self):
        """Consume and process messages from Kafka"""
        try:
            for message in self.consumer:
                topic = message.topic
                data = message.value
                
                print(f"Received message from topic {topic}: {data}")
                
                if topic == KAFKA_TOPICS['stock_data']:
                    self.process_stock_data(data)
                elif topic == KAFKA_TOPICS['news_data']:
                    self.process_news_data(data)
                elif topic == KAFKA_TOPICS['market_events']:
                    self.process_market_event(data)
                elif topic == KAFKA_TOPICS['data_requests']:
                    self.handle_data_request(data)
                
        except Exception as e:
            print(f"Error processing message: {e}")
        finally:
            self.cleanup()

    def cleanup(self):
        """Close all connections"""
        self.pg_cursor.close()
        self.pg_conn.close()
        self.mongo_client.close()
        self.cassandra_cluster.shutdown()

if __name__ == "__main__":
    service = DataService()
    service.consume_messages() 