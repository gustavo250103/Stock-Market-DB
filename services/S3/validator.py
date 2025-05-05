from kafka import KafkaConsumer
import json
import sys
import os
from datetime import datetime
import os.path

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from kafka_config.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS, CONSUMER_GROUPS

class MessageValidator:
    def __init__(self):
        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUPS['S3'],
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        # Subscribe to all topics
        self.consumer.subscribe([
            KAFKA_TOPICS['stock_data'],
            KAFKA_TOPICS['news_data'],
            KAFKA_TOPICS['market_events'],
            KAFKA_TOPICS['data_requests'],
            KAFKA_TOPICS['data_responses']
        ])

        # Create logs directory if it doesn't exist
        self.logs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
        if not os.path.exists(self.logs_dir):
            os.makedirs(self.logs_dir)

    def log_message(self, topic, message, message_type):
        """Log message to file"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_file = os.path.join(self.logs_dir, f'{topic}_{datetime.now().strftime("%Y%m%d")}.log')
        
        with open(log_file, 'a') as f:
            f.write(f"[{timestamp}] [{message_type}] {json.dumps(message)}\n")

    def validate_message(self, topic, message):
        """Validate message structure based on topic"""
        try:
            if topic == KAFKA_TOPICS['stock_data']:
                required_fields = ['symbol', 'price', 'volume', 'timestamp']
                if not all(field in message for field in required_fields):
                    return False
                if not isinstance(message['price'], (int, float)):
                    return False
                if not isinstance(message['volume'], int):
                    return False
                return True

            elif topic == KAFKA_TOPICS['news_data']:
                required_fields = ['title', 'content', 'source', 'published_at', 'sentiment']
                if not all(field in message for field in required_fields):
                    return False
                if message['sentiment'] not in ['positive', 'negative', 'neutral']:
                    return False
                return True

            elif topic == KAFKA_TOPICS['market_events']:
                required_fields = ['event_id', 'event_type', 'description', 'severity', 'timestamp']
                if not all(field in message for field in required_fields):
                    return False
                if message['severity'] not in ['low', 'medium', 'high']:
                    return False
                return True

            elif topic == KAFKA_TOPICS['data_requests']:
                required_fields = ['request_id', 'data_type', 'timestamp']
                if not all(field in message for field in required_fields):
                    return False
                if message['data_type'] not in ['stock_data', 'news_data', 'market_events']:
                    return False
                return True

            elif topic == KAFKA_TOPICS['data_responses']:
                required_fields = ['request_id', 'data_type', 'timestamp']
                if not all(field in message for field in required_fields):
                    return False
                return True

            return False
        except Exception:
            return False

    def process_messages(self):
        """Process and validate messages"""
        try:
            for message in self.consumer:
                topic = message.topic
                data = message.value
                
                # Log the received message
                self.log_message(topic, data, 'RECEIVED')
                
                # Validate the message
                is_valid = self.validate_message(topic, data)
                
                # Log validation result
                validation_result = {
                    'message': data,
                    'is_valid': is_valid,
                    'timestamp': datetime.now().isoformat()
                }
                self.log_message(topic, validation_result, 'VALIDATION')
                
                # If it's a response, try to match it with its request
                if topic == KAFKA_TOPICS['data_responses']:
                    request_id = data['request_id']
                    request_log = os.path.join(self.logs_dir, f"{KAFKA_TOPICS['data_requests']}_{datetime.now().strftime('%Y%m%d')}.log")
                    if os.path.exists(request_log):
                        with open(request_log, 'r') as f:
                            for line in f:
                                if request_id in line:
                                    self.log_message('request_response_pairs', {
                                        'request_id': request_id,
                                        'request': json.loads(line.split('] ')[-1]),
                                        'response': data,
                                        'timestamp': datetime.now().isoformat()
                                    }, 'PAIR')
                                    break
                
                print(f"Processed message from {topic}. Valid: {is_valid}")
                
        except Exception as e:
            print(f"Error processing message: {e}")

if __name__ == "__main__":
    validator = MessageValidator()
    validator.process_messages() 