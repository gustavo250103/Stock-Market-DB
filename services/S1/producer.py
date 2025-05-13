from kafka import KafkaProducer
from faker import Faker
import json
import time
import random

fake = Faker()
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    api_version=(3, 9, 0),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

def generate_data():
    return {
        'sensor_id': str(random.randint(1, 50)),
        'temperature': round(random.uniform(-10.0, 40.0), 2),
        'timestamp': fake.date_time().isoformat()
    }

if __name__ == '__main__':
    topic = 'temperature-sensor-topic'

    while True:
        data = generate_data()
        key = data['sensor_id']
        producer.send(topic, key=key, value=data)
        print(f"Sent data: {data}")
        time.sleep(1)