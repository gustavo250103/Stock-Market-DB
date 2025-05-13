from kafka import KafkaProducer
from faker import Faker
import json
import time
import random
from datetime import datetime

fake = Faker()
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    api_version=(3, 9, 0),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

def generate_postgre_data():
    return {
        'acao': 'inclusao',
        'banco': 'PostgreSQL',
        'tipo': 'ativo_financeiro',
        'dados': {
            'codigo': fake.random_element(['PETR4', 'VALE3', 'ITUB4', 'BBDC4']),
            'preco': round(random.uniform(10.0, 100.0), 2),
            'volume': random.randint(1000, 1000000),
            'variacao': round(random.uniform(-5.0, 5.0), 2),
            'timestamp': datetime.now().isoformat()
        }
    }

def generate_mongodb_data():
    return {
        'acao': 'inclusao',
        'banco': 'MongoDB',
        'tipo': 'noticia',
        'dados': {
            'titulo': fake.sentence(),
            'conteudo': fake.paragraph(),
            'fonte': fake.random_element(['Reuters', 'Bloomberg', 'Valor Econômico']),
            'data_publicacao': fake.date_time().isoformat(),
            'ativo_relacionado': fake.random_element(['PETR4', 'VALE3', 'ITUB4', 'BBDC4'])
        }
    }

def generate_cassandra_data():
    return {
        'acao': 'inclusao',
        'banco': 'Cassandra',
        'tipo': 'analise_preditiva',
        'dados': {
            'ativo': fake.random_element(['PETR4', 'VALE3', 'ITUB4', 'BBDC4']),
            'previsao_preco': round(random.uniform(10.0, 100.0), 2),
            'confianca': round(random.uniform(0.5, 0.95), 2),
            'horizonte_tempo': fake.random_element(['1d', '1w', '1m']),
            'timestamp': datetime.now().isoformat()
        }
    }

def generate_request_data():
    return {
        'acao': 'requisicao',
        'sensor_id': str(random.randint(1, 50)),
        'temperature': round(random.uniform(-10.0, 40.0), 2),
        'timestamp': fake.date_time().isoformat()
    }

if __name__ == '__main__':
    topic = 'topico-requisicoes'
    
    while True:
        # escolhe aleatoriamente entre os diferentes tipos de dados
        data_generators = [
            generate_postgre_data,
            generate_mongodb_data,
            generate_cassandra_data,
            generate_request_data
        ]
        
        data = random.choice(data_generators)()
        key = str(random.randint(1, 1000)) 
        producer.send(topic, key=key, value=data)
        print(f"Sent data: {data}")
        time.sleep(1)