from kafka import KafkaProducer
from faker import Faker
import json
import time
import random
from datetime import datetime
import uuid
import socket
import threading

fake = Faker()
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    api_version=(3, 9, 0),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

def send_to_consumer(data):
    """Envia dados diretamente para o consumer via socket"""
    try:
        # Tenta várias vezes se houver erro de conexão
        max_retries = 5
        retry_interval = 2
        
        for i in range(max_retries):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect(('consumer', 5000))
                    s.sendall(json.dumps(data).encode('utf-8'))
                    return  # Sucesso, sai da função
            except Exception as e:
                if i < max_retries - 1:
                    print(f"Tentativa {i+1} falhou: {e}. Tentando novamente em {retry_interval} segundos...")
                    time.sleep(retry_interval)
                else:
                    print(f"Todas as tentativas falharam: {e}")
    except Exception as e:
        print(f"Erro ao enviar para consumer: {e}")

def generate_postgre_data():
    request_id = str(uuid.uuid4())
    symbol = fake.random_element(['PETR4', 'VALE3', 'ITUB4', 'BBDC4'])
    price = round(random.uniform(10.0, 100.0), 2)
    volume = random.randint(1000, 1000000)
    
    return {
        'request_id': request_id,
        'acao': 'inclusao',
        'banco': 'PostgreSQL',
        'tipo': 'ativo_financeiro',
        'dados': {
            'symbol': symbol,
            'price': price,
            'volume': volume,
            'variacao': round(random.uniform(-5.0, 5.0), 2),
            'timestamp': datetime.now().isoformat()
        }
    }

def generate_mongodb_data():
    request_id = str(uuid.uuid4())
    symbol = fake.random_element(['PETR4', 'VALE3', 'ITUB4', 'BBDC4'])
    price = round(random.uniform(10.0, 100.0), 2)
    volume = random.randint(1000, 1000000)
    
    return {
        'request_id': request_id,
        'acao': 'inclusao',
        'banco': 'MongoDB',
        'tipo': 'noticia',
        'dados': {
            'symbol': symbol,
            'price': price,
            'volume': volume,
            'titulo': fake.sentence(),
            'conteudo': fake.paragraph(),
            'fonte': fake.random_element(['Reuters', 'Bloomberg', 'Valor Econômico']),
            'data_publicacao': fake.date_time().isoformat()
        }
    }

def generate_cassandra_data():
    request_id = str(uuid.uuid4())
    symbol = fake.random_element(['PETR4', 'VALE3', 'ITUB4', 'BBDC4'])
    price = round(random.uniform(10.0, 100.0), 2)
    volume = random.randint(1000, 1000000)
    
    return {
        'request_id': request_id,
        'acao': 'inclusao',
        'banco': 'Cassandra',
        'tipo': 'analise_preditiva',
        'dados': {
            'symbol': symbol,
            'price': price,
            'volume': volume,
            'previsao_preco': round(random.uniform(10.0, 100.0), 2),
            'confianca': round(random.uniform(0.5, 0.95), 2),
            'horizonte_tempo': fake.random_element(['1d', '1w', '1m']),
            'timestamp': datetime.now().isoformat()
        }
    }

def generate_request_data():
    request_id = str(uuid.uuid4())
    symbol = fake.random_element(['PETR4', 'VALE3', 'ITUB4', 'BBDC4'])
    price = round(random.uniform(10.0, 100.0), 2)
    volume = random.randint(1000, 1000000)
    
    return {
        'request_id': request_id,
        'acao': 'requisicao',
        'dados': {
            'symbol': symbol,
            'price': price,
            'volume': volume,
            'sensor_id': str(random.randint(1, 50)),
            'temperature': round(random.uniform(-10.0, 40.0), 2),
            'timestamp': datetime.now().isoformat()
        }
    }

if __name__ == '__main__':
    topic = 'stock_data'  # Alterado para corresponder ao tópico esperado pelo consumer
    
    while True:
        # escolhe aleatoriamente entre os diferentes tipos de dados
        data_generators = [
            generate_postgre_data,
            generate_mongodb_data,
            generate_cassandra_data,
            generate_request_data
        ]
        
        data = random.choice(data_generators)()
        key = data['request_id']
        
        # Extrai os dados necessários para o Elasticsearch
        stock_data = {
            'symbol': data['dados']['symbol'],
            'price': data['dados']['price'],
            'volume': data['dados']['volume'],
            'timestamp': data['dados'].get('timestamp', datetime.now().isoformat())
        }
        
        producer.send(topic, key=key, value=stock_data)
        send_to_consumer(stock_data)
        print(f"Sent data: {stock_data}")
        time.sleep(2)