from kafka import KafkaProducer
from faker import Faker
import json
import time
import random
from datetime import datetime, timedelta
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
    # Envia dados diretamente para o consumer via socket
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

def gerar_insercao_postgre():
    #Gera dados aleatórios para inserir no PostgreSQL
    id_requisicao = str(uuid.uuid4())
    simbolo = fake.random_element(['PETR4', 'VALE3', 'ITUB4', 'BBDC4'])
    preco = round(random.uniform(10.0, 100.0), 2)
    volume = random.randint(1000, 1000000)
    
    return {
        'request_id': id_requisicao,
        'acao': 'inclusao',
        'banco': 'PostgreSQL',
        'dados': {
            'codigo': simbolo,
            'preco': preco,
            'volume': volume,
            'variacao': round(random.uniform(-5.0, 5.0), 2),
            'timestamp': datetime.now().isoformat()
        }
    }

def gerar_insercao_mongodb():
    # Gera dados aleatórios para inserir no MongoDB
    id_requisicao = str(uuid.uuid4())
    simbolo = fake.random_element(['PETR4', 'VALE3', 'ITUB4', 'BBDC4'])
    preco = round(random.uniform(10.0, 100.0), 2)
    volume = random.randint(1000, 1000000)
    
    return {
        'request_id': id_requisicao,
        'acao': 'inclusao',
        'banco': 'MongoDB',
        'dados': {
            'symbol': simbolo,
            'price': preco,
            'volume': volume,
            'titulo': fake.sentence(),
            'conteudo': fake.paragraph(),
            'fonte': fake.random_element(['Reuters', 'Bloomberg', 'Valor Econômico']),
            'data_publicacao': datetime.now().isoformat()
        }
    }

def gerar_insercao_cassandra():
    # Gera dados aleatórios para inserir no Cassandra
    id_requisicao = str(uuid.uuid4())
    simbolo = fake.random_element(['PETR4', 'VALE3', 'ITUB4', 'BBDC4'])
    preco = round(random.uniform(10.0, 100.0), 2)
    volume = random.randint(1000, 1000000)
    
    return {
        'request_id': id_requisicao,
        'acao': 'inclusao',
        'banco': 'Cassandra',
        'dados': {
            'ativo': simbolo,
            'previsao_preco': round(random.uniform(10.0, 100.0), 2),
            'confianca': round(random.uniform(0.5, 0.95), 2),
            'horizonte_tempo': fake.random_element(['1d', '1w', '1m']),
            'timestamp': datetime.now().isoformat()
        }
    }

def gerar_requisicao_pesquisa():
    # Gera uma requisição de pesquisa sem especificar o banco
    id_requisicao = str(uuid.uuid4())
    simbolo = fake.random_element(['PETR4', 'VALE3', 'ITUB4', 'BBDC4'])
    
    return {
        'request_id': id_requisicao,
        'acao': 'pesquisa',
        'dados': {
            'symbol': simbolo,
            'data_inicio': (datetime.now() - timedelta(days=7)).isoformat(),
            'data_fim': datetime.now().isoformat()
        }
    }

if __name__ == '__main__':
    topico = 'topico-requisicoes'
    
    while True:
        # Escolhe aleatoriamente qual tipo de dado vai gerar
        geradores = [
            gerar_insercao_postgre,
            gerar_insercao_mongodb,
            gerar_insercao_cassandra,
            gerar_requisicao_pesquisa
        ]
        
        dados = random.choice(geradores)()
        chave = dados['request_id']
        
        producer.send(topico, key=chave, value=dados)
        send_to_consumer(dados)
        print(f"Dados enviados: {dados}")
        time.sleep(2)