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
    key_serializer=lambda k: str(k).encode('utf-8') if k is not None else None
)

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
    # Gera uma requisição de pesquisa com dados específicos para cada banco
    id_requisicao = str(uuid.uuid4())
    banco = fake.random_element(['PostgreSQL', 'MongoDB', 'Cassandra'])
    
    # Dados base comuns
    dados_base = {
        'data_inicio': (datetime.now() - timedelta(days=7)).isoformat(),
        'data_fim': datetime.now().isoformat()
    }
    
    # Dados específicos para cada banco
    if banco == 'PostgreSQL':
        dados_base.update({
            'codigo': fake.random_element(['PETR4', 'VALE3', 'ITUB4', 'BBDC4']),
            'preco': round(random.uniform(10.0, 100.0), 2),
            'volume': random.randint(1000, 1000000)
        })
    elif banco == 'MongoDB':
        dados_base.update({
            'symbol': fake.random_element(['PETR4', 'VALE3', 'ITUB4', 'BBDC4']),
            'fonte': fake.random_element(['Reuters', 'Bloomberg', 'Valor Econômico'])
        })
    elif banco == 'Cassandra':
        dados_base.update({
            'ativo': fake.random_element(['PETR4', 'VALE3', 'ITUB4', 'BBDC4']),
            'horizonte_tempo': fake.random_element(['1d', '1w', '1m'])
        })
    
    return {
        'request_id': id_requisicao,
        'acao': 'pesquisa',
        'banco': banco,
        'dados': dados_base
    }

if __name__ == '__main__':
    topico = 'topico-requisicoes'
    
    while True:
        # Primeiro escolhe entre inclusão e pesquisa (50% de chance para cada)
        tipo_operacao = random.choice(['inclusao', 'pesquisa'])
        
        if tipo_operacao == 'inclusao':
            # Se for inclusão, escolhe aleatoriamente qual banco (33.33% de chance para cada)
            banco = random.choice(['PostgreSQL', 'MongoDB', 'Cassandra'])
            if banco == 'PostgreSQL':
                dados = gerar_insercao_postgre()
            elif banco == 'MongoDB':
                dados = gerar_insercao_mongodb()
            else:  # Cassandra
                dados = gerar_insercao_cassandra()
        else:  # pesquisa
            dados = gerar_requisicao_pesquisa()
        
        chave = dados['request_id']  # Já é string do UUID
        
        producer.send(topico, key=chave, value=dados)
        print(f"Dados enviados: {dados}")
        time.sleep(2)