from kafka import KafkaConsumer
import json
import os
from datetime import datetime
import socket
import threading
import time
import sys
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError as ESConnectionError

# Diretório para armazenar as solicitações e respostas
REQUESTS_DIR = 'requests'
if not os.path.exists(REQUESTS_DIR):
    os.makedirs(REQUESTS_DIR)

def wait_for_elasticsearch(max_retries=30, retry_interval=2):
    """Espera o Elasticsearch estar disponível"""
    for i in range(max_retries):
        try:
            es = Elasticsearch(['http://elasticsearch:9200'])
            if es.ping():
                print("Conexão com Elasticsearch estabelecida com sucesso!")
                return es
        except ESConnectionError:
            print(f"Tentativa {i+1}/{max_retries}: Elasticsearch ainda não está disponível. Aguardando...")
            time.sleep(retry_interval)
    
    raise Exception("Não foi possível conectar ao Elasticsearch após várias tentativas")

# Configuração do Elasticsearch
es = wait_for_elasticsearch()

# Configuração do servidor socket
HOST = '0.0.0.0'
PORT = 5000

# Configuração do Kafka
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
KAFKA_TOPIC = 'stock_data'

def save_request(request_data):
    """Salva a solicitação em um arquivo JSON"""
    try:
        request_id = request_data['request_id']
        filename = os.path.join(REQUESTS_DIR, f'request_{request_id}.json')
        
        request_info = {
            'request_id': request_id,
            'request_timestamp': datetime.now().isoformat(),
            'status': 'pending',
            'request': request_data
        }
        
        with open(filename, 'w') as f:
            json.dump(request_info, f, indent=2)
        
        print(f"[SOCKET] Request {request_id} saved successfully")
    except Exception as e:
        print(f"[SOCKET] Error saving request: {e}")
        print(f"[SOCKET] Request data: {request_data}")

def validate_response(request_data, response_data):
    """Valida se a resposta corresponde à solicitação"""
    try:
        if request_data['acao'] == 'inclusao':
            return response_data['resultado'] == 'sucesso'
        elif request_data['acao'] == 'requisicao':
            return 'dados' in response_data
        return False
    except Exception as e:
        print(f"[VALIDATION] Error validating response: {e}")
        return False

def update_request_with_response(request_id, response_data):
    """Atualiza o arquivo da solicitação com a resposta recebida"""
    try:
        filename = os.path.join(REQUESTS_DIR, f'request_{request_id}.json')
        
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                request_info = json.load(f)
            
            request_info['status'] = 'completed'
            request_info['response'] = response_data
            request_info['response_timestamp'] = datetime.now().isoformat()
            request_info['valid'] = validate_response(request_info['request'], response_data)
            
            with open(filename, 'w') as f:
                json.dump(request_info, f, indent=2)
            
            print(f"[KAFKA] Request {request_id} updated with response. Valid: {request_info['valid']}")
        else:
            print(f"[KAFKA] Warning: Request file not found for ID {request_id}")
    except Exception as e:
        print(f"[KAFKA] Error updating request with response: {e}")

def create_index_if_not_exists():
    """Cria o índice no Elasticsearch se não existir"""
    if not es.indices.exists(index="stock_data"):
        es.indices.create(
            index="stock_data",
            body={
                "mappings": {
                    "properties": {
                        "symbol": {"type": "keyword"},
                        "price": {"type": "float"},
                        "volume": {"type": "long"},
                        "timestamp": {"type": "date"},
                        "source": {"type": "keyword"}
                    }
                }
            }
        )

def handle_client(client_socket, addr):
    """Manipula a conexão com o cliente"""
    print(f"Conexão estabelecida com {addr}")
    try:
        while True:
            data = client_socket.recv(1024)
            if not data:
                break
            
            # Processa os dados recebidos
            try:
                message = json.loads(data.decode())
                print(f"Dados recebidos: {message}")
                
                # Verifica se os campos necessários existem
                if all(key in message for key in ['symbol', 'price', 'volume']):
                    # Envia os dados para o Elasticsearch
                    es.index(
                        index="stock_data",
                        document={
                            "symbol": message["symbol"],
                            "price": float(message["price"]),
                            "volume": int(message["volume"]),
                            "timestamp": message.get("timestamp", datetime.now().isoformat()),
                            "source": "socket"
                        }
                    )
                else:
                    print("Dados incompletos recebidos via socket")
                
            except json.JSONDecodeError:
                print("Erro ao decodificar JSON")
            except Exception as e:
                print(f"Erro ao processar dados: {e}")
                
    except Exception as e:
        print(f"Erro na conexão com {addr}: {e}")
    finally:
        client_socket.close()
        print(f"Conexão encerrada com {addr}")

def start_socket_server():
    """Inicia o servidor socket"""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen(5)
    print(f"Servidor socket iniciado em {HOST}:{PORT}")
    
    while True:
        client_socket, addr = server.accept()
        client_thread = threading.Thread(target=handle_client, args=(client_socket, addr))
        client_thread.start()

def consume_kafka_messages():
    """Consome mensagens do Kafka e envia para o Elasticsearch"""
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )
    
    print("Iniciando consumo de mensagens do Kafka...")
    
    for message in consumer:
        try:
            data = message.value
            print(f"Mensagem recebida do Kafka: {data}")
            
            # Verifica se os campos necessários existem
            if all(key in data for key in ['symbol', 'price', 'volume']):
                # Envia os dados para o Elasticsearch
                es.index(
                    index="stock_data",
                    document={
                        "symbol": data["symbol"],
                        "price": float(data["price"]),
                        "volume": int(data["volume"]),
                        "timestamp": data.get("timestamp", datetime.now().isoformat()),
                        "source": "kafka"
                    }
                )
            else:
                print("Dados incompletos recebidos do Kafka")
            
        except Exception as e:
            print(f"Erro ao processar mensagem do Kafka: {e}")

def main():
    try:
        # Cria o índice no Elasticsearch
        create_index_if_not_exists()
        
        # Inicia o servidor socket em uma thread separada
        socket_thread = threading.Thread(target=start_socket_server)
        socket_thread.daemon = True
        socket_thread.start()
        
        # Inicia o consumidor Kafka em uma thread separada
        kafka_thread = threading.Thread(target=consume_kafka_messages)
        kafka_thread.daemon = True
        kafka_thread.start()
        
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Encerrando o programa...")
    except Exception as e:
        print(f"Erro fatal: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

