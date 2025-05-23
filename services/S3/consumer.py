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

# Pasta onde vamos guardar as requisições e respostas
PASTA_REQUISICOES = 'requests'
if not os.path.exists(PASTA_REQUISICOES):
    os.makedirs(PASTA_REQUISICOES)

def esperar_elasticsearch(max_tentativas=30, intervalo=2):
    # Fica esperando o Elasticsearch ficar pronto
    for i in range(max_tentativas):
        try:
            es = Elasticsearch(['http://elasticsearch:9200'])
            if es.ping():
                print("Conectamos no Elasticsearch com sucesso!")
                return es
        except ESConnectionError:
            print(f"Tentativa {i+1}/{max_tentativas}: Elasticsearch ainda não está pronto. Aguardando...")
            time.sleep(intervalo)
    
    raise Exception("Não conseguimos conectar no Elasticsearch depois de várias tentativas")

# Configuração do Elasticsearch
es = esperar_elasticsearch()

# Configuração do servidor socket
HOST = '0.0.0.0'
PORT = 5000

# Configuração do Kafka
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
KAFKA_TOPIC = 'stock_data'

def salvar_requisicao(dados_requisicao):
    """Salva a requisição em um arquivo JSON"""
    try:
        id_requisicao = dados_requisicao['request_id']
        nome_arquivo = os.path.join(PASTA_REQUISICOES, f'request_{id_requisicao}.json')
        
        info_requisicao = {
            'request_id': id_requisicao,
            'request_timestamp': datetime.now().isoformat(),
            'status': 'pending',
            'request': dados_requisicao
        }
        
        with open(nome_arquivo, 'w') as f:
            json.dump(info_requisicao, f, indent=2)
        
        print(f"[SOCKET] Requisição {id_requisicao} salva com sucesso")
    except Exception as e:
        print(f"[SOCKET] Erro ao salvar requisição: {e}")
        print(f"[SOCKET] Dados da requisição: {dados_requisicao}")

def validar_resposta(dados_requisicao, dados_resposta):
    #Verifica se a resposta bate com a requisição
    try:
        if dados_requisicao['acao'] == 'inclusao':
            return dados_resposta['resultado'] == 'sucesso'
        elif dados_requisicao['acao'] == 'requisicao':
            return 'dados' in dados_resposta
        return False
    except Exception as e:
        print(f"[VALIDACAO] Erro ao validar resposta: {e}")
        return False

def atualizar_requisicao_com_resposta(id_requisicao, dados_resposta):
    #Atualiza o arquivo da requisição com a resposta que recebemos
    try:
        nome_arquivo = os.path.join(PASTA_REQUISICOES, f'request_{id_requisicao}.json')
        
        if os.path.exists(nome_arquivo):
            with open(nome_arquivo, 'r') as f:
                info_requisicao = json.load(f)
            
            info_requisicao['status'] = 'completed'
            info_requisicao['response'] = dados_resposta
            info_requisicao['response_timestamp'] = datetime.now().isoformat()
            info_requisicao['valid'] = validar_resposta(info_requisicao['request'], dados_resposta)
            
            with open(nome_arquivo, 'w') as f:
                json.dump(info_requisicao, f, indent=2)
            
            print(f"[KAFKA] Requisição {id_requisicao} atualizada com resposta. Válida: {info_requisicao['valid']}")
        else:
            print(f"[KAFKA] Aviso: Arquivo da requisição não encontrado para ID {id_requisicao}")
    except Exception as e:
        print(f"[KAFKA] Erro ao atualizar requisição com resposta: {e}")

def criar_indice_se_nao_existir():
    #Cria o índice no Elasticsearch se ele não existir
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

def lidar_com_cliente(socket_cliente, endereco):
    #Cuida da conexão com o cliente
    print(f"Conexão estabelecida com {endereco}")
    try:
        while True:
            dados = socket_cliente.recv(1024)
            if not dados:
                break
            
            # Processa os dados que recebemos
            try:
                mensagem = json.loads(dados.decode())
                print(f"Dados recebidos: {mensagem}")
                
                # Verifica se tem todos os campos que precisamos
                if all(key in mensagem for key in ['symbol', 'price', 'volume']):
                    # Manda os dados pro Elasticsearch
                    es.index(
                        index="stock_data",
                        document={
                            "symbol": mensagem["symbol"],
                            "price": float(mensagem["price"]),
                            "volume": int(mensagem["volume"]),
                            "timestamp": mensagem.get("timestamp", datetime.now().isoformat()),
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
        print(f"Erro na conexão com {endereco}: {e}")
    finally:
        socket_cliente.close()
        print(f"Conexão encerrada com {endereco}")

def iniciar_servidor_socket():
    #Inicia o servidor socket
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.bind((HOST, PORT))
    servidor.listen(5)
    print(f"Servidor socket rodando em {HOST}:{PORT}")
    
    while True:
        socket_cliente, endereco = servidor.accept()
        thread_cliente = threading.Thread(target=lidar_com_cliente, args=(socket_cliente, endereco))
        thread_cliente.start()

def consumir_mensagens_kafka():
    #Pega as mensagens do Kafka e manda pro Elasticsearch
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )
    
    print("Começando a pegar mensagens do Kafka...")
    
    for mensagem in consumer:
        try:
            dados = mensagem.value
            print(f"Mensagem recebida do Kafka: {dados}")
            
            # Verifica se tem todos os campos que precisamos
            if all(key in dados for key in ['symbol', 'price', 'volume']):
                # Manda os dados pro Elasticsearch
                es.index(
                    index="stock_data",
                    document={
                        "symbol": dados["symbol"],
                        "price": float(dados["price"]),
                        "volume": int(dados["volume"]),
                        "timestamp": dados.get("timestamp", datetime.now().isoformat()),
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
        criar_indice_se_nao_existir()
        
        # Inicia o servidor socket em uma thread separada
        thread_socket = threading.Thread(target=iniciar_servidor_socket)
        thread_socket.daemon = True
        thread_socket.start()
        
        # Inicia o consumidor Kafka em uma thread separada
        thread_kafka = threading.Thread(target=consumir_mensagens_kafka)
        thread_kafka.daemon = True
        thread_kafka.start()
        
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Encerrando o programa...")
    except Exception as e:
        print(f"Erro fatal: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

