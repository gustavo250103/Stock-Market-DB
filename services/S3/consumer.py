from kafka import KafkaConsumer
import json
from datetime import datetime
import time
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError as ESConnectionError

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

# Configuração do Kafka
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
TOPICO_REQUISICOES = 'topico-requisicoes'
TOPICO_VALIDACOES = 'topico-validacoes'

def criar_indice_se_nao_existir():
    #Cria o índice no Elasticsearch se ele não existir
    if not es.indices.exists(index="requests_responses"):
        es.indices.create(
            index="requests_responses",
            body={
                "mappings": {
                    "properties": {
                        "request_id": {"type": "keyword"},
                        "request_timestamp": {"type": "date"},
                        "response_timestamp": {"type": "date"},
                        "status": {"type": "keyword"},
                        "banco": {"type": "keyword"},
                        "acao": {"type": "keyword"},
                        "request": {"type": "object"},
                        "response": {"type": "object"},
                        "resultado": {"type": "keyword"},
                        "dados_resposta": {"type": "object"}
                    }
                }
            }
        )

def processar_requisicao(mensagem):
    #Salva a requisição no Elasticsearch
    try:
        # Pega o ID da chave da mensagem e decodifica se for bytes
        request_id = mensagem.key.decode('utf-8') if isinstance(mensagem.key, bytes) else mensagem.key
        if not request_id:
            print(f"Erro: chave da mensagem não encontrada. Mensagem: {mensagem.value}")
            return

        # Prepara o documento para o Elasticsearch
        documento = {
            "request_id": request_id,
            "request_timestamp": datetime.now().isoformat(),
            "status": "pending",
            "request": mensagem.value,
            "banco": mensagem.value.get('banco'),
            "acao": mensagem.value.get('acao')
        }

        # Salva a requisição
        es.index(
            index="requests_responses",
            id=request_id,
            document=documento
        )
        print(f"[KAFKA] Requisição {request_id} salva com sucesso")
    except Exception as e:
        print(f"[KAFKA] Erro ao processar requisição: {e}")
        print(f"[KAFKA] Mensagem que causou o erro: {mensagem.value}")

def processar_resposta(mensagem):
    #Atualiza a requisição com a resposta no Elasticsearch
    try:
        # Pega o ID da chave da mensagem e decodifica se for bytes
        request_id = mensagem.key.decode('utf-8') if isinstance(mensagem.key, bytes) else mensagem.key
        if not request_id:
            print(f"Erro: chave da mensagem não encontrada. Mensagem: {mensagem.value}")
            return

        # Verifica se o documento existe
        if not es.exists(index="requests_responses", id=request_id):
            print(f"Erro: Documento {request_id} não encontrado no Elasticsearch")
            print(f"[KAFKA] Tentando criar novo documento com ID: {request_id}")
            # Tenta criar um novo documento se não existir
            documento = {
                "request_id": request_id,
                "request_timestamp": datetime.now().isoformat(),
                "status": "completed",
                "response": mensagem.value,
                "response_timestamp": datetime.now().isoformat(),
                "resultado": mensagem.value.get('resultado', 'N/A'),
                "dados_resposta": mensagem.value.get('dados', {})
            }
            es.index(index="requests_responses", id=request_id, document=documento)
            print(f"[KAFKA] Novo documento criado com ID: {request_id}")
            return

        # Prepara a atualização
        atualizacao = {
            "status": "completed",
            "response": mensagem.value,
            "response_timestamp": datetime.now().isoformat(),
            "resultado": mensagem.value.get('resultado', 'N/A'),
            "dados_resposta": mensagem.value.get('dados', {})
        }

        # Atualiza o documento
        try:
            es.update(
                index="requests_responses",
                id=request_id,
                body={
                    "doc": atualizacao
                }
            )
            print(f"[KAFKA] Resposta para requisição {request_id} salva com sucesso")
            print(f"[KAFKA] Dados da resposta: {atualizacao}")
        except Exception as e:
            print(f"[KAFKA] Erro ao atualizar documento no Elasticsearch: {e}")
            # Tenta obter o documento atual para debug
            try:
                doc = es.get(index="requests_responses", id=request_id)
                print(f"[KAFKA] Documento atual: {doc}")
            except Exception as e2:
                print(f"[KAFKA] Erro ao obter documento: {e2}")
    except Exception as e:
        print(f"[KAFKA] Erro ao processar resposta: {e}")
        print(f"[KAFKA] Mensagem que causou o erro: {mensagem.value}")

def main():
    try:
        # Cria o índice no Elasticsearch
        criar_indice_se_nao_existir()
        
        # Configura os consumidores Kafka
        consumer_requisicoes = KafkaConsumer(
            TOPICO_REQUISICOES,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        
        consumer_respostas = KafkaConsumer(
            TOPICO_VALIDACOES,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        
        print("Iniciando processamento de mensagens...")
        
        # Processa mensagens dos dois tópicos simultaneamente
        while True:
            # Processa mensagens de requisições
            mensagens_requisicoes = consumer_requisicoes.poll(timeout_ms=100)
            for tp, msgs in mensagens_requisicoes.items():
                for mensagem in msgs:
                    processar_requisicao(mensagem)
            
            # Processa mensagens de respostas
            mensagens_respostas = consumer_respostas.poll(timeout_ms=100)
            for tp, msgs in mensagens_respostas.items():
                for mensagem in msgs:
                    processar_resposta(mensagem)
            
            time.sleep(0.1)  # Pequena pausa para não sobrecarregar
            
    except KeyboardInterrupt:
        print("Encerrando o programa...")
    except Exception as e:
        print(f"Erro fatal: {e}")

if __name__ == "__main__":
    main()

