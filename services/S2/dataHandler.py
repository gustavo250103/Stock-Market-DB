from kafka import KafkaConsumer
from kafka import KafkaProducer
import json
import pymongo
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import RealDictCursor
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

consumer = KafkaConsumer(
    'topico-requisicoes',
    api_version=(3, 9, 0),
    bootstrap_servers='kafka:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='grupo-requisicoes',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    api_version=(3, 9, 0),
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8') if k is not None else None
)

def respostaInclusao(message, resultado, id_registro):
    resposta = {
        'chave-requisicao': message.key,
        'acao': message.value['acao'],
        'banco': message.value['banco'],
        'resultado': resultado,
        'id-registro-banco': id_registro
    }
    producer.send('topico-validacoes', resposta)

def inputMongoDB(message):
    conn = MongoClient('mongodb://mongodb:27017/')
    db = conn.StockMarket
    collection = db.noticias
    resultado = collection.insert_one(message.value['dados'])
    return resultado.inserted_id

def inputPostgreSQL(message):
    conn = psycopg2.connect(
        host='postgres',
        database='users',
        user='admin',
        password='password'
    )
    cursor = conn.cursor()
    
    # Cria a tabela se não existir
    create_table_query = """
    CREATE TABLE IF NOT EXISTS historicoFinanceiro (
        id SERIAL PRIMARY KEY,
        codigo VARCHAR(10),
        preco DECIMAL(10,2),
        volume INTEGER,
        variacao DECIMAL(10,2),
        data_hora TIMESTAMP,
        ultima_atualizacao TIMESTAMP
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    
    # Extrai os dados da mensagem
    dados = message.value['dados']
    
    # Mapeia os campos da mensagem para as colunas da tabela
    dados_mapeados = {
        'codigo': dados.get('codigo'),
        'preco': dados.get('preco'),
        'volume': dados.get('volume'),
        'variacao': dados.get('variacao'),
        'data_hora': dados.get('timestamp'),  # Mapeia timestamp para data_hora
        'ultima_atualizacao': dados.get('timestamp')  # Usa o mesmo timestamp para ultima_atualizacao
    }
    
    # Remove valores None do dicionário
    dados_mapeados = {k: v for k, v in dados_mapeados.items() if v is not None}
    
    # Cria a query de inserção dinamicamente baseada nas chaves do dicionário
    colunas = ', '.join(dados_mapeados.keys())
    valores = ', '.join(['%s'] * len(dados_mapeados))
    query = f"INSERT INTO historicoFinanceiro ({colunas}) VALUES ({valores}) RETURNING id"
    
    # Executa a query com os valores
    cursor.execute(query, list(dados_mapeados.values()))
    id_registro = cursor.fetchone()[0]
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return id_registro

def inputCassandra(message):
    # Conecta ao cluster Cassandra
    cluster = Cluster(['cassandra'])
    session = cluster.connect()
    
    # Cria o keyspace se não existir
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS stockmarket
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    
    # Usa o keyspace
    session.set_keyspace('stockmarket')
    
    # Cria a tabela se não existir
    session.execute("""
        CREATE TABLE IF NOT EXISTS analise_preditiva (
            id UUID PRIMARY KEY,
            ativo text,
            previsao_preco decimal,
            confianca decimal,
            horizonte_tempo text,
            timestamp timestamp
        )
    """)
    
    # Extrai os dados da mensagem
    dados = message.value['dados']
    
    # Prepara a query de inserção
    query = """
        INSERT INTO analise_preditiva (id, ativo, previsao_preco, confianca, horizonte_tempo, timestamp)
        VALUES (uuid(), %s, %s, %s, %s, %s)
    """
    
    # Executa a query com os valores
    session.execute(query, (
        dados['ativo'],
        float(dados['previsao_preco']),
        float(dados['confianca']),
        dados['horizonte_tempo'],
        dados['timestamp']
    ))
    
    # Fecha a conexão
    session.shutdown()
    cluster.shutdown()
    
    return "sucesso"

if __name__ == '__main__':
    for message in consumer:
        if message.value['acao'] == 'inclusao':
            print(f"Incluindo dados...")
            if message.value['banco'] == 'PostgreSQL':
                print(f"Incluindo dados no PostgreSQL...")
                result = inputPostgreSQL(message)
                respostaInclusao(message, "sucesso", result)
                print(f"Dados inseridos com sucesso! ID: {result}")
            elif message.value['banco'] == 'MongoDB':
                print(f"Incluindo dados no MongoDB...")
                result = inputMongoDB(message)
                respostaInclusao(message, "sucesso", result)
                print(f"Dados inseridos com sucesso! ID: {result}")
            elif message.value['banco'] == 'Cassandra':
                print(f"Incluindo dados no Cassandra...")
                result = inputCassandra(message)
                respostaInclusao(message, result, "sucesso")
                print(f"Dados inseridos com sucesso!")
        print(f"Received message: {message.value}")

