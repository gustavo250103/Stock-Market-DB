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

def enviar_resposta_inclusao(mensagem, resultado, id_registro):
    #Envia resposta de inclusão para o tópico de validações
    request_id = mensagem.value['request_id']  # Já é string do UUID
    resposta = {
        'chave-requisicao': request_id,
        'acao': mensagem.value['acao'],
        'banco': mensagem.value['banco'],
        'resultado': resultado,
        'id-registro-banco': id_registro
    }
    producer.send('topico-validacoes', key=request_id, value=resposta)

def inserir_mongodb(mensagem):
    #Insere dados no MongoDB
    conn = MongoClient('mongodb://mongodb:27017/')
    db = conn.StockMarket
    collection = db.noticias
    resultado = collection.insert_one(mensagem.value['dados'])
    return resultado.inserted_id

def inserir_postgresql(mensagem):
    #Insere dados no PostgreSQL
    conn = psycopg2.connect(
        host='postgres',
        database='users',
        user='admin',
        password='password'
    )
    cursor = conn.cursor()
    
    # Cria a tabela se não existir
    criar_tabela = """
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
    cursor.execute(criar_tabela)
    conn.commit()
    
    # Pega os dados da mensagem
    dados = mensagem.value['dados']
    
    # Mapeia os campos da mensagem para as colunas da tabela
    dados_mapeados = {
        'codigo': dados.get('codigo'),
        'preco': dados.get('preco'),
        'volume': dados.get('volume'),
        'variacao': dados.get('variacao'),
        'data_hora': dados.get('timestamp'),
        'ultima_atualizacao': dados.get('timestamp')
    }
    
    # Remove valores vazios do dicionário
    dados_mapeados = {k: v for k, v in dados_mapeados.items() if v is not None}
    
    # Monta a query de inserção
    colunas = ', '.join(dados_mapeados.keys())
    valores = ', '.join(['%s'] * len(dados_mapeados))
    query = f"INSERT INTO historicoFinanceiro ({colunas}) VALUES ({valores}) RETURNING id"
    
    cursor.execute(query, list(dados_mapeados.values()))
    id_registro = cursor.fetchone()[0]
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return id_registro

def inserir_cassandra(mensagem):
    #Insere dados no Cassandra
    cluster = Cluster(['cassandra'])
    session = cluster.connect()
    
    # Cria o keyspace se não existir
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS stockmarket
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    
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
    
    dados = mensagem.value['dados']
    
    query = """
        INSERT INTO analise_preditiva (id, ativo, previsao_preco, confianca, horizonte_tempo, timestamp)
        VALUES (uuid(), %s, %s, %s, %s, %s)
    """
    
    session.execute(query, (
        dados['ativo'],
        float(dados['previsao_preco']),
        float(dados['confianca']),
        dados['horizonte_tempo'],
        dados['timestamp']
    ))
    
    session.shutdown()
    cluster.shutdown()
    
    return "sucesso"

def pesquisar_postgresql(mensagem):
    #Pesquisa dados no PostgreSQL
    conn = psycopg2.connect(
        host='postgres',
        database='users',
        user='admin',
        password='password'
    )
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    dados = mensagem.value['dados']
    query = """
        SELECT * FROM historicoFinanceiro 
        WHERE codigo = %s 
        AND data_hora BETWEEN %s AND %s
        ORDER BY data_hora DESC
    """
    
    cursor.execute(query, (
        dados['codigo'],
        dados['data_inicio'],
        dados['data_fim']
    ))
    
    resultados = cursor.fetchall()
    cursor.close()
    conn.close()
    
    return resultados

def pesquisar_mongodb(mensagem):
    #Pesquisa dados no MongoDB
    conn = MongoClient('mongodb://mongodb:27017/')
    db = conn.StockMarket
    collection = db.noticias
    
    dados = mensagem.value['dados']
    query = {
        'symbol': dados['symbol'],
        'data_publicacao': {
            '$gte': dados['data_inicio'],
            '$lte': dados['data_fim']
        }
    }
    
    resultados = list(collection.find(query))
    conn.close()
    
    return resultados

def pesquisar_cassandra(mensagem):
    #Pesquisa dados no Cassandra
    cluster = Cluster(['cassandra'])
    session = cluster.connect('stockmarket')
    
    dados = mensagem.value['dados']
    query = """
        SELECT * FROM analise_preditiva 
        WHERE ativo = %s 
        AND timestamp >= %s 
        AND timestamp <= %s
        ALLOW FILTERING
    """
    
    resultados = session.execute(query, (
        dados['ativo'],
        dados['data_inicio'],
        dados['data_fim']
    ))
    
    session.shutdown()
    cluster.shutdown()
    
    return list(resultados)

def enviar_resposta_pesquisa(mensagem, resultado):
    #Envia resposta de pesquisa para o tópico de validações
    request_id = mensagem.value['request_id']  # Já é string do UUID
    resposta = {
        'chave-requisicao': request_id,
        'acao': mensagem.value['acao'],
        'banco': mensagem.value['banco'],
        'resultado': 'sucesso' if resultado else 'Nada encontrado',
        'dados': resultado
    }
    producer.send('topico-validacoes', key=request_id, value=resposta)

if __name__ == '__main__':
    for mensagem in consumer:
        if mensagem.value['acao'] == 'inclusao':
            print(f"Incluindo dados...")
            if mensagem.value['banco'] == 'PostgreSQL':
                print(f"Incluindo dados no PostgreSQL...")
                resultado = inserir_postgresql(mensagem)
                enviar_resposta_inclusao(mensagem, "sucesso", resultado)
                print(f"Dados inseridos com sucesso! ID: {resultado}")
            elif mensagem.value['banco'] == 'MongoDB':
                print(f"Incluindo dados no MongoDB...")
                resultado = inserir_mongodb(mensagem)
                enviar_resposta_inclusao(mensagem, "sucesso", resultado)
                print(f"Dados inseridos com sucesso! ID: {resultado}")
            elif mensagem.value['banco'] == 'Cassandra':
                print(f"Incluindo dados no Cassandra...")
                resultado = inserir_cassandra(mensagem)
                enviar_resposta_inclusao(mensagem, resultado, "sucesso")
                print(f"Dados inseridos com sucesso!")
        elif mensagem.value['acao'] == 'pesquisa':
            banco = mensagem.value['banco']
            print(f"Pesquisando dados no banco {banco}...")
            
            resultado = None
            if banco == 'PostgreSQL':
                resultado = pesquisar_postgresql(mensagem)
            elif banco == 'MongoDB':
                resultado = pesquisar_mongodb(mensagem)
            elif banco == 'Cassandra':
                resultado = pesquisar_cassandra(mensagem)
            
            enviar_resposta_pesquisa(mensagem, resultado)
            print(f"Pesquisa concluída no banco {banco}!")
        print(f"Mensagem recebida: {mensagem.value}")

