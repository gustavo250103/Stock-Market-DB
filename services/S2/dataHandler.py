from kafka import KafkaConsumer
from kafka import KafkaProducer
import json
import pymongo
from pymongo import MongoClient

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
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
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
    conn = MongoClient('mongodb://localhost:27017/')
    db = conn.StockMarket
    collection = db.noticias
    resultado = collection.insert_one(message.value['dados'])
    return resultado.inserted_id

if __name__ == '__main__':
    for message in consumer:
        if message.value['acao'] == 'inclusao':
            print(f"Incluindo dados...")
            if message.value['banco'] == 'PostgreSQL':
                print(f"Incluindo dados no PostgreSQL: {message.value['dados']}")
            elif message.value['banco'] == 'MongoDB':
                print(f"Incluindo dados no MongoDB...")
                result = inputMongoDB(message)
                respostaInclusao(message, "sucesso", result)
                print(f"Dados inseridos com sucesso! ID: {result}")
            elif message.value['banco'] == 'Cassandra':
                print(f"Incluindo dados no Cassandra: {message.value['dados']}")
        print(f"Received message: {message.value}")

