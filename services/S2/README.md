# Serviço 2 - Consumidor de Dados

Este serviço consome mensagens dos tópicos do Kafka e as armazena nos bancos de dados apropriados:

1. PostgreSQL: Armazena dados de ações
2. MongoDB: Armazena dados de notícias
3. Cassandra: Armazena eventos de mercado

## Pré-requisitos

1. PostgreSQL deve estar em execução e acessível
2. MongoDB deve estar em execução e acessível
3. Cassandra deve estar em execução e acessível
4. Kafka deve estar em execução e acessível

## Configuração dos Bancos de Dados

### PostgreSQL
- Nome do banco: stock_market
- Usuário: postgres
- Senha: postgres
- Host: localhost
- Porta: 5432

### MongoDB
- Host: localhost
- Porta: 27017
- Banco: stock_market
- Coleção: news

### Cassandra
- Host: localhost
- Keyspace: stock_market

## Uso

1. Instale as dependências:
   ```bash
   pip install -r ../../requirements.txt
   ```
2. Execute o consumidor:
   ```bash
   python consumer.py
   ```

O serviço irá consumir mensagens continuamente do Kafka e armazená-las nos bancos de dados apropriados. 