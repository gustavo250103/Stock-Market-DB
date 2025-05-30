# Stock-Market-DB
Análise Global de Mercados Financeiros

## Integrantes

Gustavo Dias Vicentin - 22.123.061-8

Thales Pasquotto - 22.222.033-7

## Arquitetura do Projeto

![image](https://github.com/user-attachments/assets/1ea9ff18-e885-4084-a530-bbc6d363aefa)

## Tema

Este projeto visa criar um sistema de análise financeira utilizando dados de bolsas de valores globais, taxas de câmbio do dólar e notícias econômicas internacionais para entender como diferentes fatores influenciam os mercados.

## Estrutura do Projeto

### 1.  Bancos de Dados Utilizados

 - RDB (Banco Relacional - PostgreSQL): Armazena dados históricos e atuais das bolsas de valores e do índice do dólar (ativos financeiros).
 - DB1 (NoSQL - MongoDB): Responsável pelo armazenamento de notícias econômicas coletadas de diferentes fontes (Notícias e análises financeiras relacionadas a ativos).
 - DB2 (NoSQL - Cassandra): Utilizado para armazenar análises preditivas e séries temporais de preços das ações e índices (Registro de eventos do mercado financeiro [ex: volume negociado, variações]).

### 2. Mensageria

- Utilização de Apache Kafka para gerenciar a transmissão de dados entre os serviços, garantindo uma arquitetura escalável.

### 3. Serviço

- S1 (Coletor de Dados): manchetes de jornais relevantes para o mercado.
- S2 (Processamento e Armazenamento): Realiza a limpeza, organização e armazenamento das informações coletadas nos respectivos bancos.
- S3 (Análise e Validação): validar os dados armazenados (logs).

+ Todos os dados inclusos serão provenientes da biblioteca Faker do Python.

### 4. Justificativa do Uso de cada Tecnologia

- PostgreSQL: Ideal para armazenamento estruturado de dados financeiros históricos e análises transacionais.
- MongoDB: Perfeito para armazenar notícias e textos não estruturados de grande quantidade, facilitando consultas complexas.
- Cassandra: Excelente para lidar com grandes volumes (rápido) de dados de séries temporais, garantindo escalabilidade para análise preditivas.

## Arquitetura Detalhada

### Serviços

#### S1 - Coletor de Dados (Producer)
- Responsável por gerar e enviar dados simulados para o sistema
- Implementa diferentes tipos de dados para cada banco:
  - PostgreSQL: Dados históricos financeiros (preços, volumes, variações)
  - MongoDB: Notícias e análises financeiras
  - Cassandra: Análises preditivas e previsões
- Utiliza Apache Kafka para envio das mensagens
- Gera dados aleatórios usando a biblioteca Faker

#### S2 - Processador de Dados (DataHandler)
- Consome mensagens do Kafka e processa as operações nos bancos de dados
- Implementa operações de:
  - Inserção de dados em todos os bancos
  - Pesquisa de dados com filtros temporais
- Gerencia conexões com:
  - PostgreSQL: Dados históricos financeiros
  - MongoDB: Notícias e análises
  - Cassandra: Previsões e séries temporais
- Envia respostas para validação via Kafka

#### S3 - Validador (Consumer)
- Consome mensagens de requisições e respostas
- Armazena logs no Elasticsearch
- Monitora o fluxo completo das operações
- Mantém histórico de todas as transações
- Implementa sistema de rastreamento de requisições

### Fluxo de Dados

1. O Producer (S1) gera dados simulados e envia para o tópico `topico-requisicoes`
2. O DataHandler (S2) processa as requisições e executa as operações nos bancos
3. O Consumer (S3) registra todas as operações no Elasticsearch
4. As respostas são enviadas para o tópico `topico-validacoes`

## Requisitos do Sistema

### Dependências Principais
- PostgreSQL 14+
- MongoDB 6+
- Apache Cassandra 4+
- Apache Kafka 3+
- Elasticsearch 7+
- Python packages:
  - kafka-python
  - pymongo
  - psycopg2
  - cassandra-driver
  - elasticsearch
  - faker
  - pandas
  - numpy

## Instalação e Configuração

1. Clone o repositório:
```bash
git clone https://github.com/seu-usuario/Stock-Market-DB.git
cd Stock-Market-DB
```

2. Inicie os containers Docker:
```bash
docker-compose up --build
```
## Monitoramento

O sistema utiliza Elasticsearch para monitoramento e logging:
- Índice: `requests_responses`
- Campos principais:
  - request_id: Identificador único da requisição
  - request_timestamp: Momento da requisição
  - response_timestamp: Momento da resposta
  - status: Estado da operação
  - banco: Banco de dados utilizado
  - acao: Tipo de operação
  - resultado: Resultado da operação

## Portas Utilizadas

### Bancos de Dados
- PostgreSQL: 5432
- MongoDB: 27017
- Cassandra: 9042

### Mensageria
- Apache Kafka: 9092
- Zookeeper (Kafka): 2181

### Monitoramento
- Elasticsearch: 9200
- Kibana (Interface do Elasticsearch): 5601

### Serviços
- S1 (Producer): 9092 (Kafka)
- S2 (DataHandler): 9092 (Kafka)
- S3 (Consumer): 9092 (Kafka)

> **Nota**: Todas as portas são configuráveis através do arquivo `docker-compose.yml`



