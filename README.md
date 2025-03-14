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

Utilização de Apache Kafka para gerenciar a transmissão de dados entre os serviços, garantindo uma arquitetura escalável.

### 3. Serviço

S1 (Coletor de Dados): manchetes de jornais relevantes para o mercado.
S2 (Processamento e Armazenamento): Realiza a limpeza, organização e armazenamento das informações coletadas nos respectivos bancos.
S3 (Análise e Validação): validar os dados armazenados (logs).

Todos os dados inclusos serão provenientes do Kaggle.

### 4. Justificativa do Uso de cada Tecnologia

PostgreSQL: Ideal para armazenamento estruturado de dados financeiros históricos e análises transacionais.
MongoDB: Perfeito para armazenar notícias e textos não estruturados de grande quantidade, facilitando consultas complexas.
Cassandra: Excelente para lidar com grandes volumes (rápido) de dados de séries temporais, garantindo escalabilidade para análise preditivas.

