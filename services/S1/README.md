# Serviço 1 - Produtor de Dados

Este serviço gera e envia dados simulados para tópicos do Kafka. Ele simula três tipos de dados:

1. Dados de Ações (para PostgreSQL)
   - Símbolo
   - Preço
   - Volume
   - Timestamp

2. Dados de Notícias (para MongoDB)
   - Título
   - Conteúdo
   - Fonte
   - Data de Publicação
   - Sentimento

3. Eventos de Mercado (para Cassandra)
   - ID do Evento
   - Tipo do Evento
   - Descrição
   - Severidade
   - Timestamp

## Uso

1. Certifique-se que o Kafka está em execução
2. Instale as dependências:
   ```bash
   pip install -r ../../requirements.txt
   ```
3. Execute o produtor:
   ```bash
   python producer.py
   ```

O serviço irá gerar e enviar dados continuamente para os tópicos do Kafka a cada 5 segundos. 