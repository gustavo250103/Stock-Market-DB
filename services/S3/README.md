# Serviço 3 - Validador de Mensagens

Este serviço valida e registra mensagens dos tópicos do Kafka. Ele executa as seguintes funções:

1. Consome mensagens de todos os tópicos do Kafka
2. Valida a estrutura e o conteúdo das mensagens
3. Registra tanto as mensagens recebidas quanto os resultados da validação
4. Armazena os logs em arquivos diários por tópico

## Registro de Logs

Os logs são armazenados no diretório `logs` com o seguinte formato:
- Nome do arquivo: `{tópico}_{AAAAMMDD}.log`
- Formato: `[timestamp] [tipo_mensagem] {mensagem_json}`

Tipos de mensagem:
- RECEIVED: Mensagem original recebida do Kafka
- VALIDATION: Resultado da validação da mensagem

## Regras de Validação

### Dados de Ações
- Campos obrigatórios: símbolo, preço, volume, timestamp
- O preço deve ser numérico
- O volume deve ser inteiro

### Dados de Notícias
- Campos obrigatórios: título, conteúdo, fonte, data_publicação, sentimento
- O sentimento deve ser um dos seguintes: positivo, negativo, neutro

### Eventos de Mercado
- Campos obrigatórios: id_evento, tipo_evento, descrição, severidade, timestamp
- A severidade deve ser uma das seguintes: baixa, média, alta

## Uso

1. Instale as dependências:
   ```bash
   pip install -r ../../requirements.txt
   ```
2. Execute o validador:
   ```bash
   python validator.py
   ```

O serviço irá validar mensagens e registrar os resultados continuamente. 