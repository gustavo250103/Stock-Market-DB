KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPICS = {
    'stock_data': 'stock_data_topic',
    'news_data': 'news_data_topic',
    'market_events': 'market_events_topic',
    'data_requests': 'data_requests_topic',
    'data_responses': 'data_responses_topic'
}

# Consumer group IDs
CONSUMER_GROUPS = {
    'S2': 's2_consumer_group',
    'S3': 's3_consumer_group'
} 