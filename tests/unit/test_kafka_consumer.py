"""Unit tests for Kafka Consumer"""
import pytest
from src.data_ingestion.kafka_consumer import EcommerceKafkaConsumer

def test_process_purchase():
    """Test purchase event processing"""
    consumer = EcommerceKafkaConsumer(
        bootstrap_servers=['localhost:9092'],
        topic='test-topic',
        group_id='test-group'
    )
    
    event = {
        'event_type': 'purchase',
        'transaction_id': 'TXN123',
        'user_id': 'USER001',
        'total_amount': 99.99,
        'timestamp': '2024-01-15T10:30:00'
    }
    
    result = consumer.process_transaction(event)
    assert result is not None
    assert result['transaction_id'] == 'TXN123'
    assert result['total_amount'] == 99.99
