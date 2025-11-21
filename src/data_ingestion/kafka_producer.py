"""Kafka producer for e-commerce events"""
from kafka import KafkaProducer
import json
import time
from datetime import datetime

class EcommerceProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    
    def send_transaction(self, transaction_data):
        """Send transaction event to Kafka"""
        event = {
            'event_type': 'purchase',
            'timestamp': datetime.now().isoformat(),
            'data': transaction_data
        }
        self.producer.send('ecommerce-events', value=event)
        self.producer.flush()

if __name__ == "__main__":
    producer = EcommerceProducer()
    # Example usage
    sample_transaction = {
        'transaction_id': 'TXN001',
        'user_id': 'USER123',
        'total_amount': 99.99,
        'items': [{'product_id': 'PROD001', 'quantity': 2}]
    }
    producer.send_transaction(sample_transaction)
