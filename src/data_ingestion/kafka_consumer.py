"""
Kafka Consumer for Real-time E-commerce Events
Processes transactions, user behavior, and inventory updates
"""
from kafka import KafkaConsumer
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EcommerceKafkaConsumer:
    def __init__(self, bootstrap_servers, topic, group_id):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logger.info(f"Kafka consumer initialized for topic: {topic}")
    
    def process_transaction(self, event):
        """Process individual transaction event"""
        try:
            event_type = event.get('event_type')
            timestamp = datetime.fromisoformat(event.get('timestamp'))
            
            if event_type == 'purchase':
                return self._process_purchase(event)
            elif event_type == 'cart_add':
                return self._process_cart_event(event)
            elif event_type == 'view':
                return self._process_view_event(event)
            
            logger.warning(f"Unknown event type: {event_type}")
            return None
        except Exception as e:
            logger.error(f"Error processing event: {e}")
            return None
    
    def _process_purchase(self, event):
        """Process purchase transaction"""
        return {
            'transaction_id': event.get('transaction_id'),
            'user_id': event.get('user_id'),
            'total_amount': event.get('total_amount'),
            'items': event.get('items'),
            'processed_at': datetime.now().isoformat()
        }
    
    def _process_cart_event(self, event):
        """Process cart addition event"""
        return {
            'user_id': event.get('user_id'),
            'product_id': event.get('product_id'),
            'quantity': event.get('quantity'),
            'action': 'add_to_cart'
        }
    
    def _process_view_event(self, event):
        """Process product view event"""
        return {
            'user_id': event.get('user_id'),
            'product_id': event.get('product_id'),
            'category': event.get('category'),
            'action': 'view'
        }
    
    def start_consuming(self):
        """Start consuming messages from Kafka"""
        logger.info("Starting message consumption...")
        try:
            for message in self.consumer:
                event = message.value
                processed = self.process_transaction(event)
                if processed:
                    logger.info(f"Processed event: {processed}")
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        finally:
            self.consumer.close()

if __name__ == "__main__":
    consumer = EcommerceKafkaConsumer(
        bootstrap_servers=['localhost:9092'],
        topic='ecommerce-events',
        group_id='analytics-consumer-group'
    )
    consumer.start_consuming()
