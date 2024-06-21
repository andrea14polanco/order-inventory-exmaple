import logging
from kombu import Connection, Exchange
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

rabbit_url = 'amqp://user:password@rabbitmq//'
connection = Connection(rabbit_url, connect_timeout=10, transport_options={'max_retries': 3, 'retry_delay': 5})
exchange = Exchange('orders', type='direct')

def send_order(order):
    try:
        with connection.Producer() as producer:
            exchange.declare(channel=producer.channel)
            logger.info(f"Serialized order: {json.dumps(order)}")
            producer.publish(json.dumps(order), # Convert dict to JSON string
                             exchange=exchange,
                             routing_key='order.created',
                             content_type='application/json', # Specify content type
                             content_encoding='utf-8') 
            logger.info(f"Order sent: {order}")
    except Exception as e:
        logger.error(f"Failed to send order: {e}")
