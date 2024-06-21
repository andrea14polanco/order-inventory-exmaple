import logging
import json
import threading
from kombu import Connection, Exchange, Queue, Consumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

rabbit_url = 'amqp://user:password@rabbitmq//'
connection = Connection(rabbit_url)
exchange = Exchange('orders', type='direct')
queue = Queue('inventory_update', exchange, routing_key='order.created')

# Diccionario para almacenar el inventario en memoria
inventory = {}


def process_order(body, message):
    try:
        # Log the raw message received
        logger.info(f"Received raw message: {body}")
        
        # Check if the message is already a dictionary
        if isinstance(body, dict):
            order = body
        else:
            # Deserialize JSON string to dictionary
            order = json.loads(body)
        
        logger.info(f"Deserialized order: {order}")
        
        # Actualizar el inventario
        item_id = order.get('item_id')
        quantity = order.get('quantity')

        
        # Logic to update inventory
        if item_id is not None and quantity is not None:
            if item_id in inventory:
                inventory[item_id] += quantity
            else:
                inventory[item_id] = quantity
            logger.info(f"Updated inventory: {inventory}")
        message.ack()
    except Exception as e:
        logger.error(f"Failed to process order: {e}")
        message.reject()

def start_consumer():
    def run():
        try:
            with connection.Consumer(queue, callbacks=[process_order]) as consumer:
                exchange.declare(channel=consumer.channel)
                queue.declare(channel=consumer.channel)
                logger.info("Consumer started. Waiting for messages...")
                while True:
                    connection.drain_events()
        except Exception as e:
            logger.error(f"Consumer encountered an error: {e}")

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread
