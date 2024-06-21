from flask import Flask, request, jsonify
import logging
import consumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Start the RabbitMQ consumer in the background
consumer_thread = consumer.start_consumer()

@app.route('/inventory/update', methods=['POST'])
def update_inventory():
    inventory_update = request.json
    try:
        # Logic to update inventory
        logger.info(f"Inventory update received: {inventory_update}")
        # Implement the actual update logic here
        return jsonify({"status": "Inventory updated"}), 200
    except Exception as e:
        logger.error(f"Error updating inventory: {e}")
        return jsonify({"error": "Failed to update inventory"}), 500

@app.route('/inventory', methods=['GET'])
def get_inventory():
    return jsonify(consumer.inventory), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
