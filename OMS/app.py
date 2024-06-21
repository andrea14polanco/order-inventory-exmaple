from flask import Flask, request, jsonify
from producer import send_order

app = Flask(__name__)

@app.route('/orders', methods=['POST'])
def create_order():
    order = request.json
    send_order(order)
    return jsonify({"status": "Order received"}), 200

if __name__ == "__main__":
     app.run(host="0.0.0.0", port=5002, debug=True)
