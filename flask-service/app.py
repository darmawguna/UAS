import os
from flask import Flask, request, jsonify
import mysql.connector
from confluent_kafka import Producer
import json
import datetime

app = Flask(__name__)

# Buat pool koneksi
db_config = {
    'host': os.getenv('MYSQL_HOST'),
    'port': os.getenv('MYSQL_PORT'),
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD'),
    'database': os.getenv('MYSQL_DATABASE')
}

connection_pool = mysql.connector.pooling.MySQLConnectionPool(pool_name="mypool",
                                                             pool_size=10,
                                                             **db_config)

# Kafka connection
kafka_broker = os.getenv('KAFKA_BROKER', 'redpanda:9092')
kafka_client_id = os.getenv('KAFKA_CLIENT_ID', 'api-service')
kafka_topic = os.getenv('KAFKA_TOPIC', 'KAFKAUAS')
kafka_log_topic = os.getenv('KAFKA_LOG_TOPIC', 'logs')

conf = {
    'bootstrap.servers': kafka_broker,
    'client.id': kafka_client_id
}

producer = Producer(**conf)

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

@app.route('/items', methods=['GET'])
def get_items():
    try:
        conn = connection_pool.get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM items")
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(results)
    except Exception as err:
        print("Error fetching items:", err)
        return jsonify({"error": "Error fetching items"}), 500


@app.route('/add-item', methods=['POST'])
def add_item():
    data = request.json
    name = data.get('name')
    description = data.get('description')
    try:
        conn = connection_pool.get_connection()
        cursor = conn.cursor()
        cursor.execute("INSERT INTO items (name, description) VALUES (%s, %s)", (name, description))
        conn.commit()
        new_item_id = cursor.lastrowid
        cursor.close()
        conn.close()
        new_item = {'id': new_item_id, 'name': name, 'description': description}
        
        print("Item added to database:", new_item)
        
        producer.produce(kafka_topic, 
                         key=str(new_item_id),
                         value=json.dumps({
                             'topic': kafka_topic,
                             'message': new_item,
                             'sender': kafka_client_id,
                             'status': True,
                             'created_at': str(datetime.datetime.now())
                         }), 
                         callback=delivery_report)
        
        producer.poll(1)  # Wait for message to be delivered

        print("Message sent to Kafka topic:", kafka_topic, new_item)
        
        return jsonify(new_item), 201
    except Exception as error:
        print("Error adding item:", error)
        
        error_message = json.dumps({
            'topic': kafka_log_topic,
            'message': {'name': name, 'description': description},
            'sender': kafka_client_id,
            'status': False,
            'created_at': str(datetime.datetime.now()),
            'error': str(error)
        })
        
        producer.produce(kafka_log_topic, 
                         key='error',
                         value=error_message, 
                         callback=delivery_report)
        
        producer.poll(1)  # Wait for message to be delivered
        
        print("Error log sent to Kafka topic:", kafka_log_topic, error_message)
        
        return jsonify({"error": "Error adding item"}), 500


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=3000)
