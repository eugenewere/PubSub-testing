# publisher.py
import json

import pika


class Publisher:

    def publish_message(self, exchange,queue, routing_key, message):
        connection = self.create_connection()
        channel = connection.channel()
        channel.exchange_declare(exchange=exchange, exchange_type="direct", durable=True)
        channel.queue_declare(queue=queue, auto_delete=False, durable=True)
        channel.queue_bind(queue=queue, exchange=exchange, routing_key=routing_key )
        channel.basic_publish(exchange=exchange, body=message, routing_key=routing_key)
        
    @staticmethod
    def create_connection():
        credentials = pika.credentials.PlainCredentials(username='rabbitmq', password="rabbitmq")
        param = pika.ConnectionParameters(host="localhost", port=5672, credentials=credentials)
        return pika.BlockingConnection(param)


publisher = Publisher()
for i in range(0, 10):
    publisher.publish_message(
        exchange="event",
        queue='notification',
        routing_key="event.notification",
        message=json.dumps({
            'id': i,
            'mesasge': f"Count {i}",
            'type': 'notification'
        })
    )
    publisher.publish_message(
        exchange="event",
        queue='logs',
        routing_key="event.logs",
        message=json.dumps({
            'id': i,
            'mesasge': f"Count {i}",
            'type': 'logs'
        })
    )
