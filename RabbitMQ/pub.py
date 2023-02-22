# publisher.py
import pika


class Publisher:
    def __init__(self, exchange):
        self.exchange = exchange

    def publish(self, routing_key, message):
        connection = self.create_connection()
        channel = connection.channel()
        channel.exchange_declare(exchange=self.exchange, exchange_type="fanout")
        channel.basic_publish(exchange=self.exchange, routing_key=routing_key, body=message)

    @staticmethod
    def create_connection():
        credentials = pika.credentials.PlainCredentials(username='rabbitmq', password="rabbitmq")
        param = pika.ConnectionParameters(host="localhost", port=5672, credentials=credentials)
        return pika.BlockingConnection(param)


publisher = Publisher(exchange="notifications")
for i in range(0, 10000):
    publisher.publish("notifications", f"New Data {i}")
