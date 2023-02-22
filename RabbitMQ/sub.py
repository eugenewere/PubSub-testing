# subscriber.py
import pika
import sys


class Subscriber:
    def __init__(self, queueName, bindingKey, exchange):
        self.queueName = queueName
        self.bindingKey = bindingKey
        self.exchange = exchange
        self.connection = self._create_connection()

    def __del__(self):
        self.connection.close()

    @staticmethod
    def _create_connection():
        credentials = pika.credentials.PlainCredentials(username='rabbitmq', password="rabbitmq")
        param = pika.ConnectionParameters(host="localhost", port=5672, credentials=credentials)
        return pika.BlockingConnection(param)

    @staticmethod
    def on_message_callback(channel, method, properties, body):
        binding_key = method.routing_key
        print(f"received new message {binding_key}, {body}")

    def setup(self):
        channel = self.connection.channel()
        channel.exchange_declare(exchange=self.exchange, exchange_type='fanout')
        channel.queue_declare(queue=self.queueName)
        channel.queue_bind(queue=self.queueName, exchange=self.exchange, routing_key=self.bindingKey)
        channel.basic_consume(queue=self.queueName, on_message_callback=self.on_message_callback, auto_ack=True)
        print(' [*] Waiting for data for ' + self.queueName + '. To exit press CTRL+C')
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()


subscriber = Subscriber(queueName='notifications', bindingKey='notifications', exchange='notifications')

subscriber.setup()
