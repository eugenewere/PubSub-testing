import pika
import sys
import threading
import json
import time

# class Subscriber(threading.Thread):
#     def __init__(self, queueName, bindingKey, exchange):
#         threading.Thread.__init__(self)
#         self.queueName = queueName
#         self.bindingKey = bindingKey
#         self.exchange = exchange
#         self.connection = self._create_connection()
#         self.channel = self.connection.channel()
#
#     def __del__(self):
#         self.connection.close()
#
#     @staticmethod
#     def _create_connection():
#         credentials = pika.credentials.PlainCredentials(username='rabbitmq', password="rabbitmq")
#         param = pika.ConnectionParameters(host="localhost", port=5672, credentials=credentials)
#         return pika.BlockingConnection(param)
#
#     @staticmethod
#     def on_message_callback(channel, method, properties, body):
#         binding_key = method.routing_key
#         print(f"received new message {binding_key}, {body}")
#         channel.basic_ack(delivery_tag=method.delivery_tag)
#
#     def setup(self):
#         self.channel.exchange_declare(exchange=self.exchange, exchange_type='fanout')
#         self.channel.queue_declare(queue=self.queueName, auto_delete=False)
#         self.channel.queue_bind(queue=self.queueName, exchange=self.exchange, routing_key=self.bindingKey)
#         self.channel.basic_qos(prefetch_count=THREADS * 10)
#         self.channel.basic_consume(queue=self.queueName, on_message_callback=self.on_message_callback, auto_ack=False)
#         # channel.basic_get(queue=self.queueName, on_message_callback=self.on_message_callback, auto_ack=True)
#         # channel.basic_get(self.on_message_callback,
#         #                   queue=self.queueName,
#         #                   no_ack=False)
#         threading.Thread(
#             target=self.channel.basic_consume(
#                 queue=self.queueName,
#                 on_message_callback=self.on_message_callback,
#                 auto_ack=False
#             )
#         )
#
#         print(' [*] Waiting for data for ' + self.queueName + '. To exit press CTRL+C')
#         try:
#             self.channel.start_consuming()
#         except KeyboardInterrupt:
#             self.channel.stop_consuming()
#
#
# subscriber = Subscriber(queueName='notifications', bindingKey='notifications', exchange='notifications')
#
# subscriber.setup()

ROUTING_KEY = 'notifications'
QUEUE_NAME = f'notifications'
EXCHANGE = 'notifications'
THREADS = 5


class ThreadedConsumer(threading.Thread):
    def __init__(self, queue_name, exchange, routing_key, threads=5):
        threading.Thread.__init__(self)
        credentials = pika.credentials.PlainCredentials(username='rabbitmq', password="rabbitmq")
        parameters = pika.ConnectionParameters(host="localhost", port=5672, credentials=credentials)
        connection = pika.BlockingConnection(parameters)
        self.queue_name = queue_name
        self.exchange = exchange
        self.routing_key = routing_key
        self.threads = threads
        self.channel = connection.channel()
        self.channel.exchange_declare(exchange=self.exchange, exchange_type="direct", durable=True)

        self.channel.queue_declare(queue=self.queue_name, auto_delete=False, durable=True)
        self.channel.queue_bind(queue=self.queue_name, exchange=self.exchange, routing_key=self.routing_key)
        self.channel.basic_qos(prefetch_count=self.threads*10)
        # self.channel.basic_consume(self.queue_name, on_message_callback=self.callback)
        threading.Thread(target=self.channel.basic_consume(self.queue_name, on_message_callback=self.callback))
    @staticmethod
    def callback(channel, method, properties, body):
        message = body
        time.sleep(5)
        print(f"{message}, {method.delivery_tag}, {method.routing_key}")
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.channel.stop_consuming()


for i in range(THREADS):
    print('Launch Consuming Thread', i)
    td = ThreadedConsumer(queue_name='notification', exchange='event', routing_key='event.notification')
    td2 = ThreadedConsumer(queue_name='logs', exchange='event', routing_key='event.logs')
    td.start()
    td2.start()


# """Basic message consumer example"""
# import functools
# import logging
# import pika
# from pika.exchange_type import ExchangeType
#
# LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
#               '-35s %(lineno) -5d: %(message)s')
# LOGGER = logging.getLogger(__name__)
#
# logging.basicConfig(level=logging.INFO)
#
#
# def on_message(chan, method_frame, header_frame, body, userdata=None):
#     """Called when a message is received. Log message and ack it."""
#     # LOGGER.info('Delivery properties: %s, message metadata: %s', method_frame, header_frame)
#     LOGGER.info('message body: %s tag: %s', body, method_frame.delivery_tag)
#     # sys.stdout.write(f"{body}")
#     time.sleep(2)
#     chan.basic_ack(delivery_tag=method_frame.delivery_tag)
#
#
# def main():
#     """Main method."""
#     credentials = pika.credentials.PlainCredentials(username='rabbitmq', password="rabbitmq")
#     parameters = pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials)
#     connection = pika.BlockingConnection(parameters)
#
#     channel = connection.channel()
#     channel.exchange_declare(
#         exchange='event',
#         exchange_type=ExchangeType.direct.value,
#         passive=False,
#         durable=True,
#         auto_delete=False)
#     channel.queue_declare(queue='notification', auto_delete=False, durable=True)
#     channel.queue_bind(queue='notification', exchange='event', routing_key='event.notification')
#     channel.basic_qos( prefetch_count=10)
#
#     on_message_callback = functools.partial(on_message, userdata='on_message_userdata')
#     channel.basic_consume('notification', on_message_callback)
#
#     try:
#         channel.start_consuming()
#     except KeyboardInterrupt:
#         channel.stop_consuming()
#
#     connection.close()
#
#
# if __name__ == '__main__':
#     main()