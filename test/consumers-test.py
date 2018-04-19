from kombu import Connection, Exchange, Queue, Consumer, exceptions
import socket
import json
rabbit_url = "amqp://localhost:5672/"
conn = Connection(rabbit_url)
exchange = Exchange("IoT", type="direct")
# routing_key = "registry.request.api_add_platform"
# queue_name = "registry.request.api_add_platform"
# queue = Queue(name=queue_name, exchange=exchange, routing_key=routing_key)

queue_response = Queue(name='1', exchange=exchange, routing_key='registry.request.api_add_platform')

routing_key = '1'

def process_message(body, message):
  print(body)
  body = json.loads(body)
  print(body["reply_to"])
  message.ack()
#
# while 1:
#     try:
#         conn.ensure_connection(max_retries=1)
#         with Consumer(conn, queues=queue, callbacks=[process_message]):
#             while True:
#                 conn.drain_events()
#     except (ConnectionRefusedError, exceptions.OperationalError) as hihi:
#         print('Connection lost')
#     except conn.connection_errors:
#         print('Connection error')

conn.ensure_connection()
with Consumer(conn, queues=queue_response, callbacks=[process_message]):
    while True:
        conn.drain_events()


