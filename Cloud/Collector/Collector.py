import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import ast
import json
import threading
from kombu import Producer, Connection, Consumer, exceptions, Exchange, Queue, uuid
from kombu.utils.compat import nested

BROKER_CLOUD = "localhost"

producer_connection = Connection(BROKER_CLOUD)
consumer_connection = Connection(BROKER_CLOUD)
MODE = "PUSH" # PUSH or PULL

exchange = Exchange("IoT", type="direct")

TIME_COLLECT = 5

list_platform_id = []


def collect():
    print("Collect the states of the devices")
    for platform_id in list_platform_id:
        collect_by_platform_id(platform_id)
    threading.Timer(TIME_COLLECT, collect).start()


def collect_by_platform_id(platform_id):
    print('Collect data from platform_id: ', str(platform_id))
    message_request = {
        'reply_to': 'driver.response.collector.api_get_states',
        'platform_id': platform_id
    }

    request_queue = Queue(name='driver.request.api_get_states', exchange=exchange, routing_key='driver.request.api_get_states')
    request_routing_key = 'driver.request.api_get_states'
    producer_connection.ensure_connection()
    with Producer(producer_connection) as producer:
        producer.publish(
            json.dumps(message_request),
            exchange=exchange.name,
            routing_key=request_routing_key,
            declare=[request_queue],
            retry=True
        )


def handle_collect_by_platform_id(body, message):
    print('Recived state from platform_id: ', json.loads(body)['platform_id'])
    # print(msg.payload.decode('utf-8'))
    # print(ast.literal_eval(msg.payload.decode('utf-8')))
    list_things = json.loads(body)
    print(list_things)
    request_queue = Queue(name='dbwriter.request.api_write_db', exchange=exchange, routing_key='dbwriter.request.api_write_db')
    request_routing_key = 'dbwriter.request.api_write_db'
    producer_connection.ensure_connection()
    with Producer(producer_connection) as producer:
        producer.publish(
            json.dumps(list_things),
            exchange=exchange.name,
            routing_key=request_routing_key,
            declare=[request_queue],
            retry=True
        )
    print('Send new state to Dbwriter')


def get_list_platforms():
    print("Get list platforms from Registry")
    message = {
        'reply_to': 'registry.response.collector.api_get_list_platforms',
        'platform_status': "active"
    }

    queue = Queue(name='registry.request.api_get_list_platforms', exchange=exchange, routing_key='registry.request.api_get_list_platforms')
    routing_key = 'registry.request.api_get_list_platforms'
    producer_connection.ensure_connection()
    with Producer(producer_connection) as producer:
        producer.publish(
            json.dumps(message),
            exchange=exchange.name,
            routing_key=routing_key,
            declare=[queue],
            retry=True
        )


def handle_get_list(body, message):
    global list_platform_id
    list_platforms = json.loads(body)['list_platforms']
    temp = []
    for platform in list_platforms:
        temp.append(platform['platform_id'])

    list_platform_id = temp

    print('Updated list of platform_id: ', str(list_platform_id))


def handle_notification(body, message):
    print('Have Notification')
    if json.loads(body)['notification'] == 'Have Platform_id change':
        get_list_platforms()


def run():
    queue_notification = Queue(name='collector.request.notification', exchange=exchange,
                               routing_key='collector.request.notification')
    queue_list_platforms = Queue(name='registry.response.collector.api_get_list_platforms', exchange=exchange,
                                 routing_key='registry.response.collector.api_get_list_platforms')
    queue_get_states = Queue(name='driver.response.collector.api_get_states', exchange=exchange,
                             routing_key='driver.response.collector.api_get_states')

    if MODE == 'PULL':
        print("Collector use Mode: PULL Data")
        get_list_platforms()
        collect()

    while 1:
        try:
            consumer_connection.ensure_connection(max_retries=1)
            with nested(Consumer(consumer_connection, queues=queue_notification, callbacks=[handle_notification],
                                 no_ack=True),
                        Consumer(consumer_connection, queues=queue_list_platforms, callbacks=[handle_get_list],
                                 no_ack=True),
                        Consumer(consumer_connection, queues=queue_get_states,
                                 callbacks=[handle_collect_by_platform_id], no_ack=True)):
                while True:
                    consumer_connection.drain_events()
        except (ConnectionRefusedError, exceptions.OperationalError):
            print('Connection lost')
        except consumer_connection.connection_errors:
            print('Connection error')


run()



