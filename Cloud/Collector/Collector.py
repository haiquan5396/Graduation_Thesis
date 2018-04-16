import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import ast
import json
import threading
from kombu import Producer, Connection, Consumer, exceptions, Exchange, Queue, uuid
from kombu.utils.compat import nested

BROKER_CLOUD = "localhost"
BROKER_FOG = "localhost"

producer_connection = Connection(BROKER_CLOUD)
consumer_connection = Connection(BROKER_CLOUD)
MODE = "PULL" # or PUSH

exchange = Exchange("IoT", type="direct")

TIME_COLLECT = 5

list_platforms = []


def collect():
    print("Collect the states of the devices")
    for platform_id in list_platforms:
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


def get_thing_by_id(thing_global_id):
    print('Get thing by thing_global_id')
    check_response = 0
    thing = {}

    response_queue_name = uuid()
    response_queue = Queue(name=response_queue_name, exclusive=True, auto_delete=True, exchange=exchange, routing_key=response_queue_name)

    message = {
        'caller': response_queue_name,
        'thing_global_id': thing_global_id
    }


    def handle_api_get_thing_by_global_id(body, message):
        nonlocal check_response, thing
        thing = json.loads(body)
        check_response = 1

    request_queue = Queue(name='dbwriter.request.api_get_thing_by_global_id', exchange=exchange,
                          routing_key='dbwriter.request.api_get_thing_by_global_id')
    request_routing_key = 'dbwriter.request.api_get_thing_by_global_id'

    producer_connection.ensure_connection()
    with Producer(producer_connection) as producer:
        producer.publish(
            json.dumps(message),
            exchange=exchange.name,
            routing_key=request_routing_key,
            declare=[request_queue],
            retry=True
        )

    with Consumer(producer_connection,
                  callbacks=[handle_api_get_thing_by_global_id],
                  queues=[response_queue], no_ack=True):
        while check_response == 0:
            producer_connection.drain_events()





    return thing


def get_things(list_thing_global_id):
    print('Get all thing state in list thing')
    check_response = 0
    list_thing = []
    response_queue_name = uuid()
    response_queue = Queue(name=response_queue_name, exclusive=True, auto_delete=True, exchange=exchange, routing_key=response_queue_name)

    message = {
        'caller': response_queue_name,
        'list_thing_global_id': list_thing_global_id
    }

    def handle_api_get_things(body, message):
        nonlocal check_response, list_thing
        list_thing = ast.literal_eval(body)
        check_response = 1

    request_queue = Queue(name='dbwriter.request.api_get_things', exchange=exchange, routing_key='dbwriter.request.api_get_things')
    request_routing_key = 'dbwriter.request.api_get_things'

    producer_connection.ensure_connection()
    with Producer(producer_connection) as producer:
        producer.publish(
            json.dumps(message),
            exchange=exchange.name,
            routing_key=request_routing_key,
            declare=[request_queue],
            retry=True
        )

    with Consumer(producer_connection,
                  callbacks=[handle_api_get_things],
                  queues=[response_queue], no_ack=True):
        while check_response == 0:
            producer_connection.drain_events()

    return list_thing


def get_list_platforms():
    print("Get list platforms from Registry")
    message = {
        'reply_to': 'registry.response.collector.api_get_list_platforms'
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
    global list_platforms
    list_platforms = ast.literal_eval(body)
    print('Updated list of platform_id: ', str(list_platforms))


def handle_notification(body, message):
    print('Have Notification')
    if json.loads(body)['notification'] == 'Have Platform_id change':
        get_list_platforms()


def api_get_things(body, message):
    print('API get things')
    response_routing_key = json.loads(body)['reply_to']
    list_thing_global_id = json.loads(body)['list_thing_global_id']
    message_response = get_things(list_thing_global_id)

    producer_connection.ensure_connection()
    with Producer(producer_connection) as producer:
        producer.publish(
            str(message_response),
            exchange=exchange.name,
            routing_key=response_routing_key,
            retry=True
        )


def api_get_thing_by_id(body, message):
    print('Get thing state by id')
    response_routing_key = json.loads(body)['reply_to']
    thing_global_id = json.loads(body)['thing_global_id']
    message_response = get_thing_by_id(thing_global_id)
    producer_connection.ensure_connection()
    with Producer(producer_connection) as producer:
        producer.publish(
            str(message_response),
            exchange=exchange.name,
            routing_key=response_routing_key,
            retry=True
        )


def run():
    queue_get_things = Queue(name='collector.request.api_get_things', exchange=exchange,
                             routing_key='collector.request.api_get_things')
    queue_get_thing_by_id = Queue(name='collector.request.api_get_thing_by_id', exchange=exchange,
                                  routing_key='collector.request.api_get_thing_by_id')
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
            with nested(Consumer(consumer_connection, queues=queue_get_things, callbacks=[api_get_things], no_ack=True),
                        Consumer(consumer_connection, queues=queue_get_thing_by_id, callbacks=[api_get_thing_by_id],
                                 no_ack=True),
                        Consumer(consumer_connection, queues=queue_notification, callbacks=[handle_notification],
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



