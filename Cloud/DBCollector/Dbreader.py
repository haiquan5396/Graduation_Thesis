import ast
import json
import threading
from kombu import Producer, Connection, Consumer, exceptions, Exchange, Queue, uuid
from kombu.utils.compat import nested
from influxdb import InfluxDBClient

clientDB = InfluxDBClient('localhost', 8086, 'root', 'root', 'Collector_DB')
clientDB.create_database('Collector_DB')

BROKER_CLOUD = "localhost"

producer_connection = Connection(BROKER_CLOUD)
consumer_connection = Connection(BROKER_CLOUD)
exchange = Exchange("IoT", type="direct")


def get_item_state(list_item_global_id):
    items = []
    for item_global_id in list_item_global_id:
        query_statement = 'SELECT * FROM \"' + item_global_id + '\" ORDER BY time DESC LIMIT 1'
        query_result = clientDB.query(query_statement)

        for item in query_result:
            item_state = {
                'item_global_id': item[0]['item_global_id'],
                'item_state': item[0]['item_state'],
                'last_changed': item[0]['time'],
                'thing_global_id': item[0]['thing_global_id']
            }
            items.append(item_state)
    return items


def api_get_item_state(body, message):
    print("API get_item_state")
    # Message {'list_item_global_id': [], 'reply_to': " ", }
    list_item_global_id = json.loads(body)["list_item_global_id"]
    reply_to = json.loads(body)['reply_to']
    items = get_item_state(list_item_global_id)
    message_response = {
        "items": items
    }
    producer_connection.ensure_connection()
    with Producer(producer_connection) as producer:
        producer.publish(
            json.dumps(message_response),
            exchange=exchange.name,
            routing_key=reply_to,
            retry=True
        )
    print("Done: {}".format(items))


queue_get_item_state = Queue(name='dbreader.request.api_get_item_state', exchange=exchange, routing_key='dbreader.request.api_get_item_state')

while 1:
    try:
        consumer_connection.ensure_connection(max_retries=1)
        with Consumer(consumer_connection, queues=queue_get_item_state, callbacks=[api_get_item_state], no_ack=True):
            while True:
                consumer_connection.drain_events()
    except (ConnectionRefusedError, exceptions.OperationalError):
        print('Connection lost')
    except consumer_connection.connection_errors:
        print('Connection error')
