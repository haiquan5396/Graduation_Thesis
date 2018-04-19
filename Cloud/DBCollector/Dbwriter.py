from influxdb import InfluxDBClient
import json
from kombu import Connection, Consumer, Exchange, Queue, exceptions

clientDB = InfluxDBClient('localhost', 8086, 'root', 'root', 'Collector_DB')
clientDB.create_database('Collector_DB')

BROKER_CLOUD = 'localhost'
consumer_connection = Connection(BROKER_CLOUD)
exchange = Exchange("IoT", type="direct")


def write_db(list_things):
    print("Write to database")
    data_write_db = []
    for thing in list_things['things']:
        for item in thing['items']:
            record = {
                'measurement': item['item_global_id'],
                'tags': {
                    'platform_id': list_things['platform_id'],
                    'thing_type': thing['thing_type'],
                    'thing_name': thing['thing_name'],
                    'thing_global_id': thing['thing_global_id'],
                    'thing_local_id': thing['thing_local_id'],
                    'location': thing['location'],
                    'item_type': item['item_type'],
                    'item_name': item['item_name'],
                    'item_global_id': item['item_global_id'],
                    'item_local_id': item['item_local_id'],
                    'can_set_state': item['can_set_state'],
                },
                'fields': {
                    'item_state': item['item_state'],
                }
            }

            data_write_db.append(record)

    clientDB.write_points(data_write_db)
    print('Updated Database')


def api_write_db(body, message):
    print('vao api write')
    list_things = json.loads(body)
    write_db(list_things)


queue_write_db = Queue(name='dbwriter.request.api_write_db', exchange=exchange,
                         routing_key='dbwriter.request.api_write_db')

while 1:
    try:
        consumer_connection.ensure_connection(max_retries=1)
        with Consumer(consumer_connection, queues=queue_write_db, callbacks=[api_write_db], no_ack=True):
            while True:
                consumer_connection.drain_events()
    except (ConnectionRefusedError, exceptions.OperationalError) as hihi:
        print('Connection lost')
    except consumer_connection.connection_errors:
        print('Connection error')
