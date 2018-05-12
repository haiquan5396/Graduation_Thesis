import json
from kombu import Producer, Connection, Consumer, exceptions, Exchange, Queue
from kombu.utils.compat import nested
from influxdb import InfluxDBClient
import sys
from dateutil import tz
from datetime import datetime
MODE_CODE = "DEPLOY"

# if MODE_CODE == 'DEPlOY':
BROKER_CLOUD = sys.argv[1]
HOST_INFLUXDB = sys.argv[2]
# else:
#     BROKER_CLOUD = "localhost"
#     HOST_INFLUXDB = "localhost"

clientDB = InfluxDBClient(HOST_INFLUXDB, 8086, 'root', 'root', 'Collector_DB')
clientDB.create_database('Collector_DB')

producer_connection = Connection(BROKER_CLOUD)
consumer_connection = Connection(BROKER_CLOUD)
exchange = Exchange("IoT", type="direct")


def get_item_state(list_item_global_id):
    items = []
    for item_global_id in list_item_global_id:
        query_statement = 'SELECT * FROM \"' + item_global_id + '\" ORDER BY time DESC LIMIT 1 '
        query_result = clientDB.query(query_statement)

        for item in query_result:
            item_state = {
                'item_global_id': item[0]['item_global_id'],
                'item_state': item[0]['item_state'],
                # 'last_changed': covert_time_to_correct_time_zone(item[0]['time']),
                'last_changed': item[0]['time'],
                'thing_global_id': item[0]['thing_global_id']
            }
            items.append(item_state)
    return items


def get_item_state_history(list_item_global_id, start_time, end_time):
    items=[]
    for item_global_id in list_item_global_id:
        query_statement = """SELECT * FROM \"{}\" where time > \'{}\' AND time < \'{}\'""".format(item_global_id, start_time, end_time)
        # print(query_statement)
        query_result = clientDB.query(query_statement)
        query_result = list(query_result.get_points())
        if len(query_result) > 0:
            item = {
                'item_global_id': query_result[0]['item_global_id'],
                'thing_global_id': query_result[0]['thing_global_id'],
                'history': []
            }

            for item_history in query_result:
                item_state = {
                    'last_changed': item_history['time'],
                    'item_state': item_history['item_state']
                }
                item['history'].append(item_state)
            items.append(item)
    return items
#
# get_item_state_history(['fc910950-6a5c-4a78-9e85-b870143e23e0-light.red_light-light.red_light', 'fc910950-6a5c-4a78-9e85-b870143e23e0-light.green_light-light.green_light'], '2018-05-05T10:06:12Z', '2018-05-05T10:07:12Z' )


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
    # print("Done: {}".format(items))


def api_get_item_state_history(body, message):
    print("API get_item_state_history")
    # Message {'list_item_global_id': [], 'reply_to': " ", }
    list_item_global_id = json.loads(body)["list_item_global_id"]
    reply_to = json.loads(body)['reply_to']
    start_time = json.loads(body)["start_time"]
    end_time = json.loads(body)["end_time"]
    items = get_item_state_history(list_item_global_id, start_time, end_time)
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

# def covert_time_to_correct_time_zone(time_utc):
#     from_zone = tz.gettz('UTC')
#     to_zone = tz.tzlocal()
#     time_utc = time_utc.split('.')[0]
#     time_utc = datetime.strptime(time_utc, '%Y-%m-%dT%H:%M:%S')
#     time_utc = time_utc.replace(tzinfo=from_zone)
#     time_convert = time_utc.astimezone(to_zone)
#     return str(time_convert)

queue_get_item_state = Queue(name='dbreader.request.api_get_item_state', exchange=exchange, routing_key='dbreader.request.api_get_item_state')
queue_get_item_state_history = Queue(name='dbreader.request.api_get_item_state_history', exchange=exchange, routing_key='dbreader.request.api_get_item_state_history')


while 1:
    try:
        consumer_connection.ensure_connection(max_retries=1)
        with nested (Consumer(consumer_connection, queues=queue_get_item_state, callbacks=[api_get_item_state], no_ack=True),
                     Consumer(consumer_connection, queues=queue_get_item_state_history, callbacks=[api_get_item_state_history], no_ack=True)):
            while True:
                consumer_connection.drain_events()
    except (ConnectionRefusedError, exceptions.OperationalError):
        print('Connection lost')
    except consumer_connection.connection_errors:
        print('Connection error')
