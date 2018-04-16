import ast
import json
import threading
from kombu import Producer, Connection, Consumer, exceptions, Exchange, Queue, uuid
from kombu.utils.compat import nested
from influxdb import InfluxDBClient

clientDB = InfluxDBClient('localhost', 8086, 'root', 'root', 'Collector_DB')
clientDB.create_database('Collector_DB')

BROKER_CLOUD = "localhost"
BROKER_FOG = "localhost"

producer_connection = Connection(BROKER_CLOUD)
consumer_connection = Connection(BROKER_CLOUD)


def get_thing_by_global_id(thing_global_id):
    print('Get thing by thing_global_id')
    temp = "SELECT *, item_global_id FROM collector WHERE thing_global_id =\'"+thing_global_id+"\' GROUP BY item_global_id ORDER BY time DESC LIMIT 1"
    query_result = clientDB.query(temp)

    thing = {
        'thing_global_id': thing_global_id,
        'items': []
    }

    for item in query_result:
        temp = {
            'item_global_id': item[0]['item_global_id'],
            'item_type': item[0]['item_type'],
            'item_state': item[0]['item_state']
        }
        thing['items'].append(temp)

    return thing


def get_things(list_thing_global_id):
    things = []
    for thing_global_id in list_thing_global_id:
        things.append(get_thing_by_global_id(thing_global_id))
    return things


def api_get_thing_by_global_id(client, userdata, msg):
    print("api_get_thing_by_global_id")
    caller = json.loads(msg.payload.decode('utf-8'))['caller']
    thing_global_id = json.loads(msg.payload.decode('utf-8'))['thing_global_id']
    # print(get_thing_by_global_id(thing_global_id))
    clientMQTT.publish('dbwriter/response/{}/api_get_thing_by_global_id'.format(caller), json.dumps(get_thing_by_global_id(thing_global_id)))


def api_get_things(client, userdata, msg):
    print("api_get_things")
    caller = json.loads(msg.payload.decode('utf-8'))['caller']
    list_thing_global_id = json.loads(msg.payload.decode('utf-8'))['list_thing_global_id']
    # print(get_thing_by_global_id(thing_global_id))
    clientMQTT.publish('dbwriter/response/{}/api_get_things'.format(caller), str(get_things(list_thing_global_id)))
