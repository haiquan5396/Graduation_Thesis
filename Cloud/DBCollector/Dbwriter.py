import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import json

clientDB = InfluxDBClient('localhost', 8086, 'root', 'root', 'Collector_DB')
clientDB.create_database('Collector_DB')

broker_cloud = "iot.eclipse.org"
clientMQTT = mqtt.Client()  # create new instance
clientMQTT.connect(broker_cloud)  # connect to broker


def write_db(list_things):
    print("Write to database")
    data_write_db = []
    for thing in list_things['things']:
        for item in thing['items']:
            record = {
                'measurement': 'collector',
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


def api_write_db(client, userdata, msg):
    print('vao api write')
    list_things = json.loads(msg.payload.decode('utf-8'))
    write_db(list_things)


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

clientMQTT.subscribe('dbwriter/request/api_write_db')
clientMQTT.message_callback_add('dbwriter/request/api_write_db', api_write_db)

clientMQTT.subscribe('dbwriter/request/api_get_thing_by_global_id')
clientMQTT.message_callback_add('dbwriter/request/api_get_thing_by_global_id', api_get_thing_by_global_id)

clientMQTT.subscribe('dbwriter/request/api_get_things')
clientMQTT.message_callback_add('dbwriter/request/api_get_things', api_get_things)

clientMQTT.loop_forever()