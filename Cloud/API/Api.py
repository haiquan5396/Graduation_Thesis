import paho.mqtt.client as mqtt
import ast
import json

broker_cloud = "iot.eclipse.org"
broker_fog = "broker.hivemq.com"

client_mqtt_cloud_1 = mqtt.Client()  # create new instance
client_mqtt_cloud_1.connect(broker_cloud)  # connect to broker


def get_things_global_id():
    print('Call to Registry get global_id that are available')
    list_id = []
    # check_response = 0
    # list_id = []
    # message_registry = {
    #     'caller': "api"
    # }
    #
    # def handle_get_thing_global_id(client, userdata, msg):
    #     nonlocal list_id, check_response
    #     result_registry = ast.literal_eval(msg.payload.decode('utf-8'))
    #     temp = []
    #     for thing in result_registry:
    #         temp.append(thing['thing_global_id'])
    #     list_id = temp
    #     check_response = 1
    #     client_mqtt_cloud_1.unsubscribe('registry/response/api/api_get_things')
    #
    # client_mqtt_cloud_1.publish('registry/request/api_get_things', json.dumps(message_registry))
    # client_mqtt_cloud_1.subscribe('registry/response/api/api_get_things')
    # client_mqtt_cloud_1.message_callback_add('registry/response/api/api_get_things', handle_get_thing_global_id)
    #
    # while check_response == 0:
    #     client_mqtt_cloud_1.loop()
    #     continue
    for thing in api_get_things_info():
        list_id.append(thing['thing_global_id'])
    return list_id


def get_things_global_id_in_platform(platform_id):
    print("Call Registry get thing global id in platform_id")
    list_id = []
    for thing in api_get_things_info():
        if thing['platform_id'] == platform_id:
            list_id.append(thing['thing_global_id'])
    return list_id


def api_get_things_info():
    print('Call to Registry get Things info')
    check_response = 0
    result = []
    message = {
        'caller': "api"
    }

    def handle_api_get_things(client, userdata, msg):
        nonlocal check_response, result
        result = ast.literal_eval(msg.payload.decode('utf-8'))
        check_response = 1
        client_mqtt_cloud_1.message_callback_remove('registry/response/api/api_get_things')
        client_mqtt_cloud_1.unsubscribe('registry/response/api/api_get_things')

    client_mqtt_cloud_1.subscribe('registry/response/api/api_get_things')
    client_mqtt_cloud_1.message_callback_add('registry/response/api/api_get_things', handle_api_get_things)
    client_mqtt_cloud_1.publish('registry/request/api_get_things', json.dumps(message))


    while check_response == 0:
        # print('Trong Loop Info')
        client_mqtt_cloud_1.loop()
        continue
    return result


def api_get_thing_info_in_platform(platform_id):
    print('Call to Registry get information things in platform_id')
    result = []
    things_in_system = api_get_things_info()
    for thing in things_in_system:
        if thing['platform_id'] == platform_id:
            result.append(thing)
    return result


def api_get_thing_state_by_id(thing_global_id):

    print('Call to Collector get Things by id')
    check_response = 0
    result = []
    message = {
        'caller': "api",
        'thing_global_id': thing_global_id
    }

    def handle_api_get_thing_by_id(client, userdata, msg):
        nonlocal check_response, result
        result = json.loads(msg.payload.decode('utf-8'))
        check_response = 1
        client_mqtt_cloud_1.message_callback_remove('collector/response/api/api_get_thing_by_id')
        client_mqtt_cloud_1.unsubscribe('collector/response/api/api_get_thing_by_id')

    client_mqtt_cloud_1.subscribe('collector/response/api/api_get_thing_by_id')
    client_mqtt_cloud_1.message_callback_add('collector/response/api/api_get_thing_by_id', handle_api_get_thing_by_id)
    client_mqtt_cloud_1.publish('collector/request/api_get_thing_by_id', json.dumps(message))

    while check_response == 0:
        client_mqtt_cloud_1.loop()
        continue
    return result


def api_get_things_state_in_platform(platform_id):
    print('Get things state in platform_id')
    check_response = 0
    result = []

    message_collector = {
        'caller': 'api',
        'list_thing_global_id': get_things_global_id_in_platform(platform_id)
    }

    def handle_api_get_things(client, userdata, msg):
        nonlocal check_response, result
        result = ast.literal_eval(msg.payload.decode('utf-8'))
        check_response = 1
        # client_mqtt_cloud_1.message_callback_remove('collector/response/api/api_get_things')
        # client_mqtt_cloud_1.unsubscribe('collector/response/api/api_get_things')

    client_mqtt_cloud_1.publish('collector/request/api_get_things', json.dumps(message_collector))
    client_mqtt_cloud_1.subscribe('collector/response/api/api_get_things')
    client_mqtt_cloud_1.message_callback_add('collector/response/api/api_get_things', handle_api_get_things)
    while check_response == 0:
        client_mqtt_cloud_1.loop()
        continue

    return result


def api_get_things_state():
    print('Get the state of the Things that are available')
    check_response = 0
    result = []

    message_collector = {
        'caller': 'api',
        'list_thing_global_id': get_things_global_id()
    }

    def handle_api_get_things(client, userdata, msg):
        # print("all")
        nonlocal check_response, result
        result = ast.literal_eval(msg.payload.decode('utf-8'))
        check_response = 1
        client_mqtt_cloud_1.message_callback_remove('collector/response/api/api_get_things')
        client_mqtt_cloud_1.unsubscribe('collector/response/api/api_get_things')

    client_mqtt_cloud_1.subscribe('collector/response/api/api_get_things')
    client_mqtt_cloud_1.message_callback_add('collector/response/api/api_get_things', handle_api_get_things)
    client_mqtt_cloud_1.publish('collector/request/api_get_things', json.dumps(message_collector))

    while check_response == 0:
        client_mqtt_cloud_1.loop()
        continue

    return result


# def api_get_thing_state_by_id(thing_global_id):
#     print('Get the state of the Things by thing_global_id')
#     check_response = 0
#     result = []
#
#     message_collector = {
#         'caller': 'api',
#         'thing_global_id': thing_global_id
#     }
#
#     def handle_api_get_thing_by_id(client, userdata, msg):
#         nonlocal check_response, result
#         result = json.loads(msg.payload.decode('utf-8'))
#         check_response = 1
#         client_mqtt_cloud_1.message_callback_remove('collector/response/api/api_get_thing_by_id')
#         client_mqtt_cloud_1.unsubscribe('collector/response/api/api_get_thing_by_id')
#
#     client_mqtt_cloud_1.subscribe('collector/response/api/api_get_thing_by_id')
#     client_mqtt_cloud_1.message_callback_add('collector/response/api/api_get_thing_by_id', handle_api_get_thing_by_id)
#     client_mqtt_cloud_1.publish('collector/request/api_get_thing_by_id', json.dumps(message_collector))
#
#     while check_response == 0:
#         client_mqtt_cloud_1.loop()
#         continue
#
#     return result


def api_get_item_state_by_id(thing_global_id, item_global_id):
    list_thing = api_get_things_state()
    for thing in list_thing:
        if thing['thing_global_id'] == thing_global_id:
            for item in thing['items']:
                if item['item_global_id'] == item_global_id:
                    return item


def api_set_state(thing_global_id, item_global_id, new_state):
    print('Call to Registry get information things')
    client_mqtt_fog_1 = mqtt.Client()  # create new instance
    client_mqtt_fog_1.connect(broker_fog)  # connect to broker
    result = []
    things_in_system = api_get_things_info()
    for thing in things_in_system:
        if thing['thing_global_id'] == thing_global_id:
            for item in thing['items']:
                if item['item_global_id'] == item_global_id:
                    result = {
                        'caller': 'api',
                        'thing_local_id': thing['thing_local_id'],
                        'thing_name': thing['thing_name'],
                        'thing_type': thing['thing_type'],
                        'thing_location': thing['thing_location'],
                        'item_local_id': item['item_local_id'],
                        'item_name': item['item_name'],
                        'item_type': item['item_type'],
                        'new_state': new_state
                    }
                platform_id = thing['platform_id']
                client_mqtt_fog_1.publish('{}/request/api_set_state'.format(platform_id), json.dumps(result))

    print("Send set state")
    return result
