import paho.mqtt.client as mqtt
import json
import requests
import hashlib

platform_id = 'Quan'

host_homeAssistant = '192.168.0.110'
port_homeAssistant = '8123'

pre_info = []

broker_fog = "localhost"
clientMQTT = mqtt.Client()  # create new instance
clientMQTT.connect(broker_fog)  # connect to broker


def connect_platform(url):
    while True:
        try:
            # url = 'http://' + host_homeAssistant + ':' + port_homeAssistant + '/api/states'
            response = requests.get(url).json()
            return response
        except:
            print("Error connect to Platform")
            continue


def get_states():
    print('Get state of all things')

    url = 'http://' + host_homeAssistant + ':' + port_homeAssistant + '/api/states'
    response = connect_platform(url)
    list_thing = {
        'platform_id': str(platform_id),
        'things': []
    }

    for thing in response:
        # print(thing['entity_id'])
        thing_type = thing['entity_id'].split(".")[0]
        if thing_type != 'group' and thing_type != 'automation':
            # Trong HomeAssistant do quản lý chỉ đến mức Thing nên ta coi các item của 1 thing là chính nó.
            # VD: đèn là một thing và có item chính là bản thân cái đèn đó
            item_type = thing_type
            item_state = thing['state']
            # if item_type == 'sensor' and thing['entity_id'] == 'sensor.temperature':
            #     item_state = int(thing['state'])
            # else:
            #     item_state = thing['state']

            thing_temp = {
                'thing_type': thing_type,
                'thing_name': thing['attributes']['friendly_name'],
                'thing_global_id': platform_id + '/' + thing['entity_id'],
                'thing_local_id': thing['entity_id'],
                'location': get_location_of_thing(response, thing['entity_id']),
                'items': [
                    {
                        'item_type': item_type,
                        'item_name': thing['attributes']['friendly_name'],
                        'item_global_id': platform_id + '/' + thing['entity_id'] + '/' + thing['entity_id'],
                        'item_local_id': thing['entity_id'],
                        'item_state': item_state,
                        'can_set_state': check_can_set_state(item_type),

                    }
                ]
            }
            list_thing['things'].append(thing_temp)

    return list_thing


def check_can_set_state(item_type):
    url = 'http://' + host_homeAssistant + ':' + port_homeAssistant + '/api/services'
    response = connect_platform(url)
    for service in response:
        if service['domain'] == item_type:
            return True
    return False


def get_location_of_thing(list_things, thing_id):
    for temp in list_things:
        if temp['entity_id'].split(".")[0] == 'group':
            for thing_in_group in temp['attributes']['entity_id']:
                if thing_in_group == thing_id:
                    return temp['entity_id'].split(".")[1]
    return None


def check_configuration_changes():
    global pre_info
    print('Check for changes')

    url = 'http://' + host_homeAssistant + ':' + port_homeAssistant + '/api/states'
    response = connect_platform(url)

    now_info = []

    for thing in response:
        # print(thing['entity_id'])
        thing_type = thing['entity_id'].split(".")[0]
        if thing_type != 'group' and thing_type != 'automation':
            # Trong HomeAssistant do quản lý chỉ đến mức Thing nên ta coi các item của 1 thing là chính nó.
            # VD: đèn là một thing và có item chính là bản thân cái đèn đó

            thing_temp = {
                'thing_type': thing_type,
                'thing_name': thing['attributes']['friendly_name'],
                'thing_global_id': platform_id + '/' + thing['entity_id'],
                'thing_local_id': thing['entity_id'],
                'location': get_location_of_thing(response, thing['entity_id']),
                'items': [
                    {
                        'item_type': thing_type,
                        'item_name': thing['attributes']['friendly_name'],
                        'item_global_id': platform_id + '/' + thing['entity_id'] + '/' + thing['entity_id'],
                        'item_local_id': thing['entity_id'],
                        'can_set_state': check_can_set_state(thing_type),

                    }
                ]
            }
            now_info.append(thing_temp)

    hash_now = hashlib.md5(str(now_info).encode())
    hash_pre = hashlib.md5(str(pre_info).encode())
    if hash_now.hexdigest() == hash_pre.hexdigest():
        return {
            'have_change': False,
            'now_info': None,
            'platform_id': platform_id,
        }

    else:
        pre_info = now_info
        return {
            'have_change': True,
            'now_info': now_info,
            'platform_id': platform_id,
        }


def init():
    print('Init and get platform_id from Registry')
    message = {
        'platform': 'Home Assistant',
        'host': host_homeAssistant,
        'port': port_homeAssistant,
    }

    topic_response = 'registry/response/' + host_homeAssistant + '/' + port_homeAssistant

    def handle_init(client, userdata, msg):
        print('Handle_init')
        global platform_id
        platform_id = json.loads(msg.payload.decode('utf-8'))['platform_id']
        print ('Platform_id recived: ', platform_id)
        clientMQTT.unsubscribe(topic_response)

        clientMQTT.subscribe(str(platform_id) + '/request/api_get_states')
        clientMQTT.message_callback_add(str(platform_id) + '/request/api_get_states', api_get_states)

        clientMQTT.subscribe(str(platform_id) + '/request/api_check_configuration_changes')
        clientMQTT.message_callback_add(str(platform_id) + '/request/api_check_configuration_changes', api_check_configuration_changes)

        clientMQTT.subscribe(str(platform_id) + '/request/api_set_state')
        clientMQTT.message_callback_add(str(platform_id) + '/request/api_set_state', api_set_state)

    clientMQTT.subscribe(topic_response)
    clientMQTT.message_callback_add(topic_response, handle_init)
    clientMQTT.publish('registry/request/api_add_platform', json.dumps(message))


def api_get_states(client, userdata, msg):
    reply_to = json.loads(msg.payload.decode('utf-8'))['reply_to']
    message_respone = get_states()
    message_respone['reply_to'] = reply_to
    clientMQTT.publish('driver/response/filter/api_get_states', json.dumps(message_respone))


def api_check_configuration_changes(client, userdata, msg):
    message_response = check_configuration_changes()
    message_response['reply_to'] = json.loads(msg.payload.decode('utf-8'))['reply_to']
    print ('api_check_configuration_changes')
    clientMQTT.publish('driver/response/forwarder/api_check_configuration_changes', json.dumps(message_response))


def api_set_state(client, userdata, msg):
    message = json.loads(msg.payload.decode('utf-8'))
    caller = message['caller']
    thing_local_id = message['thing_local_id']
    thing_type = message['thing_type']
    item_local_id = message['item_local_id']
    item_type = message['item_type']
    new_state = message['new_state']
    print('Set sate of {} to {}'.format(thing_local_id, new_state))
    if item_type == 'light':
        print('Call Service ')
        if new_state == "ON":
            url = 'http://' + host_homeAssistant + ':' + port_homeAssistant + '/api/services/light/turn_on'
            data = {"entity_id": item_local_id}
            response = requests.post(url, json.dumps(data))
        else:
            url = 'http://' + host_homeAssistant + ':' + port_homeAssistant + '/api/services/light/turn_off'
            data = {"entity_id": item_local_id}
            response = requests.post(url, json.dumps(data))
    else:
        print('Type are not support')


init()

clientMQTT.loop_forever()
