from kombu import Connection, Producer, Consumer, Queue, uuid, Exchange
import json
import sys
import socket
import datetime

MODE_CODE = "DEVELOP"

# if MODE_CODE == "DEPLOY":
BROKER_CLOUD = sys.argv[1]
# else:
# BROKER_CLOUD="localhost"


rabbitmq_connection = Connection(BROKER_CLOUD)
exchange = Exchange("IoT", type="direct")

#=====================================================
from flask import Flask, jsonify, request

app = Flask(__name__)


@app.route('/api/platforms', defaults={'platform_status': 'active'}, methods=['GET'])
@app.route('/api/platforms/<platform_status>', methods=['GET'])
def api_get_platforms(platform_status):
    return jsonify(get_list_platforms(platform_status))


@app.route('/api/things', defaults={'thing_status': 'active', 'item_status': 'active'}, methods=['GET'])
@app.route('/api/things/<thing_status>/<item_status>', methods=['GET'])
def api_get_things_state(thing_status, item_status):
    # print("thing: {} item: {}".format(thing_status, item_status))
    # print(BROKER_CLOUD)
    return jsonify(get_things_state(thing_status, item_status))


@app.route('/api/things/<thing_global_id>', methods=['GET'])
def api_get_thing_state_by_global_id(thing_global_id):
    return jsonify(get_thing_state_by_global_id(thing_global_id))


@app.route('/api/things/platform_id/<platform_id>', defaults={'thing_status': 'active', 'item_status': 'active'}, methods=['GET'])
@app.route('/api/things/platform_id/<platform_id>/<thing_status>/<item_status>', methods=['GET'])
def api_get_things_state_by_platform_id(platform_id, thing_status, item_status):
    return jsonify(get_things_state_by_platform_id(platform_id, thing_status, item_status))


@app.route('/api/history/things/<start_time>/<end_time>', defaults={'thing_status': 'active', 'item_status': 'active'}, methods=['GET'])
@app.route('/api/history/things/<thing_status>/<item_status>/<start_time>/<end_time>', methods=['GET'])
def api_get_thing_state_history(thing_status, item_status, start_time, end_time):
    try:
        datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
        return jsonify(get_things_state_history(thing_status, item_status, start_time, end_time))
    except ValueError:
        return jsonify({'error': 'Incorrect data format, should be %Y-%m-%d %H:%M:%S'})




@app.route('/api/history/things/<thing_global_id>/<start_time>/<end_time>', methods=['GET'])
def api_get_thing_state_history_by_global_id(thing_global_id, start_time, end_time):
    try:
        datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
        return jsonify(get_things_state_history_by_global_id(thing_global_id, start_time, end_time))
    except ValueError:
        return jsonify({'error': 'Incorrect data format, should be %Y-%m-%d %H:%M:%S'})


@app.route('/api/history/things/platform_id/<platform_id>/<start_time>/<end_time>', defaults={'thing_status': 'active', 'item_status': 'active'}, methods=['GET'])
@app.route('/api/history/things/platform_id/<platform_id>/<thing_status>/<item_status>/<start_time>/<end_time>', methods=['GET'])
def api_get_things_state_history_by_platform_id(platform_id, thing_status, item_status, start_time, end_time):
    try:
        datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
        return jsonify(get_things_state_history_by_platform_id(platform_id, thing_status, item_status, start_time, end_time))
    except ValueError:
        return jsonify({'error': 'Incorrect data format, should be %Y-%m-%d %H:%M:%S'})


@app.route('/api/items', methods=['POST'])
def api_set_state():
    request_message = request.json
    thing_global_id = request_message['thing_global_id']
    item_global_id = request_message['item_global_id']
    new_state = request_message['new_state']
    set_state(thing_global_id, item_global_id, new_state)
    return jsonify(request.json)


def get_things_state_history(thing_status, item_status, start_time, end_time):
    print("API get things state history")
    try:
        list_things_info = get_things_info(thing_status, item_status)['things']
        list_things_state = get_things_state_history_by_list_thing(list_things_info, start_time, end_time)
        return list_things_state
    except (KeyError, IndexError):
        error = {
            'error': "Can not connect to service"
        }

        return error


def get_things_state_history_by_global_id(thing_global_id, start_time, end_time):
    try:
        list_things_info = get_thing_info_by_global_id(thing_global_id)['things']
        list_things_state = get_things_state_history_by_list_thing(list_things_info, start_time, end_time)
        return list_things_state
    except (KeyError, IndexError):
        error = {
            'error': "Can not connect to service"
        }

        return error


def get_things_state_history_by_platform_id(platform_id, thing_status, item_status, start_time, end_time):
    try:
        list_things_info = get_things_info_by_platform_id(platform_id, thing_status, item_status)['things']
        list_things_state = get_things_state_history_by_list_thing(list_things_info, start_time, end_time)
        return list_things_state

    except (KeyError, IndexError):
        error = {
            'error': "Can not connect to service"
        }

        return error


def get_things_state_history_by_list_thing(list_things_info, start_time, end_time):
    print("get_things_state_history_by_list_thing")
    list_item_global_id = []
    for thing_info in list_things_info:
        for item_info in thing_info['items']:
            list_item_global_id.append(item_info['item_global_id'])

    list_item_state = get_items_state_history(list_item_global_id, start_time, end_time)['items']
    # print(list_item_state[0])
    for item_collect in list_item_state:
        for thing_info in list_things_info:
            if item_collect['thing_global_id'] == thing_info['thing_global_id']:
                for item_info in thing_info['items']:
                    if item_info['item_global_id'] == item_collect['item_global_id']:
                        item_info['history'] = item_collect['history']
                        break
                break

    for thing in list_things_info:
        for item in thing['items']:
            if 'history' in item:
                continue
            else:
                item['history'] = []
    list_things_state = list_things_info
    return list_things_state


def get_items_state_history(list_item_global_id, start_time, end_time):
    message_request = {
        'list_item_global_id': list_item_global_id,
        'reply_to': "dbreader.response.api.api_get_item_state_history",
        'start_time': start_time,
        'end_time': end_time
    }

    # request to api_get_things of Registry
    queue_response = Queue(name='dbreader.response.api.api_get_item_state_history', exchange=exchange,
                           routing_key='dbreader.response.api.api_get_item_state_history')
    request_routing_key = 'dbreader.request.api_get_item_state_history'
    message_response = request(rabbitmq_connection, message_request, exchange, request_routing_key, queue_response)

    # message_response = {"items": [{'item_global_id': "", 'item_state': "", 'last_changed': ""}]}
    return message_response


def get_list_platforms(platform_status):
    print("API list platforms from Registry")

    if platform_status in ['active', "inactive", "all"]:
        message_request = {
            'reply_to': 'registry.response.api.api_get_list_platforms',
            'platform_status': platform_status
        }

        #request to api_get_list_platform of Registry
        queue_response = Queue(name='registry.response.api.api_get_list_platforms', exchange=exchange, routing_key='registry.response.api.api_get_list_platforms')
        request_routing_key = 'registry.request.api_get_list_platforms'
        message_response = request(rabbitmq_connection, message_request, exchange, request_routing_key, queue_response)
        if 'list_platforms' in message_response:
            return message_response['list_platforms']
        else:
            # have error
            return message_response
    else:
        return None


def get_things_info(thing_status, item_status):
    print("API get things info with thing_status and item_status")

    if (thing_status in ["active", "inactive", "all"]) \
            and (item_status in ["active", "inactive", "all"]):

        message_request = {
            'reply_to': 'registry.response.api.api_get_things',
            'thing_status': thing_status,
            'item_status': item_status
        }

        # request to api_get_things of Registry
        queue_response = Queue(name='registry.response.api.api_get_things', exchange=exchange, routing_key='registry.response.api.api_get_things')
        request_routing_key = 'registry.request.api_get_things'
        message_response = request(rabbitmq_connection, message_request, exchange, request_routing_key, queue_response)
        return message_response
    else:
        return None


def get_things_state(thing_status, item_status):
    try:
        list_things_info = get_things_info(thing_status, item_status)['things']
        list_things_state = get_things_state_by_list_thing(list_things_info)
        return list_things_state

    except (KeyError, IndexError):
        error = {
            'error': "Can not connect to service"
        }

        return error


def get_items_state(list_item_global_id):
    message_request = {
        'list_item_global_id': list_item_global_id,
        'reply_to': "dbreader.response.api.api_get_item_state"
    }

    # request to api_get_things of Registry
    queue_response = Queue(name='dbreader.response.api.api_get_item_state', exchange=exchange,
                           routing_key='dbreader.response.api.api_get_item_state')
    request_routing_key = 'dbreader.request.api_get_item_state'
    message_response = request(rabbitmq_connection, message_request, exchange, request_routing_key, queue_response)
    # message_response = {"items": [{'item_global_id': "", 'item_state': "", 'last_changed': ""}]}
    return message_response


def get_thing_info_by_global_id(thing_global_id):

    print("API get things by thing_global_id")
    message_request = {
        'reply_to': 'registry.response.api.api_get_thing_by_global_id',
        'thing_global_id': thing_global_id
    }

    # request to api_get_things of Registry
    queue_response = Queue(name='registry.response.api.api_get_thing_by_global_id', exchange=exchange, routing_key='registry.response.api.api_get_thing_by_global_id')
    request_routing_key = 'registry.request.api_get_thing_by_global_id'
    message_response = request(rabbitmq_connection, message_request, exchange, request_routing_key, queue_response)

    return message_response


def get_thing_state_by_global_id(thing_global_id):
    try:
        list_things_info = get_thing_info_by_global_id(thing_global_id)['things']
        list_things_state = get_things_state_by_list_thing(list_things_info)
        return list_things_state
    except (KeyError, IndexError):
        error = {
            'error': "Can not connect to service"
        }
        return error


def get_things_state_by_platform_id(platform_id, thing_status, item_status):
    try:
        list_things_info = get_things_info_by_platform_id(platform_id, thing_status, item_status)['things']
        list_things_state = get_things_state_by_list_thing(list_things_info)
        return list_things_state
    except (KeyError, IndexError):
        error = {
            'error': "Can not connect to service"
        }
        return error


def get_things_info_by_platform_id(platform_id, thing_status, item_status):

    print("API get things info in platform_id with thing_status and item_status")

    if (thing_status in ["active", "inactive", "all"]) \
            and (item_status in ["active", "inactive", "all"]):

        message_request = {
            'reply_to': 'registry.response.api.api_get_things_by_platform_id',
            'thing_status': thing_status,
            'item_status': item_status,
            'platform_id': platform_id
        }

        # # request to api_get_things of Registry
        queue_response = Queue(name='registry.response.api.api_get_things_by_platform_id', exchange=exchange, routing_key='registry.response.api.api_get_things_by_platform_id')
        request_routing_key = 'registry.request.api_get_things_by_platform_id'
        message_response = request(rabbitmq_connection, message_request, exchange, request_routing_key, queue_response)
        return message_response
    else:
        return None


def get_things_state_by_list_thing(list_things_info):

    list_item_global_id = []
    for thing_info in list_things_info:
        for item_info in thing_info['items']:
            list_item_global_id.append(item_info['item_global_id'])

    list_item_state = get_items_state(list_item_global_id)['items']
    # print(list_item_state[0])
    for item_collect in list_item_state:
        for thing_info in list_things_info:
            if item_collect['thing_global_id'] == thing_info['thing_global_id']:
                for item_info in thing_info['items']:
                    if item_info['item_global_id'] == item_collect['item_global_id']:
                        item_info['item_state'] = item_collect['item_state']
                        item_info['last_changed'] = item_collect['last_changed']
                        break
                break

    list_things_state = list_things_info
    return list_things_state


def set_state(thing_global_id, item_global_id, new_state):
    thing = get_thing_info_by_global_id(thing_global_id)['things'][0]
    for item in thing['items']:
        if item['item_global_id'] == item_global_id:
            message_request ={
                'thing_local_id': thing['thing_local_id'],
                'thing_name': thing['thing_name'],
                'thing_type': thing['thing_type'],
                'location': thing['location'],
                'item_local_id': item['item_local_id'],
                'item_name': item['item_name'],
                'item_type': item['item_type'],
                'new_state': new_state,
                'platform_id': thing['platform_id']
            }
            break

    request_routing_key = 'driver.request.api_set_state'
    rabbitmq_connection.ensure_connection()
    with Producer(rabbitmq_connection) as producer:
        producer.publish(
            json.dumps(message_request),
            exchange=exchange.name,
            routing_key=request_routing_key,
            retry=True
        )


def request(conn, message_request, exchange, request_routing_key, queue_response):
    # request_routing_key = 'registry.request.api_get_things_by_platform_id'
    conn.ensure_connection()
    with Producer(conn) as producer:
        producer.publish(
            json.dumps(message_request),
            exchange=exchange.name,
            routing_key=request_routing_key,
            declare=[queue_response],
            retry=True
        )

    message_response = None

    def on_response(body, message):
        nonlocal message_response
        message_response = json.loads(body)

    with Consumer(conn, queues=queue_response, callbacks=[on_response], no_ack=True):
        try:
            while message_response is None:
                conn.drain_events(timeout=2)
        except socket.timeout:
            return {
                'error': 'Can not connect to service'
            }

    return message_response


if __name__ == '__main__':

    app.run(host='0.0.0.0', port=5000, threaded=True)
