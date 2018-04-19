from kombu import Connection, Producer, Consumer, Queue, uuid, Exchange
import json

BROKER_CLOUD = "localhost"
rabbitmq_connection = Connection(BROKER_CLOUD)
exchange = Exchange("IoT", type="direct")

#=====================================================
from flask import Flask, jsonify, request

app = Flask(__name__)


@app.route('/api/platforms', defaults={'platform_status': 'active'}, methods=['GET'])
@app.route('/api/platforms/<platform_status>', methods=['GET'])
def api_get_platforms(platform_status):
    return jsonify(get_list_platforms(platform_status)), 201


@app.route('/api/things', defaults={'thing_status': 'active', 'item_status': 'active'}, methods=['GET'])
@app.route('/api/things/<thing_status>/<item_status>', methods=['GET'])
def api_get_things_state(thing_status, item_status):
    print("thing: {} item: {}".format(thing_status, item_status))
    return jsonify(get_things_state(thing_status, item_status))


@app.route('/api/things/<thing_global_id>', methods=['GET'])
def api_get_thing_state_by_global_id(thing_global_id):
    return jsonify(get_thing_state_by_global_id(thing_global_id))


@app.route('/api/things/platform_id/<platform_id>', defaults={'thing_status': 'active', 'item_status': 'active'}, methods=['GET'])
@app.route('/api/things/platform_id/<platform_id>/<thing_status>/<item_status>', methods=['GET'])
def api_get_things_state_by_platform_id(platform_id, thing_status, item_status):
    return jsonify(get_things_state_by_platform_id(platform_id, thing_status, item_status))


@app.route('/api/items', methods=['POST'])
def api_set_state():
    request_message = request.json
    thing_global_id = request_message['thing_global_id']
    item_global_id = request_message['item_global_id']
    new_state = request_message['new_state']
    set_state(thing_global_id, item_global_id, new_state)
    return jsonify(request.json)


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
        rabbitmq_connection.ensure_connection()
        with Producer(rabbitmq_connection) as producer:
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

        with Consumer(rabbitmq_connection, queues=queue_response, callbacks=[on_response], no_ack=True):
            while message_response is None:
                rabbitmq_connection.drain_events()

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

        #request to api_get_things of Registry
        queue_response = Queue(name='registry.response.api.api_get_things', exchange=exchange, routing_key='registry.response.api.api_get_things')
        request_routing_key = 'registry.request.api_get_things'
        rabbitmq_connection.ensure_connection()
        with Producer(rabbitmq_connection) as producer:
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

        with Consumer(rabbitmq_connection, queues=queue_response, callbacks=[on_response], no_ack=True):
            while message_response is None:
                rabbitmq_connection.drain_events()

        return message_response
    else:
        return None


def get_things_state(thing_status, item_status):

    list_things_info = get_things_info(thing_status, item_status)['things']
    list_things_state = get_things_state_by_list_thing(list_things_info)
    return list_things_state


def get_items_state(list_item_global_id):
    message_request = {
        'list_item_global_id': list_item_global_id,
        'reply_to': "dbreader.response.api.api_get_item_state"
    }

    # request to api_get_things of Registry
    queue_response = Queue(name='dbreader.response.api.api_get_item_state', exchange=exchange,
                           routing_key='dbreader.response.api.api_get_item_state')
    request_routing_key = 'dbreader.request.api_get_item_state'
    rabbitmq_connection.ensure_connection()
    with Producer(rabbitmq_connection) as producer:
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

    with Consumer(rabbitmq_connection, queues=queue_response, callbacks=[on_response], no_ack=True):
        while message_response is None:
            rabbitmq_connection.drain_events()

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
    rabbitmq_connection.ensure_connection()
    with Producer(rabbitmq_connection) as producer:
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

    with Consumer(rabbitmq_connection, queues=queue_response, callbacks=[on_response], no_ack=True):
        while message_response is None:
            rabbitmq_connection.drain_events()

    return message_response


def get_thing_state_by_global_id(thing_global_id):
    list_things_info = get_thing_info_by_global_id(thing_global_id)['things']
    list_things_state = get_things_state_by_list_thing(list_things_info)
    return list_things_state


def get_things_state_by_platform_id(platform_id, thing_status, item_status):
    list_things_info = get_things_info_by_platform_id(platform_id, thing_status, item_status)['things']
    list_things_state = get_things_state_by_list_thing(list_things_info)
    return list_things_state


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

        # request to api_get_things of Registry
        queue_response = Queue(name='registry.response.api.api_get_things_by_platform_id', exchange=exchange, routing_key='registry.response.api.api_get_things_by_platform_id')
        request_routing_key = 'registry.request.api_get_things_by_platform_id'
        rabbitmq_connection.ensure_connection()
        with Producer(rabbitmq_connection) as producer:
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

        with Consumer(rabbitmq_connection, queues=queue_response, callbacks=[on_response], no_ack=True):
            while message_response is None:
                rabbitmq_connection.drain_events()

        return message_response
    else:
        return None


def get_things_state_by_list_thing(list_things_info):

    list_item_global_id = []
    for thing_info in list_things_info:
        for item_info in thing_info['items']:
            list_item_global_id.append(item_info['item_global_id'])

    list_item_state = get_items_state(list_item_global_id)['items']
    print(list_item_state)
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



if __name__ == '__main__':
    app.run(debug=True)