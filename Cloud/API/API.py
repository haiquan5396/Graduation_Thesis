from kombu import Connection, Producer, Consumer, Queue, uuid, Exchange
import json

BROKER_CLOUD = "localhost"
rabbitmq_connection = Connection(BROKER_CLOUD)
exchange = Exchange("IoT", type="direct")

#=====================================================
from flask import Flask, jsonify

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
    print("API get things state with thing_status and item_status")

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
    print(get_things_info(thing_status, item_status))
    list_things_info = get_things_info(thing_status, item_status)['things']

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

    list_thing_state = list_things_info
    return list_thing_state


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
    list_things_info = get_things_info("all", "all")['things']
    for thing in list_things_info:
        if thing['thing_global_id'] == thing_global_id:
            return thing


def get_thing_state_by_global_id(thing_global_id):
    thing_info = get_thing_info_by_global_id(thing_global_id)
    list_item_global_id = []

    for item_info in thing_info['items']:
        list_item_global_id.append(item_info['item_global_id'])

    list_item_state = get_items_state(list_item_global_id)['items']

    for item_collect in list_item_state:
        for item_info in thing_info['items']:
            if item_info['item_global_id'] == item_collect['item_global_id']:
                item_info['item_state'] = item_collect['item_state']
                item_info['last_changed'] = item_collect['last_changed']
                break
    thing_state = thing_info
    return thing_state


if __name__ == '__main__':
    app.run(debug=True)