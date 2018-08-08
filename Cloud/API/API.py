from kombu import Connection, Producer, Consumer, Queue, uuid, Exchange
import json
import sys
import socket
import datetime
import os
MODE_CODE = "Develop"
# MODE_CODE = "Deploy"

if MODE_CODE == "Deploy":
    BROKER_CLOUD = sys.argv[1]
else:
    BROKER_CLOUD = "0.0.0.0"


rabbitmq_connection = Connection(BROKER_CLOUD)
exchange = Exchange("IoT", type="direct")

my_path = os.path.dirname(__file__)
filename = os.path.join(my_path, '../../Semantic_Analysis/metric_domain.json')

with open(filename) as json_file:
    metric_domain_file = json.load(json_file)

#=====================================================
from flask import Flask, jsonify, request

app = Flask(__name__)


@app.route('/api/platforms', defaults={'platform_status': 'active'}, methods=['GET'])
@app.route('/api/platforms/<platform_status>', methods=['GET'])
def api_get_platforms(platform_status):
    return jsonify(get_list_platforms(platform_status))


@app.route('/api/resources', defaults={'resource_status': 'active', 'metric_status': 'active'}, methods=['GET'])
@app.route('/api/resources/<resource_status>/<metric_status>', methods=['GET'])
def api_get_resources(resource_status, metric_status):
    return jsonify(get_resources(resource_status=resource_status, metric_status=metric_status))


@app.route('/api/resource/<resource_id>', methods=['GET'])
def api_get_resources_by_resource_id(resource_id):
    return jsonify(get_resources(resource_id=resource_id, resource_status="all", metric_status="all"))


@app.route('/api/resources/platform_id/<platform_id>', defaults={'resource_status': 'active', 'metric_status': 'active'}, methods=['GET'])
@app.route('/api/resources/platform_id/<platform_id>/<resource_status>/<metric_status>', methods=['GET'])
def api_get_resources_platform_id(platform_id, resource_status, metric_status):
    return jsonify(get_resources(platform_id=platform_id, resource_status=resource_status, metric_status=metric_status))


@app.route('/api/history/resources/<start_time>/<end_time>', defaults={'resource_status': 'active', 'metric_status': 'active', 'scale': '0s'}, methods=['GET'])
@app.route('/api/history/resources/<start_time>/<end_time>/<scale>', defaults={'resource_status': 'active', 'metric_status': 'active'}, methods=['GET'])
@app.route('/api/history/resources/<resource_status>/<metric_status>/<start_time>/<end_time>/<scale>', methods=['GET'])
def api_get_resources_history(resource_status, metric_status, start_time, end_time, scale):
    try:
        datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
        return jsonify(get_resources_history(start_time=start_time, end_time=end_time, scale=scale, resource_status=resource_status, metric_status=metric_status))
    except ValueError:
        return jsonify({'error': 'Incorrect data format, should be %Y-%m-%d %H:%M:%S'})


@app.route('/api/history/metric/<metric_id>/<start_time>/<end_time>', defaults={'scale': '0s'}, methods=['GET'])
@app.route('/api/history/metric/<metric_id>/<start_time>/<end_time>/<scale>', methods=['GET'])
def api_get_metric_history_by_id(metric_id, start_time, end_time, scale):
    try:
        datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
        list_metric_id = [metric_id]
        return jsonify(get_metric_state_history(list_metric_id, start_time, end_time, scale))
    except ValueError:
        return jsonify({'error': 'Incorrect data format, should be %Y-%m-%d %H:%M:%S',
                        'start_time': start_time,
                        'end_time': end_time,
                        'metric_id': metric_id})


@app.route('/api/history/resource/<resource_id>/<start_time>/<end_time>', defaults={'scale': '0s'}, methods=['GET'])
@app.route('/api/history/resource/<resource_id>/<start_time>/<end_time>/<scale>', methods=['GET'])
def api_get_resources_history_by_resource_id(resource_id, start_time, end_time, scale):
    try:
        datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
        return jsonify(get_resources_history(start_time=start_time, end_time=end_time, scale=scale, resource_id=resource_id, resource_status="all", metric_status="all"))
    except ValueError:
        return jsonify({'error': 'Incorrect data format, should be %Y-%m-%d %H:%M:%S'})


@app.route('/api/history/resources/platform_id/<platform_id>/<start_time>/<end_time>', defaults={'resource_status': 'active', 'metric_status': 'active', 'scale': '0s'}, methods=['GET'])
@app.route('/api/history/resources/platform_id/<platform_id>/<start_time>/<end_time>/<scale>', defaults={'resource_status': 'active', 'metric_status': 'active'}, methods=['GET'])
@app.route('/api/history/resources/platform_id/<platform_id>/<resource_status>/<metric_status>/<start_time>/<end_time>/<scale>', methods=['GET'])
def api_get_resources_history_by_platform_id(platform_id, resource_status, metric_status, start_time, end_time, scale):
    try:
        datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
        return jsonify(get_resources_history(platform_id=platform_id, resource_status=resource_status, metric_status=metric_status, start_time=start_time, end_time=end_time, scale=scale))
    except ValueError:
        return jsonify({'error': 'Incorrect data format, should be %Y-%m-%d %H:%M:%S'})


@app.route('/api/metric', methods=['POST'])
def api_set_state():
    request_message = request.json
    resource_id = request_message['ResourceId']
    metric_id = request_message['MetricId']
    new_state = request_message['new_value']
    return jsonify(set_state(resource_id, metric_id, new_state))


# prevent cached responses
@app.after_request
def add_header(response):

    response.headers["Cache-Control"] = "no-cache, no-store, max-age=0"
    response.headers["Pragma"] = "no-cache"
    return response


def get_resources_history(start_time, end_time, scale, resource_status=None, metric_status=None, platform_id=None, resource_id=None):
    print("API get things state history")
    try:
        list_resources_info = get_resources_info(resource_status=resource_status, metric_status=metric_status, platform_id=platform_id, resource_id=resource_id)
        list_resources_state = get_resources_state_history_by_list_thing(list_resources_info, start_time, end_time, scale)
        return list_resources_state
    except (KeyError, IndexError):
        error = {
            'error': "Can not connect to service"
        }

        return error


def get_resources_state_history_by_list_thing(list_resources_info, start_time, end_time, scale):
    print("get_things_state_history_by_list_thing")
    list_metric_id = []
    for resource_info in list_resources_info:
        for metric_info in resource_info['metrics']:
            list_metric_id.append(metric_info['MetricId'])

    list_metric_state = get_metric_state_history(list_metric_id, start_time, end_time, scale)
    # print(list_item_state[0])
    for metric_state in list_metric_state:
        for resource_info in list_resources_info:
            for metric_info in resource_info['metrics']:
                if metric_state['MetricId'] == metric_info['MetricId']:
                    metric_info['history'] = metric_state['history']
                    if 'max_global' in metric_state:
                        metric_info['max_global'] = metric_state['max_global']
                    if 'min_global' in metric_state:
                        metric_info['min_global'] = metric_state['min_global']
                    if 'average_global' in metric_state:
                        metric_info['average_global'] = metric_state['average_global']
                    break

    for resource_info in list_resources_info:
        for metric_info in resource_info['metrics']:
            if 'history' in metric_info:
                continue
            else:
                metric_info['history'] = []
    list_things_state = list_resources_info
    return list_things_state


def get_metric_state_history(list_metric_id, start_time, end_time, scale):
    message_request = {
        'header': {},
        'body':{
            'list_metric_id': list_metric_id,
            'start_time': start_time,
            'end_time': end_time,
            'scale': scale
        }

    }

    # request to api_get_things of Registry
    request_routing_key = 'dbreader.request.api_get_metric_history'
    message_response = request_service(rabbitmq_connection, message_request, exchange, request_routing_key)

    # message_response = {"items": [{'item_global_id': "", 'item_state': "", 'last_changed': ""}]}
    return message_response['body']['metrics']


def get_list_platforms(platform_status):
    print("API list platforms from Registry")

    if platform_status in ['active', "inactive", "all"]:
        message_request = {
            'header':{
                'PlatformStatus': platform_status
            }
        }

        # request to api_get_list_platform of Registry
        request_routing_key = 'registry.request.api_get_list_platforms'
        message_response = request_service(rabbitmq_connection, message_request, exchange, request_routing_key)
        print(message_response)
        if 'list_platforms' in message_response['body']:
            return message_response['body']['list_platforms']
        else:
            # have error
            return message_response
    else:
        return None


def get_resources_info(resource_status=None, metric_status=None, platform_id=None, resource_id=None):
    print("API get resource info")
    if ((resource_status in ["active", "inactive", "all"]) and (metric_status in ["active", "inactive", "all"])) or platform_id is not None or resource_id is not None:
        message_request = {
            'header': {},
            'body': {
                "PlatformId": None,
                "ResourceStatus": None,
                "MetricStatus": None,
                "ResourceId": None,
            }
        }

        if (resource_status in ["active", "inactive", "all"]) and (metric_status in ["active", "inactive", "all"]):
            message_request['body']['ResourceStatus'] = resource_status
            message_request['body']['MetricStatus'] = metric_status

        if platform_id is not None:
            message_request['body']['PlatformId'] = platform_id

        if resource_id is not None:
            message_request['body']['ResourceId'] = resource_id
        print(message_request)
        # request to api_get_things of Registry
        request_routing_key = 'registry.request.api_get_resources'
        message_response = request_service(rabbitmq_connection, message_request, exchange, request_routing_key)
        print(message_response)
        return message_response['body']['resources']
    else:
        return None


def get_resources(resource_status=None, metric_status=None, platform_id=None, resource_id=None):
    try:
        list_resources_info = get_resources_info(resource_status=resource_status, metric_status=metric_status, platform_id=platform_id, resource_id=resource_id)
        list_resources_state = get_resources_state_by_list_resource(list_resources_info)
        return list_resources_state

    except (KeyError, IndexError):
        error = {
            'error': "Can not connect to service"
        }
        return error


def get_metric_state(list_metric_id):
    message_request = {
        'header': {},
        'body': {
            'list_metric_id': list_metric_id
        }
    }
    # request to api_get_things of Registry
    request_routing_key = 'dbreader.request.api_get_metric'
    message_response = request_service(rabbitmq_connection, message_request, exchange, request_routing_key)
    # message_response = {"items": [{'item_global_id': "", 'item_state': "", 'last_changed': ""}]}
    return message_response['body']["metrics"]


def get_resources_state_by_list_resource(list_resources_info):
    list_metric_id = []
    for resource_info in list_resources_info:
        for metric_info in resource_info['metrics']:
            list_metric_id.append(metric_info['MetricId'])

    list_metric_state = get_metric_state(list_metric_id)
    # print(list_item_state[0])
    for metric_state in list_metric_state:
        for resource_info in list_resources_info:
            for metric in resource_info['metrics']:
                if metric['MetricId'] == metric_state['MetricId']:
                    metric['DataPoint'] = metric_state["DataPoint"]
                    break

    list_things_state = list_resources_info
    return list_things_state


def set_state(resource_id, metric_id, new_value):
    try:
        resource = get_resources_info(resource_id=resource_id)[0]
    except:
        print("Wrong resource_id")
        return
    for metric in resource['metrics']:
        if metric['MetricId'] == metric_id:
            domain_name = metric["MetricDomain"]
            if metric_domain_file[domain_name]['can_set_value'] is True:
                value_domain = metric_domain_file[domain_name]["value"]
                if isinstance(value_domain, list):
                    if new_value in value_domain:
                        value_mapped = new_value
                    elif 'mapping' in metric_domain_file[domain_name]:
                        if new_value in metric_domain_file[domain_name]['mapping']:
                            value_mapped = metric_domain_file[domain_name]['mapping'][new_value]
                        else:
                            return "Don't have mapping value with {}".format(new_value)
                    else:
                        return "ERROR typedata"

                elif value_domain == "number":
                    if isinstance(new_value, int) or isinstance(new_value, float):
                        value_mapped = new_value
                    else:
                        return "ERROR typedata"

                message_request ={
                    'header': {
                        "PlatformId": resource['information']['PlatformId']
                    },
                    'body': {
                        'information': resource['information'],
                        'metric': metric,
                        'new_value': value_mapped
                    }
                }
                print("SET STATE: {}".format(message_request))
                request_routing_key = 'driver.request.api_set_state'
                # message_response = request_service(rabbitmq_connection, message_request, exchange, request_routing_key)
                # # message_response = {"items": [{'item_global_id': "", 'item_state': "", 'last_changed': ""}]}
                # return message_response
                rabbitmq_connection.ensure_connection()
                with Producer(rabbitmq_connection) as producer:
                    producer.publish(
                        json.dumps(message_request),
                        exchange=exchange.name,
                        routing_key=request_routing_key,
                        retry=True
                    )
                return "Public set state"

            else:
                return {
                    'error': 'MetricDomain can not set state'
                }


def request_service(conn, message_request, exchange_request, request_routing_key):
    id_response = uuid()
    queue_response = Queue(name=id_response, exchange=exchange_request, routing_key=id_response, exclusive=True, auto_delete=True)
    message_request['header']['reply_to'] = id_response
    conn.ensure_connection()
    with Producer(conn) as producer:
        producer.publish(
            json.dumps(message_request),
            exchange=exchange_request.name,
            routing_key=request_routing_key,
            declare=[queue_response],
            retry=True
        )

    message_response = None

    def on_response(body, message):
        nonlocal message_response
        message_response = json.loads(body)
    try:

        with Consumer(conn, queues=queue_response, callbacks=[on_response], no_ack=True):
            try:
                while message_response is None:
                    conn.drain_events(timeout=10)
            except socket.timeout:
                return {
                    'error': 'Can not connect to service'
                }
    except Exception:
        print("cannot create Consumer: " + request_routing_key)
        return {
            'error': 'Cannot create Consumer'
        }

    return message_response


if __name__ == '__main__':

    app.run(host='0.0.0.0', port=5000, threaded=True)
