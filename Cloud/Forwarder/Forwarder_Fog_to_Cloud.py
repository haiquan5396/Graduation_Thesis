
import paho.mqtt.client as mqtt
import json
from kombu import Connection, Queue, Exchange, Producer

BROKER_CLOUD = "localhost"  #rabbitmq
BROKER_FOG = "localhost"    #mosquitto
MODE_COLLECT = "PULL"       #or PUSH

# create Client Mosquitto
client_fog = mqtt.Client()
client_fog.connect(BROKER_FOG)

# Creat connection to rabbitmq cloud
rabbitmq_connection = Connection(BROKER_CLOUD)
exchange = Exchange('IoT', type='direct')


def on_message_registry(client, userdata, msg):
    print("Forward to Registry api_check_configuration_changes")
    data = json.loads(msg.payload.decode("utf-8"))    # vd: data = {"have_change": False, "now_info": [{}], "platform_id": "", "reply_to": ""}
    reply_to = data['reply_to']
    routing_key = reply_to
    queue_name = reply_to
    queue = Queue(name=queue_name, exchange = exchange, routing_key =routing_key)

    rabbitmq_connection.ensure_connection()
    with Producer(rabbitmq_connection) as producer:
        producer.publish(
            json.dumps(data),
            exchange=exchange.name,
            routing_key=routing_key,
            declare=[queue],
            retry=True
        )


# On message for sub on /driver/response/filter/..
def on_message_filter(client, userdata, msg):
    print('Forward to Collector api_get_states')
    data = json.loads(msg.payload.decode("utf-8"))

    reply_to = data['reply_to']
    routing_key = reply_to
    queue_name = reply_to
    queue = Queue(name=queue_name, exchange = exchange, routing_key =routing_key)

    rabbitmq_connection.ensure_connection()
    with Producer(rabbitmq_connection) as producer:
        producer.publish(
            json.dumps(data),
            exchange=exchange.name,
            routing_key=routing_key,
            declare=[queue],
            retry=True
        )



#On message for registry/request/api-add-platform
def on_message_add_platform(client, userdata, msg):
    print('Forward to Registry api_add_platform')
    data = json.loads(msg.payload.decode('utf-8'))
    routing_key = "registry.request.api_add_platform"
    queue_name = "registry.request.api_add_platform"
    queue = Queue(name=queue_name, exchange = exchange, routing_key =routing_key)
    rabbitmq_connection.ensure_connection()
    with Producer(rabbitmq_connection) as producer:
        producer.publish(
            json.dumps(data),
            exchange=exchange.name,
            routing_key=routing_key,
            declare=[queue],
            retry=True
        )


def on_disconnect(client, userdata, rc):
    if rc != 0:
        print("disconnect to Mosquitto.")


def on_connect(client, userdata, flags, rc):
    print("connect to Mosquitto")
    client_fog.message_callback_add("driver/response/forwarder/api_check_configuration_changes", on_message_registry)
    client_fog.message_callback_add("filter/response/forwarder/api_get_states", on_message_filter)
    client_fog.message_callback_add("registry/request/api_add_platform", on_message_add_platform)

    client_fog.subscribe("driver/response/forwarder/api_check_configuration_changes")
    client_fog.subscribe("filter/response/forwarder/api_get_states")
    client_fog.subscribe("registry/request/api_add_platform")


client_fog.on_disconnect = on_disconnect
client_fog.on_connect = on_connect
client_fog.loop_forever()
