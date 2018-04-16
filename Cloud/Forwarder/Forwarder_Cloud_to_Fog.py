
import paho.mqtt.client as mqtt
import json
from kombu import Connection, Queue, Exchange, Consumer, exceptions
from kombu.utils.compat import nested

BROKER_CLOUD = "localhost"  #rabbitmq
BROKER_FOG = "localhost"    #mosquitto
MODE_COLLECT = "PULL"       #or PUSH

#create Client Mosquitto
client_fog = mqtt.Client()
client_fog.connect(BROKER_FOG)
client_fog.loop_start()

#Creat connection to rabbitmq cloud
rabbitmq_connection = Connection(BROKER_CLOUD)
exchange = Exchange('IoT', type='direct')


def on_message_check_config(body, message):
    print("Forward from Registry to Driver: check configuration")
    body = json.loads(body)
    platform_id = body['platform_id']
    broker_fog_topic = "{}/request/api_check_configuration_changes".format(platform_id)
    try:
        client_fog.publish(broker_fog_topic, json.dumps(body))
    except:
        print("Error publish message in on_message_check_config")
    message.ack()


#On message for sub on /driver/response/filter/..

def on_message_collect(body, message):
    print('Forward from Collector to Driver: collect states')
    body = json.loads(body)
    platform_id = body['platform_id']
    broker_fog_topic = "{}/request/api_get_states".format(platform_id)
    try:
        client_fog.publish(broker_fog_topic, json.dumps(body))
    except:
        print("Error publish message in on_message_collect function")
    message.ack()


# On message for registry/request/api-add-platform
def on_message_add_platform(body, message):
    print("Forward from Registry to Driver: add platform")
    body = json.loads(body)
    broker_fog_topic = "registry/response/{}/{}".format(body['host'], body['port'])
    try:
        client_fog.publish(broker_fog_topic, json.dumps(body))
    except:
        print("Error publish message in on_message_add_platform function")
    message.ack()


def on_disconnect(client, userdata, rc):
    if rc != 0:
        print("disconnect to Mosquitto.")
        client_fog.publish("hihi", 'lala')


def on_connect(client, userdata, flags, rc):
    print("connect to Mosquitto")

client_fog.on_disconnect = on_disconnect
client_fog.on_connect = on_connect

# declare rabbitmq resources
queue_add_platform = Queue(name='registry.response.driver.api_add_platform', exchange=exchange, routing_key='registry.response.driver.api_add_platform')
queue_check_config = Queue(name='driver.request.api_check_configuration_changes', exchange=exchange, routing_key='driver.request.api_check_configuration_changes')
queue_collect = Queue(name='driver.request.api_get_states', exchange=exchange, routing_key='driver.request.api_get_states')


while 1:
    try:
        rabbitmq_connection.ensure_connection(max_retries=3)
        with nested(Consumer(rabbitmq_connection, queues=queue_add_platform, callbacks=[on_message_add_platform]),
                    Consumer(rabbitmq_connection, queues=queue_check_config, callbacks=[on_message_check_config]),
                    Consumer(rabbitmq_connection, queues=queue_collect, callbacks=[on_message_collect])):
            while True:
                rabbitmq_connection.drain_events()
    except (ConnectionRefusedError, exceptions.OperationalError) as hihi:
        print('Connection lost')
    except rabbitmq_connection.connection_errors:
        print('Connection error')
