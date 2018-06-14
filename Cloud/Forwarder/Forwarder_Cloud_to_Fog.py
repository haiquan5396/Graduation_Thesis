import paho.mqtt.client as mqtt
import json
from kombu import Connection, Queue, Exchange, Consumer, exceptions
from kombu.utils.compat import nested
import sys


class ForwarderCloudToFog():
    def __init__(self, broker_cloud, broker_fog):
        # create Client Mosquitto
        self.client_fog = mqtt.Client()
        self.client_fog.connect(BROKER_FOG)
        self.client_fog.loop_start()

        # Creat connection to rabbitmq cloud
        self.rabbitmq_connection = Connection(BROKER_CLOUD)
        self.exchange = Exchange('IoT', type='direct')

    # Registry request to collect configuration
    def on_message_check_config(self, body, message):
        print("Forward from Registry to Driver: check configuration")
        body = json.loads(body)
        platform_id = body['platform_id']
        broker_fog_topic = "{}/request/api_check_configuration_changes".format(platform_id)
        try:
            self.client_fog.publish(broker_fog_topic, json.dumps(body))
        except:
            print("Error publish message in on_message_check_config")
        message.ack()

    # API request set_state to Driver
    def on_message_set_state(self, body, message):
        print("Forward Set State")
        body = json.loads(body)
        platform_id = body['platform_id']
        broker_fog_topic = "{}/request/api_set_state".format(platform_id)
        try:
            self.client_fog.publish(broker_fog_topic, json.dumps(body))
        except:
            print("Error publish message in on_message_set_state function")
        message.ack()

    # Registry response add_platform to driver
    def on_message_add_platform(self, body, message):
        print("Forward from Registry to Driver: add platform")
        body = json.loads(body)
        broker_fog_topic = "registry/response/{}/{}".format(body['host'], body['port'])
        try:
            self.client_fog.publish(broker_fog_topic, json.dumps(body))
        except:
            print("Error publish message in on_message_add_platform function")
        message.ack()

    # Collector request to collect data
    def on_message_collect(self, body, message):
        print('Forward from Collector to Driver: collect states')
        body = json.loads(body)
        platform_id = body['platform_id']
        broker_fog_topic = "{}/request/api_get_states".format(platform_id)
        try:
            self.client_fog.publish(broker_fog_topic, json.dumps(body))
        except:
            print("Error publish message in on_message_collect function")
        message.ack()

    def on_disconnect(self, client, userdata, rc):
        if rc != 0:
            print("disconnect to Mosquitto.")

    def on_connect(self, client, userdata, flags, rc):
        print("connect to Mosquitto")

    def run(self):
        self.client_fog.on_disconnect = self.on_disconnect
        self.client_fog.on_connect = self.on_connect

        # declare rabbitmq resources
        queue_add_platform = Queue(name='registry.response.driver.api_add_platform', exchange=self.exchange,
                                   routing_key='registry.response.driver.api_add_platform')
        queue_check_config = Queue(name='driver.request.api_check_configuration_changes', exchange=self.exchange,
                                   routing_key='driver.request.api_check_configuration_changes')
        queue_collect = Queue(name='driver.request.api_get_states', exchange=self.exchange,
                              routing_key='driver.request.api_get_states')
        queue_set_state = Queue(name='driver.request.api_set_state', exchange=self.exchange,
                                routing_key='driver.request.api_set_state')

        while 1:
            try:
                self.rabbitmq_connection.ensure_connection(max_retries=3)
                with nested(
                        Consumer(self.rabbitmq_connection, queues=queue_add_platform, callbacks=[self.on_message_add_platform]),
                        Consumer(self.rabbitmq_connection, queues=queue_check_config, callbacks=[self.on_message_check_config]),
                        Consumer(self.rabbitmq_connection, queues=queue_collect, callbacks=[self.on_message_collect]),
                        Consumer(self.rabbitmq_connection, queues=queue_set_state, callbacks=[self.on_message_set_state])):
                    while True:
                        self.rabbitmq_connection.drain_events()
            except (ConnectionRefusedError, exceptions.OperationalError):
                print('Connection lost')
            except self.rabbitmq_connection.connection_errors:
                print('Connection error')

if __name__ == '__main__':
    MODE_CODE = 'Develop'
    # MODE_CODE = 'Deploy'

    if MODE_CODE == 'Develop':
        BROKER_CLOUD = 'localhost'  # rabbitmq
        BROKER_FOG = 'localhost'  # mosquitto

    else:
        BROKER_CLOUD = sys.argv[1]  #rabbitmq
        BROKER_FOG = sys.argv[2]    #mosquitto

    forwarder = ForwarderCloudToFog(BROKER_CLOUD, BROKER_FOG)
    forwarder.run()