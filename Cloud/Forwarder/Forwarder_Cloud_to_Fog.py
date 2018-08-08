import paho.mqtt.client as mqtt
import json
from kombu import Connection, Queue, Exchange, Consumer, exceptions
from kombu.utils.compat import nested
import sys
#from Performance_Monitoring.message_monitor import MessageMonitor


class ForwarderCloudToFog:
    def __init__(self, broker_cloud, broker_fog):
        # create Client Mosquitto
        self.client_fog = mqtt.Client()
        self.client_fog.connect(BROKER_FOG, keepalive=20)
        self.client_fog.loop_start()

        # Creat connection to rabbitmq cloud
        self.rabbitmq_connection = Connection(BROKER_CLOUD)
        self.exchange = Exchange('IoT', type='direct')
        #self.message_monitor = MessageMonitor('0.0.0.0', 8086)


    # Registry request to collect configuration
    def on_message_check_config(self, body, message):
        print("Forward from Registry to Driver: check configuration")
        body = json.loads(body)
        platform_id = body['header']['PlatformId']
        broker_fog_topic = "{}/request/api_check_configuration_changes".format(platform_id)
        #body['message_monitor'] = self.message_monitor.monitor(body, 'cloud_to_fog', 'on_message_check_config')

        try:
            self.client_fog.publish(broker_fog_topic, json.dumps(body))
        except:
            print("Error publish message in on_message_check_config")
        message.ack()

    # API request set_state to Driver
    def on_message_set_state(self, body, message):
        print("Forward Set State")
        body = json.loads(body)
        platform_id = body['header']['PlatformId']
        broker_fog_topic = "{}/request/api_set_state".format(platform_id)
        #body['message_monitor'] = self.message_monitor.monitor(body, 'cloud_to_fog', 'on_message_set_state')

        try:
            self.client_fog.publish(broker_fog_topic, json.dumps(body), qos=2)
        except:
            print("Error publish message in on_message_set_state function")
        message.ack()

    # Registry response add_platform to driver
    def on_message_add_platform(self, body, message):
        print("Forward from Registry to Driver: add platform")
        message_content = json.loads(body)
        broker_fog_topic = "registry/response/{}/{}".format(message_content['header']['PlatformHost'], message_content['header']['PlatformPort'])
        #body['message_monitor'] = self.message_monitor.monitor(body, 'cloud_to_fog', 'on_message_add_platform')

        try:
            self.client_fog.publish(broker_fog_topic, json.dumps(message_content))
        except:
            print("Error publish message in on_message_add_platform function")
        message.ack()

    # Collector request to collect data
    def on_message_collect(self, body, message):
        print('Forward from Collector to Driver: collect states')
        message_content = json.loads(body)
        platform_id = message_content['header']['PlatformId']
        broker_fog_topic = "{}/request/api_get_states".format(platform_id)
        #body['message_monitor'] = self.message_monitor.monitor(body, 'cloud_to_fog', 'on_message_collect')

        try:
            self.client_fog.publish(broker_fog_topic, json.dumps(message_content))
        except:
            print("Error publish message in on_message_collect function")
        message.ack()

    def on_message_update_now_configuration(self, body, message):
        print('Forward from Registry to Driver: update now configuration')
        body = json.loads(body)
        platform_id = body['header']['PlatformId']
        broker_fog_topic = "{}/request/api_update_now_configuration".format(platform_id)
        #body['message_monitor'] = self.message_monitor.monitor(body, 'cloud_to_fog', 'on_message_collect')

        try:
            print(body)
            self.client_fog.publish(broker_fog_topic, json.dumps(body))
        except:
            print("Error publish message in on_message_collect function")
        message.ack()

    def on_message_check_platform_active(self, body, message):
        print('Forward from Registry to Driver: check_platform_active')
        body = json.loads(body)
        platform_id = body['header']['PlatformId']
        broker_fog_topic = "{}/request/api_check_platform_active".format(platform_id)
        #body['message_monitor'] = self.message_monitor.monitor(body, 'cloud_to_fog', 'on_message_collect')

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
                                   routing_key='registry.response.driver.api_add_platform', message_ttl=20)
        queue_check_config = Queue(name='driver.request.api_check_configuration_changes', exchange=self.exchange,
                                   routing_key='driver.request.api_check_configuration_changes', message_ttl=20)
        queue_collect = Queue(name='driver.request.api_get_states', exchange=self.exchange,
                              routing_key='driver.request.api_get_states', message_ttl=20)
        queue_set_state = Queue(name='driver.request.api_set_state', exchange=self.exchange,
                                routing_key='driver.request.api_set_state', message_ttl=20)
        queue_update_config = Queue(name='driver.request.api_update_now_configuration', exchange=self.exchange,
                                routing_key='driver.request.api_update_now_configuration', message_ttl=20)
        queue_check_active = Queue(name='driver.request.api_check_platform_active', exchange=self.exchange,
                                routing_key='driver.request.api_check_platform_active', message_ttl=20)

        while 1:
            try:
                self.rabbitmq_connection.ensure_connection(max_retries=3)
                with nested(
                        Consumer(self.rabbitmq_connection, queues=queue_add_platform, callbacks=[self.on_message_add_platform]),
                        Consumer(self.rabbitmq_connection, queues=queue_check_config, callbacks=[self.on_message_check_config]),
                        Consumer(self.rabbitmq_connection, queues=queue_collect, callbacks=[self.on_message_collect]),
                        Consumer(self.rabbitmq_connection, queues=queue_set_state, callbacks=[self.on_message_set_state]),
                        Consumer(self.rabbitmq_connection, queues=queue_update_config,callbacks=[self.on_message_update_now_configuration]),
                        Consumer(self.rabbitmq_connection, queues=queue_check_active,callbacks=[self.on_message_check_platform_active])
                ):
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