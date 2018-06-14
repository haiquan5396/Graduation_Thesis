import paho.mqtt.client as mqtt
import json
from kombu import Connection, Queue, Exchange, Producer
import sys

# BROKER_CLOUD = sys.argv[1]  #rabbitmq
# BROKER_FOG = sys.argv[2]    #mosquitto

BROKER_CLOUD = 'localhost'  #rabbitmq
BROKER_FOG = 'localhost'    #mosquitto

class ForwarderFogToCloud():
    def __init__(self, broker_cloud, broker_fog):
        self.client_fog = mqtt.Client()
        self.client_fog.connect(broker_fog)

        # Creat connection to rabbitmq cloud
        self.rabbitmq_connection = Connection(broker_cloud)
        self.exchange = Exchange('IoT', type='direct')

    def on_message_registry(self, client, userdata, msg):
        print("Forward to Registry api_check_configuration_changes")
        data = json.loads(msg.payload.decode(
            "utf-8"))  # vd: data = {"have_change": False, "now_info": [{}], "platform_id": "", "reply_to": ""}
        reply_to = data['reply_to']
        routing_key = reply_to
        queue_name = reply_to
        queue = Queue(name=queue_name, exchange=self.exchange, routing_key=routing_key)

        self.rabbitmq_connection.ensure_connection()
        with Producer(self.rabbitmq_connection) as producer:
            producer.publish(
                json.dumps(data),
                exchange=self.exchange.name,
                routing_key=routing_key,
                declare=[queue],
                retry=True
            )

    # On message for sub on /driver/response/filter/..
    def on_message_filter(self, client, userdata, msg):
        print('Forward to Collector api_get_states')
        data = json.loads(msg.payload.decode("utf-8"))

        reply_to = data['reply_to']
        routing_key = reply_to
        queue_name = reply_to
        queue = Queue(name=queue_name, exchange=self.exchange, routing_key=routing_key)

        self.rabbitmq_connection.ensure_connection()
        with Producer(self.rabbitmq_connection) as producer:
            producer.publish(
                json.dumps(data),
                exchange=self.exchange.name,
                routing_key=routing_key,
                declare=[queue],
                retry=True
            )

    # On message for registry/request/api-add-platform
    def on_message_add_platform(self, client, userdata, msg):
        print('Forward to Registry api_add_platform')
        data = json.loads(msg.payload.decode('utf-8'))
        routing_key = "registry.request.api_add_platform"
        queue_name = "registry.request.api_add_platform"
        queue = Queue(name=queue_name, exchange=self.exchange, routing_key=routing_key)
        self.rabbitmq_connection.ensure_connection()
        with Producer(self.rabbitmq_connection) as producer:
            producer.publish(
                json.dumps(data),
                exchange=self.exchange.name,
                routing_key=routing_key,
                declare=[queue],
                retry=True
            )

    def on_connect(self, client, userdata, flags, rc):
        print("connect to Mosquitto")
        self.client_fog.message_callback_add("driver/response/forwarder/api_check_configuration_changes",
                                        self.on_message_registry)
        self.client_fog.message_callback_add("filter/response/forwarder/api_get_states", self.on_message_filter)
        self.client_fog.message_callback_add("registry/request/api_add_platform", self.on_message_add_platform)

        self.client_fog.subscribe("driver/response/forwarder/api_check_configuration_changes")
        self.client_fog.subscribe("filter/response/forwarder/api_get_states")
        self.client_fog.subscribe("registry/request/api_add_platform")

    def on_disconnect(self, client, userdata, rc):
        if rc != 0:
            print("disconnect to Mosquitto.")

    def run(self):
        self.client_fog.on_disconnect = self.on_disconnect
        self.client_fog.on_connect = self.on_connect
        self.client_fog.loop_forever()

if __name__ == '__main__':
    MODE_CODE = 'Develop'
    # MODE_CODE = 'Deploy'

    if MODE_CODE == 'Develop':
        BROKER_CLOUD = 'localhost'  # rabbitmq
        BROKER_FOG = 'localhost'  # mosquitto

    else:
        BROKER_CLOUD = sys.argv[1]  #rabbitmq
        BROKER_FOG = sys.argv[2]    #mosquitto

    forwarder = ForwarderFogToCloud(BROKER_CLOUD, BROKER_FOG)
    forwarder.run()
