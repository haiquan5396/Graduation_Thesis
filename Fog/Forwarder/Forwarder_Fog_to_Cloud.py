import paho.mqtt.client as mqtt
import json
from kombu import Connection, Queue, Exchange, Producer
import sys
from Performance_Monitoring.message_monitor import MessageMonitor
# BROKER_CLOUD = sys.argv[1]  #rabbitmq
# BROKER_FOG = sys.argv[2]    #mosquitto

BROKER_CLOUD = 'localhost'  #rabbitmq
BROKER_FOG = 'localhost'    #mosquitto


class ForwarderFogToCloud:
    def __init__(self, broker_cloud, broker_fog):
        self.client_fog = mqtt.Client()
        self.client_fog.connect(broker_fog)

        # Creat connection to rabbitmq cloud
        self.rabbitmq_connection = Connection(broker_cloud)
        self.exchange = Exchange('IoT', type='direct')
        #self.message_monitor = MessageMonitor('0.0.0.0', 8086)

    def on_message_registry(self, client, userdata, msg):
        print("Forward to Registry api_check_configuration_changes")
        message = json.loads(msg.payload.decode("utf-8"))  # vd: data = {"have_change": False, "now_info": [{}], "platform_id": "", "reply_to": ""}
        queue_name = message['header']['reply_to']
        # data['message_monitor'] = self.message_monitor.monitor(data, 'fog_to_cloud', 'on_message_registry')
        self.publish_messages(message, self.rabbitmq_connection, queue_name, self.exchange)

    # On message for sub on /driver/response/filter/..
    def on_message_filter(self, client, userdata, msg):
        print('Forward to Collector api_get_states')
        message = json.loads(msg.payload.decode("utf-8"))
        queue_name = message['header']['reply_to']
        #data['message_monitor'] = self.message_monitor.monitor(data, 'fog_to_cloud', 'on_message_filter')
        self.publish_messages(message, self.rabbitmq_connection, queue_name, self.exchange)

    # On message for registry/request/api-add-platform
    def on_message_add_platform(self, client, userdata, msg):
        print('Forward to Registry api_add_platform')
        message = json.loads(msg.payload.decode('utf-8'))
        queue_name = "registry.request.api_add_platform"
        # data['message_monitor'] = self.message_monitor.monitor(data, 'fog_to_cloud', 'on_message_add_platform')
        self.publish_messages(message, self.rabbitmq_connection, queue_name, self.exchange)

    def on_message_check_platform_active(self, client, userdata, msg):
        print('Forward to Registry check_platform_active')
        message = json.loads(msg.payload.decode('utf-8'))
        queue_name = message['header']['reply_to']
        # data['message_monitor'] = self.message_monitor.monitor(data, 'fog_to_cloud', 'on_message_add_platform')
        self.publish_messages(message, self.rabbitmq_connection, queue_name, self.exchange)

    def on_connect(self, client, userdata, flags, rc):
        print("connect to Mosquitto")
        self.client_fog.message_callback_add("driver/response/forwarder/api_check_configuration_changes", self.on_message_registry)
        self.client_fog.message_callback_add("filter/response/forwarder/api_get_states", self.on_message_filter)
        self.client_fog.message_callback_add("registry/request/api_add_platform", self.on_message_add_platform)
        self.client_fog.message_callback_add("driver/response/forwarder/api_check_platform_active", self.on_message_check_platform_active)

        self.client_fog.subscribe("driver/response/forwarder/api_check_configuration_changes")
        self.client_fog.subscribe("filter/response/forwarder/api_get_states")
        self.client_fog.subscribe("registry/request/api_add_platform")
        self.client_fog.subscribe("driver/response/forwarder/api_check_platform_active")

    def on_disconnect(self, client, userdata, rc):
        if rc != 0:
            print("disconnect to Mosquitto.")

    def publish_messages(self, message, conn, queue_name, exchange, routing_key=None, queue_routing_key=None):

        if queue_routing_key is None:
            queue_routing_key = queue_name
        if routing_key is None:
            routing_key = queue_name

        queue_publish = Queue(name=queue_name, exchange=exchange, routing_key=queue_routing_key, message_ttl=20)

        conn.ensure_connection()
        with Producer(conn) as producer:
            producer.publish(
                json.dumps(message),
                exchange=exchange.name,
                routing_key=routing_key,
                declare=[queue_publish],
                retry=True
            )

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
