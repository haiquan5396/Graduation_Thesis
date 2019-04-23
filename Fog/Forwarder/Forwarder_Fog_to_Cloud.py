import json
import sys
from Communicator.broker_cloud import BrokerCloudClient
from Communicator.broker_fog import BrokerFogClient
import Logging.config_logging as logging

_LOGGER = logging.get_logger(__name__)


class ForwarderFogToCloud:
    def __init__(self, broker_cloud, broker_fog):

        # create connection to Broker_Fog
        self.client_fog = BrokerFogClient(broker_fog)

        # Create connection to Broker_Cloud
        self.client_cloud = BrokerCloudClient(broker_cloud, 'IoT', 'direct')

    def on_message_registry(self, client, userdata, msg):
        _LOGGER.info("Forward from Driver to Registry: api_check_configuration_changes")
        message = json.loads(msg.payload.decode("utf-8"))  # vd: data = {"have_change": False, "now_info": [{}], "platform_id": "", "reply_to": ""}
        queue_name = message['header']['reply_to']
        self.client_cloud.publish_messages(message, queue_name)

    def on_message_filter(self, client, userdata, msg):
        # start = time.time()
        _LOGGER.info('Forward from Filter to Collector vs Rule Engine: api_get_states')
        message = json.loads(msg.payload.decode("utf-8"))

        # publish to collector
        queue_name = message['header']['reply_to']
        self.client_cloud.publish_messages(message, queue_name)

        # publish to rule_engine
        # queue_rule = 'rule.request.states'
        # self.client_cloud.publish_messages(message, queue_rule)
        # self.logger.warning("TIME: {}".format(time.time() - start))

    def on_message_add_platform(self, client, userdata, msg):
        _LOGGER.info('Forward from Driver to Registry: api_add_platform')
        message = json.loads(msg.payload.decode('utf-8'))
        queue_name = "registry.request.api_add_platform"
        self.client_cloud.publish_messages(message, queue_name)

    def on_message_check_platform_active(self, client, userdata, msg):
        _LOGGER.info('Forward from Driver to Registry: check_platform_active')
        message = json.loads(msg.payload.decode('utf-8'))
        queue_name = message['header']['reply_to']
        self.client_cloud.publish_messages(message, queue_name)

    def run(self):
        info_subscribers = [
            {
                'topic': "driver/response/forwarder/api_check_configuration_changes",
                'callback': self.on_message_registry
            },
            {
                'topic': "filter/response/forwarder/api_get_states",
                'callback': self.on_message_filter
            },
            {
                'topic': "registry/request/api_add_platform",
                'callback': self.on_message_add_platform
            },
            {
                'topic': "driver/response/forwarder/api_check_platform_active",
                'callback': self.on_message_check_platform_active
            }
        ]
        self.client_fog.subscribe_message(info_subscribers)
        self.client_fog.loop(loop_forever=True)


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
