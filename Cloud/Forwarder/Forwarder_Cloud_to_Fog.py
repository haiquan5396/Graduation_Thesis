import json
import sys
import os
from Communicator.broker_cloud import BrokerCloudClient
from Communicator.broker_fog import BrokerFogClient
import Logging.config_logging as logging

_LOGGER = logging.get_logger(__name__)


class ForwarderCloudToFog:
    def __init__(self, broker_cloud, broker_fog):
        # create connection to Broker_Fog
        self.client_fog = BrokerFogClient(broker_fog)

        # Create connection to Broker_Cloud
        self.client_cloud = BrokerCloudClient(broker_cloud, 'IoT', 'direct')

    # Registry request to collect configuration
    def on_message_check_config(self, body, message):
        _LOGGER.info("Forward from Registry to Driver: check configuration")
        body = json.loads(body)
        platform_id = body['header']['PlatformId']
        broker_fog_topic = "{}/request/api_check_configuration_changes".format(platform_id)
        self.client_fog.publish_message(broker_fog_topic, json.dumps(body))

    # API request set_state to Driver
    def on_message_set_state(self, body, message):
        _LOGGER.info("Forward Set State")
        body = json.loads(body)
        platform_id = body['header']['PlatformId']
        broker_fog_topic = "{}/request/api_set_state".format(platform_id)
        self.client_fog.publish_message(broker_fog_topic, json.dumps(body), 2)

    # Registry response add_platform to driver
    def on_message_add_platform(self, body, message):
        _LOGGER.info("Forward from Registry to Driver: add platform")
        message_content = json.loads(body)
        broker_fog_topic = "registry/response/{}/{}".format(message_content['header']['PlatformHost'], message_content['header']['PlatformPort'])
        self.client_fog.publish_message(broker_fog_topic, json.dumps(message_content))

    # Collector request to collect data
    def on_message_collect(self, body, message):
        _LOGGER.info('Forward from Collector to Driver: collect states')
        message_content = json.loads(body)
        platform_id = message_content['header']['PlatformId']
        broker_fog_topic = "{}/request/api_get_states".format(platform_id)
        self.client_fog.publish_message(broker_fog_topic, json.dumps(message_content))

    def on_message_update_now_configuration(self, body, message):
        _LOGGER.info('Forward from Registry to Driver: update now configuration')
        body = json.loads(body)
        platform_id = body['header']['PlatformId']
        broker_fog_topic = "{}/request/api_update_now_configuration".format(platform_id)
        self.client_fog.publish_message(broker_fog_topic, json.dumps(body))

    def on_message_check_platform_active(self, body, message):
        _LOGGER.info('Forward from Registry to Driver: check_platform_active')
        body = json.loads(body)
        platform_id = body['header']['PlatformId']
        broker_fog_topic = "{}/request/api_check_platform_active".format(platform_id)
        self.client_fog.publish_message(broker_fog_topic, json.dumps(body))

    def run(self):

        info_consumers = [
            {
                'queue_name': 'registry.response.driver.api_add_platform',
                'routing_key': 'registry.response.driver.api_add_platform',
                'callback': self.on_message_add_platform
            },
            {
                'queue_name': 'driver.request.api_check_configuration_changes',
                'routing_key': 'driver.request.api_check_configuration_changes',
                'callback': self.on_message_check_config

            },
            {
                'queue_name': 'driver.request.api_get_states',
                'routing_key': 'driver.request.api_get_states',
                'callback': self.on_message_collect
            },
            {
                'queue_name': 'driver.request.api_set_state',
                'routing_key': 'driver.request.api_set_state',
                'callback': self.on_message_set_state
            },
            {
                'queue_name': 'driver.request.api_update_now_configuration',
                'routing_key': 'driver.request.api_update_now_configuration',
                'callback': self.on_message_update_now_configuration
            },

            {
                'queue_name': 'driver.request.api_check_platform_active',
                'routing_key': 'driver.request.api_check_platform_active',
                'callback': self.on_message_check_platform_active
            }
        ]
        self.client_fog.loop(loop_forever=False)
        self.client_cloud.subscribe_message(info_consumers)


if __name__ == '__main__':
    MODE_CODE = 'Develop'
    # MODE_CODE = 'Deploy'

    if MODE_CODE == 'Develop':
        # BROKER_CLOUD = os.environ['BROKER_CLOUD']  # rabbitmq
        # BROKER_FOG = os.environ['BROKER_FOG'] # mosquitto
        BROKER_CLOUD = 'localhost'  # rabbitmq
        BROKER_FOG = 'localhost' # mosquitto

    else:
        BROKER_CLOUD = sys.argv[1]  #rabbitmq
        BROKER_FOG = sys.argv[2]    #mosquitto

    forwarder = ForwarderCloudToFog(BROKER_CLOUD, BROKER_FOG)
    forwarder.run()