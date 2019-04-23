import json
import sys
from datetime import datetime
from Communicator.broker_cloud import BrokerCloudClient
import Logging.config_logging as logging

_LOGGER = logging.get_logger(__name__)


class Collector:
    def __init__(self, broker_cloud, time_collect):
        self.time_collect = time_collect
        self.list_platform_id = []
        self.client_producer_cloud = BrokerCloudClient(broker_cloud, 'IoT', 'direct')
        self.client_consumer_cloud = BrokerCloudClient(broker_cloud, 'IoT', 'direct')

    def collect(self):
        for platform_id in self.list_platform_id:
            self.collect_by_platform_id(platform_id)

    def collect_by_platform_id(self, platform_id):
        _LOGGER.info('Collect data from platform_id: {}'.format(str(platform_id)))
        message_request = {
            'header': {
                'reply_to': 'driver.response.collector.api_get_states',
                'PlatformId': platform_id,
                'mode': 'PULL'
            }
        }

        queue_name = 'driver.request.api_get_states'
        self.client_producer_cloud.publish_messages(message_request, queue_name)

    def handle_collect_by_platform_id(self, body, message):
        _LOGGER.info('Received state from platform_id: {}'.format(json.loads(body)['header']['PlatformId']))
        states = json.loads(body)['body']['states']
        _LOGGER.debug('State : {}'.format(states))
        points = []
        for state in states:
            points.append({
                "MetricId": state['MetricId'],
                "DataType": state['DataPoint']['DataType'],
                "Value": state['DataPoint']['Value'],
                "TimeCollect": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })

        data_points = {
            'header': json.loads(body)['header'],
            'body': {
                'data_points': points
            }
        }
        queue_name = 'dbwriter.request.api_write_db'
        self.client_producer_cloud.publish_messages(data_points, queue_name)

    def get_list_platforms(self):
        _LOGGER.info("Get list platforms from Registry")
        message = {
            "header": {
                'reply_to': 'registry.response.collector.api_get_list_platforms',
                'PlatformStatus': "active"
            }
        }

        queue_name = 'registry.request.api_get_list_platforms'
        self.client_producer_cloud.publish_messages(message, queue_name)

    def handle_get_list(self, body, message):
        list_platforms = json.loads(body)['body']['list_platforms']
        temp = []
        for platform in list_platforms:
            temp.append(platform['PlatformId'])

        self.list_platform_id = temp
        self.collect()
        _LOGGER.info('Updated list of platform_id: {}'.format(self.list_platform_id))

    def handle_notification(self, body, message):
        _LOGGER.info('Have Notification')
        if json.loads(body)['notification'] == 'Have Platform_id change':
            self.get_list_platforms()

    def run(self):

        info_consumers = [
            {
                'queue_name': 'collector.request.notification',
                'routing_key': 'collector.request.notification',
                'callback': self.handle_notification
            },
            {
                'queue_name': 'registry.response.collector.api_get_list_platforms',
                'routing_key': 'registry.response.collector.api_get_list_platforms',
                'callback': self.handle_get_list

            },
            {
                'queue_name': 'driver.response.collector.api_get_states',
                'routing_key': 'driver.response.collector.api_get_states',
                'callback': self.handle_collect_by_platform_id
            }
        ]
        self.get_list_platforms()
        self.client_consumer_cloud.subscribe_message(info_consumers)


if __name__ == '__main__':
    MODE_CODE = 'Develop'
    # MODE_CODE = 'Deploy'

    if MODE_CODE == 'Develop':
        BROKER_CLOUD = "localhost"
        MODE = "PULL"
        TIME_COLLECT = 5
    else:
        BROKER_CLOUD = sys.argv[1]
        TIME_COLLECT = int(sys.argv[3])

    collector = Collector(BROKER_CLOUD, TIME_COLLECT)
    collector.run()