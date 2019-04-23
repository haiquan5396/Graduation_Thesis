import json
import uuid
import time
import threading
import copy
import sys
from Cloud.Registry.db_communicator import DbCommunicator
import Logging.config_logging as logging
from Communicator.broker_cloud import BrokerCloudClient

_LOGGER = logging.get_logger(__name__)


class Registry:
    def __init__(self, broker_cloud, mode, time_inactive_platform, time_update_conf, time_check_platform_active):
        self.time_update_conf = time_update_conf
        self.time_check_platform_active = time_check_platform_active
        self.time_inactive_platform = time_inactive_platform
        self.mode = mode
        self.dbcommunitor = DbCommunicator("Registry", "root", "root", "0.0.0.0")

        self.client_producer_cloud = BrokerCloudClient(broker_cloud, 'IoT', 'direct')
        self.client_consumer_cloud = BrokerCloudClient(broker_cloud, 'IoT', 'direct')

    def update_config_changes_by_platform_id(self, platform_id):

        message = {
            'header': {
                'reply_to': 'driver.response.registry.api_check_configuration_changes',
                'PlatformId': platform_id,
                'mode': 'PULL'
            }
        }

        queue_name = 'driver.request.api_check_configuration_changes'
        self.client_producer_cloud.publish_messages(message, queue_name)

    def check_platform_active(self):
        while 1:
            list_platforms = self.dbcommunitor.get_platforms(platform_status='all')
            for platform in list_platforms:
                if (time.time() - platform['LastResponse']) > self.time_inactive_platform and platform['PlatformStatus'] == 'active':
                    _LOGGER.info(" Change status of platform {} - {} to inactive".format(platform['PlatformName'], platform['PlatformId']))
                    platform['PlatformStatus'] = 'inactive'
                    sources = self.dbcommunitor.get_sources(platform_id=platform['PlatformId'], get_source_id_of_metric=True)
                    info_sources = []
                    info_metrics = []
                    for source in sources:
                        source['information']['SourceStatus'] = 'inactive'
                        for metric in source['metrics']:
                            metric['MetricStatus'] = 'inactive'
                            info_metrics.append(metric)
                        info_sources.append(source['information'])
                    self.dbcommunitor.update_metrics(info_metrics)
                    self.dbcommunitor.update_info_sources(info_sources)
                    self.dbcommunitor.update_platform(info_platform=platform)

                    self.send_notification_to_collector()

            time.sleep(self.time_check_platform_active)

    def update_changes_to_db(self, new_info, platform_id):
        # print("Update change of {} to database".format(platform_id))
        now_info = self.dbcommunitor.get_sources(platform_id=platform_id, source_status="all", metric_status="all")
        inactive_sources = copy.deepcopy(now_info)

        db_update_source = []
        db_update_metric = []
        db_new_source = []
        db_new_metric = []

        for new_source in new_info:
            info_new_source = copy.deepcopy(new_source['information'])
            if 'SourceId' in info_new_source:
                for now_source in now_info:
                    if now_source['information']["SourceId"] == info_new_source["SourceId"]:
                        info_new_source['SourceStatus'] = 'active'
                        db_update_source.append(info_new_source)

                        inactive_metrics = copy.deepcopy(now_source['metrics'])
                        for new_metric in new_source['metrics']:
                            if 'MetricId' in new_metric:
                                for now_metric in now_source['metrics']:
                                    if now_metric["MetricId"] == new_metric["MetricId"]:
                                        new_metric['MetricStatus'] = 'active'
                                        new_metric['SourceId'] = info_new_source['SourceId']
                                        db_update_metric.append(new_metric)
                                        inactive_metrics.remove(now_metric)
                                        break
                            else:
                                # New metric
                                new_metric['SourceId'] = info_new_source['SourceId']
                                new_metric['MetricStatus'] = 'active'
                                new_metric['MetricId'] = str(uuid.uuid4())
                                db_new_metric.append(new_metric)

                        if len(inactive_metrics) != 0:
                            # Inactive metrics
                            for metric in inactive_metrics:
                                metric['SourceId'] = info_new_source['SourceId']
                                metric['MetricStatus'] = 'inactive'
                                db_update_metric.append(metric)

                        inactive_sources.remove(now_source)
                        break
            else:
                # New Source
                new_source_id = str(uuid.uuid4())
                new_source['information']['SourceId'] = new_source_id
                new_source['information']['SourceStatus'] = 'active'
                db_new_source.append(new_source['information'])
                for metric in new_source['metrics']:
                    metric['SourceId'] = new_source_id
                    metric['MetricStatus'] = 'active'
                    metric['MetricId'] = str(uuid.uuid4())
                    db_new_metric.append(metric)

        if len(inactive_sources) != 0:
            # Inactive sources
            for source in inactive_sources:
                source['information']['SourceStatus'] = 'inactive'
                db_update_source.append(source['information'])
                for metric in source['metrics']:
                    metric['MetricStatus'] = 'inactive'
                    metric['SourceId'] = source['information']['SourceId']
                    db_update_metric.append(metric)
        # start = time.time()
        self.dbcommunitor.update_info_sources(infos=db_new_source, new_source=True)
        # print("Write DB update_info_sources_new: {}".format(time.time() - start))
        # start = time.time()
        self.dbcommunitor.update_info_sources(infos=db_update_source)
        # print("Write DB update_info_sources_active_and_inactive: {}".format(time.time() - start))
        # start = time.time()
        self.dbcommunitor.update_metrics(info_metrics=db_new_metric, new_metric=True)
        # print("Write DB update_metrics_new: {}".format(time.time() - start))
        # start = time.time()
        self.dbcommunitor.update_metrics(info_metrics=db_update_metric)
        # print("Write DB update_metrics_active_and_inactive: {}".format(time.time()-start))

    def handle_configuration_changes(self, body, message):
        header = json.loads(body)['header']
        body = json.loads(body)['body']

        platform_id = header['PlatformId']

        platform = self.dbcommunitor.get_platforms(platform_id=platform_id)[0]
        if platform['PlatformStatus'] == 'active':
            if body['is_change'] is False:
                # print('Platform have Id: {} no changes'.format(platform_id))
                if header['mode'] == "PULL":
                    new_info = body['new_info']
                    self.update_changes_to_db(new_info, platform_id)

            else:
                # start = time.time()
                _LOGGER.info('Platform have Id: {} changed sources configuration'.format(platform_id))
                _LOGGER.debug("Sources configuration received from Forwarder: {}".format(body))
                new_info = body['new_info']
                self.update_changes_to_db(new_info, platform_id)
                # end= time.time()
                # print("process time: {}".format(end - start))

            message_config = {
                'header': {
                    'PlatformId': platform_id
                },
                'body': {
                    'active_sources': self.dbcommunitor.get_sources(platform_id=platform_id, source_status='active', metric_status='active')
                }

            }
            queue_name = 'driver.request.api_update_now_configuration'
            self.client_producer_cloud.publish_messages(message_config, queue_name)

    def api_get_list_platforms(self, body, message):
        _LOGGER.info("API get list platform with platform_status")
        header = json.loads(body)['header']
        platform_status = header['PlatformStatus']
        queue_name = header['reply_to']

        message_response = {
            'header': {},
            'body': {}
        }
        message_response['body']['list_platforms'] = self.dbcommunitor.get_platforms(platform_status=platform_status)
        self.client_producer_cloud.publish_messages(message_response, queue_name)

    def api_add_platform(self, body, message):
        header = json.loads(body)['header']
        body = json.loads(body)['body']

        message_response = {
            'header': {},
            'body': {}
        }

        platform_id = ""
        if header['registered'] is True:
            platform_id = header['PlatformId']
            _LOGGER.info("Platform {} - {} come back to system".format(body['PlatformName'], platform_id))
            info_platform = {
                "PlatformId": platform_id,
                "PlatformName": body['PlatformName'],
                "PlatformType": body['PlatformType'],
                "PlatformHost": body['PlatformHost'],
                "PlatformPort": body['PlatformPort'],
                "PlatformStatus": "active",
                "LastResponse": time.time()
            }
            self.dbcommunitor.update_platform(info_platform)

        else:
            _LOGGER.info("Add new Platform to system")
            platform_id = str(uuid.uuid4())
            _LOGGER.info('Generate id for {} platform : {}'.format(body['PlatformName'], platform_id))

            info_platform = {
                "PlatformId": platform_id,
                "PlatformName": body['PlatformName'],
                "PlatformType": body['PlatformType'],
                "PlatformHost": body['PlatformHost'],
                "PlatformPort": body['PlatformPort'],
                "PlatformStatus": "active",
                "LastResponse": time.time()
            }
            self.dbcommunitor.update_platform(info_platform, new_platform=True)

        sources = self.dbcommunitor.get_sources(platform_id=platform_id)
        # print(sources)
        message_response['header']['PlatformId'] = platform_id
        message_response['header']['PlatformHost'] = body['PlatformHost']
        message_response['header']['PlatformPort'] = body['PlatformPort']
        message_response['body']['sources'] = sources

        queue_name = 'registry.response.driver.api_add_platform'
        self.client_producer_cloud.publish_messages(message_response, queue_name)
        self.send_notification_to_collector()

    def api_get_sources(self, body, message):
        _LOGGER.info('API get sources')
        message_received = json.loads(body)

        reply_to = message_received['header']['reply_to']
        platform_id = message_received['body']['PlatformId']
        source_id = message_received['body']['SourceId']
        metric_status = message_received['body']['MetricStatus']
        source_status = message_received['body']['SourceStatus']
        # print(message_received)
        message_response = {
            'body': {
                "sources": self.dbcommunitor.get_sources(platform_id=platform_id, source_id=source_id, source_status=source_status, metric_status=metric_status)
            }
        }

        self.client_producer_cloud.publish_messages(message_response, reply_to)

    def handle_check_platform_active(self, body, message):

        header = json.loads(body)['header']
        body = json.loads(body)['body']
        platform_id = header['PlatformId']
        _LOGGER.debug("Handle message when platform {} response message check active".format(platform_id))
        if body['active'] is True:
            platform = self.dbcommunitor.get_platforms(platform_id=platform_id)[0]
            if platform['PlatformStatus'] == 'inactive':
                platform['PlatformStatus'] = 'active'
                self.update_config_changes_by_platform_id(platform['PlatformId'])
            platform['LastResponse'] = time.time()
            self.dbcommunitor.update_platform(info_platform=platform)

    def send_notification_to_collector(self):
        _LOGGER.info('Send notification to Collector')
        message = {
            'notification': 'Have Platform_id change'
        }

        queue_name = 'collector.request.notification'
        self.client_producer_cloud.publish_messages(message, queue_name)

    def run(self):

        info_consumers = [
            {
                'queue_name': 'registry.request.api_get_sources',
                'routing_key': 'registry.request.api_get_sources',
                'callback': self.api_get_sources
            },
            {
                'queue_name': 'registry.request.api_get_list_platforms',
                'routing_key': 'registry.request.api_get_list_platforms',
                'callback': self.api_get_list_platforms

            },
            {
                'queue_name': 'registry.request.api_add_platform',
                'routing_key': 'registry.request.api_add_platform',
                'callback': self.api_add_platform
            },
            {
                'queue_name': 'driver.response.registry.api_check_configuration_changes',
                'routing_key': 'driver.response.registry.api_check_configuration_changes',
                'callback': self.handle_configuration_changes
            },
            {
                'queue_name': 'driver.response.registry.api_check_platform_active',
                'routing_key': 'driver.response.registry.api_check_platform_active',
                'callback': self.handle_check_platform_active
            }
        ]

        thread_check_active = threading.Thread(target=self.check_platform_active)
        thread_check_active.setDaemon(True)
        thread_check_active.start()

        self.client_consumer_cloud.subscribe_message(info_consumers)


if __name__ == '__main__':
    MODE_CODE = 'Develop'
    # MODE_CODE = 'Deploy'

    if MODE_CODE == 'Develop':
        BROKER_CLOUD = '0.0.0.0'  # rabbitmq
        MODE = 'PULL'  # or PUSH or PULL

        dbconfig = {
            "database": "Registry",
            "user": "root",
            "host": '0.0.0.0',
            "passwd": "root",
            "autocommit": "True"
        }

        TIME_INACTIVE_PLATFORM = 120     # Time when platform is marked inactive
        TIME_UPDATE_CONF = 5            # Time when registry send request update conf to Driver
        TIME_CHECK_PLATFORM_ACTIVE = 60  # Time when check active_platform in system
    else:
        BROKER_CLOUD = sys.argv[1]  #rabbitmq
        MODE = sys.argv[2] # or PUSH or PULL

        dbconfig = {
          "database": "Registry",
          "user":     "root",
          "host":     sys.argv[3],
          "passwd":   "root",
          "autocommit": "True"
        }

        TIME_INACTIVE_PLATFORM = int(sys.argv[4])
        TIME_UPDATE_CONF = int(sys.argv[5])
        TIME_CHECK_PLATFORM_ACTIVE = int(sys.argv[6])

    registry = Registry(BROKER_CLOUD, MODE, TIME_INACTIVE_PLATFORM, TIME_UPDATE_CONF, TIME_CHECK_PLATFORM_ACTIVE)
    registry.run()
