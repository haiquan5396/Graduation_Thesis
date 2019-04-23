import json
import configparser
import threading
from ast import literal_eval
import os
import copy
import requests
import time
import Logging.config_logging as logging
from Communicator.broker_fog import BrokerFogClient

_LOGGER = logging.get_logger(__name__)


class Driver:

    def __init__(self, config_path, time_push):
        self.now_info = []
        self.now_metric_domain = {}     # 'local_id': 'metric_domain'
        self.list_mapping_id = {}       # 'local_id': 'global_id'
        self.info_receive_from_registry = []
        config = configparser.ConfigParser()
        config.read(config_path)
        self.time_push_config = int(time_push)
        self.host = config['PLATFORM']['host']
        self.port = config['PLATFORM']['port']
        self.platform_name = config['PLATFORM']['platform_name']
        self.platform_type = config['PLATFORM']['platform_type']
        self.platform_id = None

        my_path = os.path.dirname(__file__)
        filename = os.path.join(my_path, '../../Semantic_Analysis/metric_domain.json')

        with open(filename) as json_file:
            self.metric_domain_file = json.load(json_file)

        broker_fog = config['BROKER']['host']
        self.clientMQTT = BrokerFogClient(broker_fog)

        registration = {
            "header": {},
            "body": {
                'PlatformHost': self.host,
                'PlatformPort': self.port,
                'PlatformType': self.platform_type,
                'PlatformName': self.platform_name
            }
        }

        if 'platform_id' in config['PLATFORM']:
            _LOGGER.info("Platform have a platform_id")
            registration["header"]["registered"] = True
            registration["header"]["PlatformId"] = config['PLATFORM']['platform_id']
        else:
            _LOGGER.info("Platform don't have a platform_id")
            registration["header"]["registered"] = False

        topic_response = 'registry/response/' + self.host + "/" + self.port

        check_response = 0

        def handle_init(client, userdata, msg):

            nonlocal check_response
            _LOGGER.debug("Response from Registry "+str(json.loads(msg.payload.decode('utf-8'))))
            header = json.loads(msg.payload.decode('utf-8'))['header']
            body = json.loads(msg.payload.decode('utf-8'))['body']
            self.platform_id = header['PlatformId']
            if 'platform_id' not in config['PLATFORM']:
                with open(config_path, 'w') as file:
                    config['PLATFORM']['platform_id'] = self.platform_id
                    config.write(file)

            _LOGGER.info('Platform_id: ' + self.platform_id)
            self.handle_info_from_registry(info_receive_from_registry=body['sources'], init_time=True)
            self.clientMQTT.unsubscribe([topic_response])
            check_response = 1

        info_subscriber_init = [
            {
                'topic': topic_response,
                'callback': handle_init
            }
        ]

        self.clientMQTT.subscribe_message(info_subscriber_init)
        _LOGGER.debug('Registration: ' + str(registration))
        self.clientMQTT.publish_message('registry/request/api_add_platform', json.dumps(registration))
        while self.platform_id is None or check_response == 0:
            _LOGGER.debug("Wait for Registry response")
            self.clientMQTT.loop_one_time()

        info_subscribers = [
            {
                'topic': str(self.platform_id) + '/request/api_get_states',
                'callback': self.api_get_states
            },
            {
                'topic': str(self.platform_id) + '/request/api_update_now_configuration',
                'callback': self.api_update_now_configuration
            },
            {
                'topic': str(self.platform_id) + '/request/api_check_platform_active',
                'callback': self.check_platform_active
            },
            {
                'topic': str(self.platform_id) + '/request/api_check_configuration_changes',
                'callback': self.api_check_configuration_changes
            },
            {
                'topic': str(self.platform_id) + '/request/api_set_state',
                'callback': self.api_set_state
            }
        ]
        self.clientMQTT.subscribe_message(info_subscribers)

    def handle_info_from_registry(self, info_receive_from_registry, init_time=False):
        new_info = []
        _LOGGER.debug('Information received from the Registry: {}'.format(info_receive_from_registry))
        for source in info_receive_from_registry:
            temp_source = {}
            self.list_mapping_id[source['information']['LocalId']] = source['information']['SourceId']

            # delete SourceStatus, MetricStatus, SourceId, MetricId
            temp_source['information'] = copy.deepcopy(source['information'])
            del temp_source['information']['SourceId']
            if "SourceStatus" in temp_source['information']:
                del temp_source['information']['SourceStatus']
            metrics = []
            for metric in source['metrics']:
                self.list_mapping_id[metric['MetricLocalId']] = metric['MetricId']
                self.now_metric_domain[metric['MetricLocalId']] = metric['MetricDomain']
                temp_metric = copy.deepcopy(metric)
                del temp_metric['MetricId']
                if "MetricStatus" in metric:
                    del temp_metric["MetricStatus"]
                metrics.append(temp_metric)

            temp_source['metrics'] = metrics
            new_info.append(temp_source)
        if init_time is False:
            print("UPDATE: {}".format(self.now_info))
            self.now_info = new_info
        else:
            print("INIT: {}".format(self.now_info))

            self.now_info = []

    def mapping_id(self, info, ids, is_config=True):
        if is_config is True:
            for source in info:
                if source['information']['LocalId'] in ids:
                    source['information']['SourceId'] = ids[source['information']['LocalId']]

                for metric in source['metrics']:
                    if metric['MetricLocalId'] in ids:
                        metric['MetricId'] = ids[metric['MetricLocalId']]
        else:
            for metric in info:
                if metric['MetricLocalId'] in ids:
                    metric['MetricId'] = ids[metric['MetricLocalId']]

    def api_get_states(self, client, userdata, msg):
        _LOGGER.info("API get states")
        message = json.loads(msg.payload.decode('utf-8'))
        states = self.get_states()

        message_response = {
            "header": message['header'],
            "body": {}
        }

        self.mapping_id(states, copy.deepcopy(self.list_mapping_id), is_config=False)
        message_response['body']['states'] = states
        self.clientMQTT.publish_message('driver/response/filter/api_get_states', json.dumps(message_response))

    def api_check_configuration_changes(self, client, userdata, msg):
        _LOGGER.info('API check configuration changes')
        message = json.loads(msg.payload.decode('utf-8'))
        message_response = {
            "header": message['header'],
            "body": {}
        }

        config = self.check_configuration_changes()
        self.mapping_id(config['new_info'], copy.deepcopy(self.list_mapping_id))
        message_response['body'] = config
        self.clientMQTT.publish_message('driver/response/forwarder/api_check_configuration_changes', json.dumps(message_response))

    # This API is called when driver send configuration change
    # then registry response active_sources to driver for update now_configuration
    def api_update_now_configuration(self, client, userdata, msg):
        print("VAOOOOOOOOOOOO")
        message = json.loads(msg.payload.decode('utf-8'))
        _LOGGER.info("API update now configuration")
        self.handle_info_from_registry(info_receive_from_registry=message['body']['active_sources'])

    def api_set_state(self, client, userdata, msg):
        message = json.loads(msg.payload.decode('utf-8'))
        body = message['body']
        metric_local_id = body['metric']['MetricLocalId']
        metric_name = body['metric']['MetricName']
        metric_domain = body['metric']['MetricDomain']
        new_value = body['new_value']
        _LOGGER.info("API set state: {} to {}".format(metric_name, new_value))
        self.set_state(metric_local_id, metric_name, metric_domain, new_value)

    def push_configuration_changes(self):
        # time.sleep(10)
        while 1:
            message = {
                "header":{},
                "body": {}
            }
            config = self.check_configuration_changes()
            self.mapping_id(config['new_info'], copy.deepcopy(self.list_mapping_id))
            message['body'] = config
            if message['body']['is_change'] is True:
                message['header']['reply_to'] = 'driver.response.registry.api_check_configuration_changes'
                message['header']['PlatformId'] = self.platform_id
                message['header']['mode'] = "PUSH"
                _LOGGER.info("Push to Registry info because have change")
                self.clientMQTT.publish_message('driver/response/forwarder/api_check_configuration_changes', json.dumps(message))
            else:
                _LOGGER.info("Don't push to Registry info because no change")
                _LOGGER.debug("Message: {}".format(message))
            time.sleep(self.time_push_config)

    def push_get_state(self):
        while 1:
            message = {
                "header":{},
                "body": {}
            }
            states = self.get_states()
            message['header']['reply_to'] = 'driver.response.collector.api_get_states'
            message['header']['PlatformId'] = self.platform_id
            message['header']['mode'] = "PUSH"
            self.mapping_id(states, copy.deepcopy(self.list_mapping_id), is_config=False)
            message['body']['states'] = states
            _LOGGER.debug("Push state to Filter")
            self.clientMQTT.publish_message('driver/response/filter/api_get_states', json.dumps(message))
            time.sleep(2)

    def get_states(self):
        pass

    def set_state(self, metric_local_id, metric_name, metric_domain, new_value):
        pass

    def check_configuration_changes(self):
        pass

    def create_registration(self, driver_id, driver_type, driver_host):
        registration = {
            "header": {},
            "body": {
                'driver_host': driver_host,
                'driver_type': driver_type
            }
        }

        if driver_id is None:
            _LOGGER.info("Platform don't have a PlatformId")
            registration["header"]["registered"] = False
        else:
            _LOGGER.info("Platform have a PlatformId")
            registration["header"]["registered"] = True
            registration["header"]["DriverId"] = driver_id

        return registration

    def check_platform_active(self):
        while 1:
            _LOGGER.debug('Check platform active and send to Registry')
            message = {
                "header": {
                    'PlatformId': self.platform_id,
                    'reply_to': 'driver.response.registry.api_check_platform_active'
                },
                "body": {}
            }

            try:
                response = requests.get('http://' + self.host + ':' + self.port)
                if response.status_code == 200:
                    message['body']['active'] = True
                    self.clientMQTT.publish_message('driver/response/forwarder/api_check_platform_active', json.dumps(message))
                else:
                    _LOGGER.debug('Check platform active and realize it inactive')
                    pass
            except:
                _LOGGER.debug('Check platform active and raise exception')
                pass
            time.sleep(20)

    def detect_metric_domain(self, sentence, value):
        # value must casted to the corresponding type before pass to function
        #print("sentence: {}, value: {}".format(sentence, value))
        max_score = 0
        domain = "unknown"
        sentence = sentence.lower()

        for key in self.metric_domain_file.keys():
            score_domain = 0
            # Count words
            for word in sentence.split(" "):
                for word_domain in self.metric_domain_file[key]["words"]:
                    if word_domain in word:
                        score_domain = score_domain + 1

            # Check if value in value domain
            value_domain = self.metric_domain_file[key]["value"]
            if isinstance(value_domain, list):
                if value in value_domain:
                    score_domain = score_domain + 1
                if 'mapping' in self.metric_domain_file[key]:
                    if str(value) in self.metric_domain_file[key]['mapping']:
                        score_domain = score_domain + 1

            elif value_domain == "number":
                if isinstance(value, int) or isinstance(value, float):
                    score_domain = score_domain + 1

            if score_domain > max_score:
                max_score = score_domain
                domain = key
        #print("domain: {}".format(domain))
        return domain

    # This function mapping value of data point to unified style
    def mapping_data_value(self, domain_name, value, datatype):
        value_domain = self.metric_domain_file[domain_name]["value"]
        if isinstance(value_domain, list):
            if value in value_domain:
                return [value, datatype]
            elif 'mapping' in self.metric_domain_file[domain_name]:
                if value in self.metric_domain_file[domain_name]['mapping']:
                    value_mapped = self.metric_domain_file[domain_name]['mapping'][value]
                    return [value_mapped, self.detect_data_type(str(value_mapped))[0]]
            else:
                return "ERROR typedata"

        elif value_domain == "number":
            if datatype == "float" or datatype == "int":
                return [value, datatype]
            else:
                return "ERROR typedata"

    @staticmethod
    def detect_data_type(value):

        try:
            number = literal_eval(str(value))
        except:
            return ["string", value]

        if isinstance(number, int) or (isinstance(number, float) and number.is_integer()):
            return ["int", int(number)]
        elif isinstance(number, float):
            return ["float", float(number)]
        else:
            return ["unknown", value]

    def ordered(self, obj):
        if isinstance(obj, dict):
            return sorted((k, self.ordered(v)) for k, v in obj.items())
        if isinstance(obj, list):
            return sorted(self.ordered(x) for x in obj)
        else:
            return obj

    def run(self):

        thread_push_check_active= threading.Thread(target=self.check_platform_active)
        thread_push_check_active.setDaemon(True)
        thread_push_check_active.start()

        thread_push_get_state= threading.Thread(target=self.push_get_state)
        thread_push_get_state.setDaemon(True)
        thread_push_get_state.start()

        thread_check_config= threading.Thread(target=self.push_configuration_changes)
        thread_check_config.setDaemon(True)
        thread_check_config.start()

        self.clientMQTT.loop(loop_forever=True)