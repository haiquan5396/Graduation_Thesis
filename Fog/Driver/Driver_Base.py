import paho.mqtt.client as mqtt
import json
import configparser
import threading
from Performance_Monitoring.message_monitor import MessageMonitor

import time
class Driver:

    def __init__(self, config_path, mode, time_push):
        config = configparser.ConfigParser()
        config.read(config_path)
        self.mode = mode    #PULL or PUSH
        self.time_push = time_push
        self.host = config['PLATFORM']['host']
        self.port = config['PLATFORM']['port']
        self.platform_name = config['PLATFORM']['platform_name']
        self.platform_id = None

        broker_fog = config['BROKER']['host']
        self.clientMQTT = mqtt.Client('DRIVER')
        self.clientMQTT.connect(broker_fog)
        self.clientMQTT.on_connect = self.on_connect
        self.clientMQTT.on_disconnect = self.on_disconnect

        if 'platform_id' in config['PLATFORM']:
            print("Have platform_id")
            # self.platform_id = config['PLATFORM']['platform_id']
            message = {
                'platform_name': self.platform_name,
                'host': self.host,
                'port': self.port,
                'platform_id': config['PLATFORM']['platform_id']
            }
        else:
            print('Init and get platform_id from Registry')
            message = {
                'platform_name': self.platform_name,
                'host': self.host,
                'port': self.port,
            }

        topic_response = 'registry/response/' + self.host + '/' + self.port

        check_response = 0

        def handle_init(client, userdata, msg):
            print('Handle_init')
            nonlocal check_response
            self.platform_id = json.loads(msg.payload.decode('utf-8'))['platform_id']
            if 'platform_id' not in config['PLATFORM']:
                with open(config_path, 'w') as file:
                    config['PLATFORM']['platform_id']= self.platform_id
                    config.write(file)
                print('Platform_id received: ', self.platform_id)
            self.clientMQTT.unsubscribe(topic_response)
            check_response = 1

        self.clientMQTT.subscribe(topic_response)
        self.clientMQTT.message_callback_add(topic_response, handle_init)
        # self.clientMQTT.loop_start()
        self.clientMQTT.publish('registry/request/api_add_platform', json.dumps(message))

        while self.platform_id is None or check_response == 0:
            print("Wait for Registry response")
            self.clientMQTT.loop()

        if mode == 'PULL':
            self.clientMQTT.subscribe(str(self.platform_id) + '/request/api_get_states')
            self.clientMQTT.message_callback_add(str(self.platform_id) + '/request/api_get_states', self.api_get_states)

            self.clientMQTT.subscribe(str(self.platform_id) + '/request/api_check_configuration_changes')
            self.clientMQTT.message_callback_add(str(self.platform_id) + '/request/api_check_configuration_changes', self.api_check_configuration_changes)

        self.clientMQTT.subscribe(str(self.platform_id) + '/request/api_set_state', qos=2)
        self.clientMQTT.message_callback_add(str(self.platform_id) + '/request/api_set_state', self.api_set_state)
        # self.clientMQTT.loop_stop()
        self.message_monitor = MessageMonitor('0.0.0.0', 8086)

    def api_get_states(self, client, userdata, msg):
        body = json.loads(msg.payload.decode('utf-8'))
        reply_to = body['reply_to']
        message_response = dict()
        message_response = self.get_states()
        message_response['reply_to'] = reply_to
        # message_response['message_monitor']={
        #     'start_time': 123.2,
        #     'from_service': "111",
        #     'from_function': "2222",
        #     'id': "aaaa"
        # }
        message_response['message_monitor'] = self.message_monitor.monitor(body, 'driver', 'api_get_states')
        self.clientMQTT.publish('driver/response/filter/api_get_states', json.dumps(message_response))

    def api_check_configuration_changes(self, client, userdata, msg):
        print('api_check_configuration_changes')
        body = json.loads(msg.payload.decode('utf-8'))
        message_response = dict()
        message_response = self.check_configuration_changes()
        message_response['reply_to'] = body['reply_to']
        message_response['message_monitor'] = self.message_monitor.monitor(body, 'driver', 'api_check_configuration_changes')

        # message_response['message_monitor']={
        #     'start_time': 23.3,
        #     'from_service': "111",
        #     'from_function': "2222",
        #     'id': "aaaa"
        # }
        self.clientMQTT.publish('driver/response/forwarder/api_check_configuration_changes', json.dumps(message_response))

    def api_set_state(self, client, userdata, msg):
        message = json.loads(msg.payload.decode('utf-8'))
        thing_local_id = message['thing_local_id']
        thing_type = message['thing_type']
        item_local_id = message['item_local_id']
        item_type = message['item_type']
        new_state = message['new_state']
        location = message['location']
        thing_name = message['thing_name']
        item_name = message['item_name']
        self.set_state(thing_type, thing_local_id, location, thing_name,
                       item_type, item_local_id, item_name, new_state)
        self.message_monitor.end_message(message, 'driver', 'api_set_state')

    def on_disconnect(self, client, userdata, rc):
        if rc != 0:
            print("disconnect to Mosquitto.")

    def on_connect(self, client, userdata, flags, rc):
        print("connected to Mosquitto")
        if self.platform_id is not None:
            if self.mode == 'PULL':
                self.clientMQTT.subscribe(str(self.platform_id) + '/request/api_get_states')
                self.clientMQTT.message_callback_add(str(self.platform_id) + '/request/api_get_states', self.api_get_states)

                self.clientMQTT.subscribe(str(self.platform_id) + '/request/api_check_configuration_changes')
                self.clientMQTT.message_callback_add(str(self.platform_id) + '/request/api_check_configuration_changes', self.api_check_configuration_changes)

            self.clientMQTT.subscribe(str(self.platform_id) + '/request/api_set_state', qos=2)
            self.clientMQTT.message_callback_add(str(self.platform_id) + '/request/api_set_state', self.api_set_state)

    def run(self):
        if self.mode == 'PUSH':
            self.push_configuration_changes()
            self.push_get_state()
        self.clientMQTT.loop_forever()

    def push_configuration_changes(self):
        TIME_PUSH_CONFIG = self.time_push
        message = self.check_configuration_changes()
        message['reply_to'] = 'driver.response.registry.api_check_configuration_changes'
        message['message_monitor'] = self.message_monitor.monitor({}, 'driver', 'push_configuration_changes')

        self.clientMQTT.publish('driver/response/forwarder/api_check_configuration_changes', json.dumps(message))
        threading.Timer(TIME_PUSH_CONFIG, self.push_configuration_changes).start()

    def push_get_state(self):
        TIME_PUSH_STATE = self.time_push
        message = self.get_states()
        message['reply_to'] = 'driver.response.collector.api_get_states'
        message['message_monitor'] = self.message_monitor.monitor({}, 'driver', 'push_get_state')
        self.clientMQTT.publish('driver/response/filter/api_get_states', json.dumps(message))
        threading.Timer(TIME_PUSH_STATE, self.push_get_state).start()

    def get_states(self):
        pass

    def set_state(self, thing_type, thing_local_id, location, thing_name,
                  item_type, item_local_id, item_name, new_state):
        pass

    def check_configuration_changes(self):
        pass
