import json
import threading
from kombu import Producer, Connection, Consumer, exceptions, Exchange, Queue, uuid
from kombu.utils.compat import nested
import sys
#from Performance_Monitoring.message_monitor import MessageMonitor
from datetime import datetime


class Collector():
    def __init__(self, broker_cloud, mode, time_collect):
        self.mode = mode
        self.time_collect = time_collect
        self.producer_connection = Connection(broker_cloud)
        self.consumer_connection = Connection(broker_cloud)
        self.exchange = Exchange("IoT", type="direct")
        self.list_platform_id = []
        #self.message_monitor = MessageMonitor('0.0.0.0', 8086)

    def collect(self):
        print("Collect the states of the devices")
        for platform_id in self.list_platform_id:
            self.collect_by_platform_id(platform_id)
        # threading.Timer(self.time_collect, self.collect).start()

    def collect_by_platform_id(self, platform_id):
        print('Collect data from platform_id: ', str(platform_id))
        message_request = {
            'header':{
                'reply_to': 'driver.response.collector.api_get_states',
                'PlatformId': platform_id,
                'mode': 'PULL'
            }
        }

        #message_request['message_monitor'] = self.message_monitor.monitor({}, 'collector', 'collect_by_platform_id')

        request_queue = Queue(name='driver.request.api_get_states', exchange=self.exchange,
                              routing_key='driver.request.api_get_states', message_ttl=20)
        request_routing_key = 'driver.request.api_get_states'
        self.producer_connection.ensure_connection()
        with Producer(self.producer_connection) as producer:
            producer.publish(
                json.dumps(message_request),
                exchange=self.exchange.name,
                routing_key=request_routing_key,
                declare=[request_queue],
                retry=True
            )

    def handle_collect_by_platform_id(self, body, message):
        print('Recived state from platform_id: ', json.loads(body)['header']['PlatformId'])
        # print(msg.payload.decode('utf-8'))
        # print(ast.literal_eval(msg.payload.decode('utf-8')))
        states = json.loads(body)['body']['states']
        # print(list_things)
        #list_things['message_monitor'] = self.message_monitor.monitor(list_things, 'collector', 'handle_collect_by_platform_id')
        points = []
        for state in states:
            points.append({
                "MetricId": state['MetricId'],
                "DataType": state['DataPoint']['DataType'],
                "Value": state['DataPoint']['Value'],
                "TimeCollect": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })

        message = {
            'header': {},
            'body': {
                'data_points': points
            }
        }
        request_queue = Queue(name='dbwriter.request.api_write_db', exchange=self.exchange,
                              routing_key='dbwriter.request.api_write_db', message_ttl=20)
        request_routing_key = 'dbwriter.request.api_write_db'
        self.producer_connection.ensure_connection()
        with Producer(self.producer_connection) as producer:
            producer.publish(
                json.dumps(message),
                exchange=self.exchange.name,
                routing_key=request_routing_key,
                declare=[request_queue],
                retry=True
            )
        print('Send new state to Dbwriter: {}'.format(message))

        # # send to monitor
        # request_queue = Queue(name='monitor.request.collector', exchange=self.exchange,
        #                       routing_key='monitor.request.collector', message_ttl=20)
        # request_routing_key = 'monitor.request.collector'
        # with Producer(self.producer_connection) as producer:
        #     producer.publish(
        #         json.dumps(list_things),
        #         exchange=self.exchange.name,
        #         routing_key=request_routing_key,
        #         declare=[request_queue],
        #         retry=True
        #     )

    def get_list_platforms(self):
        print("Get list platforms from Registry")
        message = {
            "header": {
                'reply_to': 'registry.response.collector.api_get_list_platforms',
                'PlatformStatus': "active"
            }
        }

        #message['message_monitor'] = self.message_monitor.monitor({}, 'collector', 'get_list_platforms')

        queue = Queue(name='registry.request.api_get_list_platforms', exchange=self.exchange,
                      routing_key='registry.request.api_get_list_platforms', message_ttl=20)
        routing_key = 'registry.request.api_get_list_platforms'
        self.producer_connection.ensure_connection()
        with Producer(self.producer_connection) as producer:
            producer.publish(
                json.dumps(message),
                exchange=self.exchange.name,
                routing_key=routing_key,
                declare=[queue],
                retry=True
            )

    def handle_get_list(self, body, message):
        list_platforms = json.loads(body)['body']['list_platforms']
        temp = []
        for platform in list_platforms:
            temp.append(platform['PlatformId'])

        self.list_platform_id = temp
        self.collect()
        print("New list platform: {}".format(list_platforms))
        print('Updated list of platform_id: ', str(self.list_platform_id))

    def handle_notification(self, body, message):
        print('Have Notification')
        if json.loads(body)['notification'] == 'Have Platform_id change':
            self.get_list_platforms()

    def run(self):
        queue_notification = Queue(name='collector.request.notification', exchange=self.exchange,
                                   routing_key='collector.request.notification', message_ttl=20)
        queue_list_platforms = Queue(name='registry.response.collector.api_get_list_platforms', exchange=self.exchange,
                                     routing_key='registry.response.collector.api_get_list_platforms', message_ttl=20)
        queue_get_states = Queue(name='driver.response.collector.api_get_states', exchange=self.exchange,
                                 routing_key='driver.response.collector.api_get_states', message_ttl=20)

        # if self.mode == 'PULL':
        #     print("Collector use Mode: PULL Data")
        self.get_list_platforms()
        #     self.collect()

        while 1:
            try:
                self.consumer_connection.ensure_connection(max_retries=1)
                with nested(Consumer(self.consumer_connection, queues=queue_notification, callbacks=[self.handle_notification],
                                     no_ack=True),
                            Consumer(self.consumer_connection, queues=queue_list_platforms, callbacks=[self.handle_get_list],
                                     no_ack=True),
                            Consumer(self.consumer_connection, queues=queue_get_states,
                                     callbacks=[self.handle_collect_by_platform_id], no_ack=True)):
                    while True:
                        self.consumer_connection.drain_events()
            except (ConnectionRefusedError, exceptions.OperationalError):
                print('Connection lost')
            except self.consumer_connection.connection_errors:
                print('Connection error')


if __name__ == '__main__':

    MODE_CODE = 'Develop'
    # MODE_CODE = 'Deploy'

    if MODE_CODE == 'Develop':
        BROKER_CLOUD = "localhost"
        MODE = "PULL"
        TIME_COLLECT = 5
    else:
        BROKER_CLOUD = sys.argv[1]
        MODE = sys.argv[2] #PULL or PUSH
        TIME_COLLECT = int(sys.argv[3])

    collector = Collector(BROKER_CLOUD, MODE, TIME_COLLECT)
    collector.run()