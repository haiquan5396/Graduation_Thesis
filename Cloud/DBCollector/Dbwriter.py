from influxdb import InfluxDBClient
import json
from kombu import Connection, Consumer, Exchange, Queue, exceptions
import sys
from datetime import datetime
from Performance_Monitoring.message_monitor import MessageMonitor


class DBwriter:
    def __init__(self, broker_cloud, host_influxdb):
        self.clientDB = InfluxDBClient(host_influxdb, 8086, 'root', 'root', 'Collector_DB')
        self.clientDB.create_database('Collector_DB')

        self.consumer_connection = Connection(broker_cloud)
        self.exchange = Exchange("IoT", type="direct")
        self.message_monitor = MessageMonitor('0.0.0.0', 8086)

    def write_db(self, list_things):
        # print("Write to database")
        data_write_db = []
        for thing in list_things['things']:
            for item in thing['items']:
                record = {
                    'measurement': item['item_global_id'],
                    'tags': {
                        'platform_id': list_things['platform_id'],
                        'thing_type': thing['thing_type'],
                        'thing_name': thing['thing_name'],
                        'thing_global_id': thing['thing_global_id'],
                        'thing_local_id': thing['thing_local_id'],
                        'location': thing['location'],
                        'item_type': item['item_type'],
                        'item_name': item['item_name'],
                        'item_global_id': item['item_global_id'],
                        'item_local_id': item['item_local_id'],
                        'can_set_state': item['can_set_state'],
                    },
                    'fields': {
                        'item_state': item['item_state'],
                    },
                    'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }

                data_write_db.append(record)

        self.clientDB.write_points(data_write_db)
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ': Updated Database')

    def api_write_db(self, body, message):
        list_things = json.loads(body)
        self.write_db(list_things)
        self.message_monitor.end_message(list_things, 'write_db', 'api_write_db')

    def run(self):
        queue_write_db = Queue(name='dbwriter.request.api_write_db', exchange=self.exchange,
                               routing_key='dbwriter.request.api_write_db', message_ttl=20)
        while 1:
            try:
                self.consumer_connection.ensure_connection(max_retries=1)
                with Consumer(self.consumer_connection, queues=queue_write_db, callbacks=[self.api_write_db], no_ack=True):
                    while True:
                        self.consumer_connection.drain_events()
            except (ConnectionRefusedError, exceptions.OperationalError):
                print('Connection lost')
            except self.consumer_connection.connection_errors:
                print('Connection error')


if __name__ == '__main__':

    # MODE_CODE = 'Develop'
    MODE_CODE = 'Deploy'

    if MODE_CODE == 'Develop':

        BROKER_CLOUD = "localhost"
        HOST_INFLUXDB = "localhost"
    else:
        BROKER_CLOUD = sys.argv[1]
        HOST_INFLUXDB = sys.argv[2]

    db_writer = DBwriter(BROKER_CLOUD, HOST_INFLUXDB)
    db_writer.run()