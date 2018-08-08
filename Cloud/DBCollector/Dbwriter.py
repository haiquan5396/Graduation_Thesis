from influxdb import InfluxDBClient
import json
from kombu import Connection, Consumer, Exchange, Queue, exceptions
import sys
from datetime import datetime


class DBwriter:
    def __init__(self, broker_cloud, host_influxdb):
        self.clientDB = InfluxDBClient(host_influxdb, 8086, 'root', 'root', 'Collector_DB')
        self.clientDB.create_database('Collector_DB')

        self.consumer_connection = Connection(broker_cloud)
        self.exchange = Exchange("IoT", type="direct")

    def write_db(self, data_points):

        print(len(data_points))
        for point in data_points:
            record = [{
                'measurement': point['MetricId'],
                'tags': {
                    'DataType': point['DataType'],
                },
                'fields': {
                    'Value': point['Value'],
                },
                'time': point['TimeCollect']
            }]
            try:
                self.clientDB.write_points(record)
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ': Updated Database')
            except:
                print("Can't write to database : {}".format(point['MetricId']))
                print("Delete mesurement....")
                self.clientDB.drop_measurement(measurement=point['MetricId'])

    def api_write_db(self, body, message):
        data_points = json.loads(body)['body']['data_points']
        self.write_db(data_points)

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

    MODE_CODE = 'Develop'
    # MODE_CODE = 'Deploy'

    if MODE_CODE == 'Develop':

        BROKER_CLOUD = "localhost"
        HOST_INFLUXDB = "localhost"
    else:
        BROKER_CLOUD = sys.argv[1]
        HOST_INFLUXDB = sys.argv[2]

    db_writer = DBwriter(BROKER_CLOUD, HOST_INFLUXDB)
    db_writer.run()