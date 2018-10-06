from influxdb import InfluxDBClient
import json
from kombu import Connection, Consumer, Exchange, Queue, exceptions, Producer
import sys
from datetime import datetime
import logging
from Performance_Monitoring.message_monitor_new_model import MessageMonitor
import time

class DBwriter:
    def __init__(self, broker_cloud, host_influxdb):

        # ----->configure logging <-----
        # if not os.path.exists('logging'):
        #     os.makedirs('logging')
        # handler = logging.handlers.RotatingFileHandler('logging/driver.log', maxBytes=200,
        #                               backupCount=1)
        handler = logging.StreamHandler()
        formatter = logging.Formatter(fmt='[%(asctime)s - %(levelname)s - %(name)s] - %(message)s',
                                      datefmt='%m-%d-%Y %H:%M:%S')
        handler.setFormatter(formatter)
        self.logger = logging.getLogger(__name__)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)
        # -----> end configure logging <-----

        self.clientDB = InfluxDBClient(host_influxdb, 8086, 'root', 'root', 'Collector_DB')
        self.clientDB.create_database('Collector_DB')

        self.consumer_connection = Connection(broker_cloud)
        self.producer_connection = Connection(broker_cloud)
        self.exchange = Exchange("IoT", type="direct")
        self.message_monitor = MessageMonitor('0.0.0.0', 8086)

    def write_db(self, data_points):

        self.logger.debug("Received data points: {}".format(data_points))
        records = []
        for point in data_points:
            records.append({
                'measurement': point['MetricId'],
                'tags': {
                    'DataType': point['DataType'],
                },
                'fields': {
                    'Value': point['Value'],
                },
                'time': point['TimeCollect']
            })
        try:
            self.clientDB.write_points(records)
            #self.logger.info("Updated Database")
        except:
            self.logger.error("Can't write to database : {}".format(point['MetricId']))
            # self.clientDB.drop_measurement(measurement=point['MetricId'])
            self.logger.warning("Delete mesurement: {}".format(point['MetricId']))

    def api_write_db(self, body, message):
        data_points = json.loads(body)['body']['data_points']
        start = time.time()
        self.write_db(data_points)
        self.logger.warning("TIME: {}".format(time.time()-start))

        # notify to script
        header = json.loads(body)['header']
        notification_message = {
            'header': {},
            'body': {}
        }
        #print(json.loads(body)['header'])
        queue_name = 'notification.script'
        notification_message['header']['message_monitor'] = self.message_monitor.monitor(json.loads(body)['header'], 'write_db', 'api_write_db')
        self.publish_messages(notification_message, self.producer_connection, queue_name, self.exchange)
        self.logger.warning("TIME: {}".format(time.time() - start))
        message.ack()

    def publish_messages(self, message, conn, queue_name, exchange, routing_key=None, queue_routing_key=None):
        self.logger.debug("message: {}".format(message))
        if queue_routing_key is None:
            queue_routing_key = queue_name
        if routing_key is None:
            routing_key = queue_name

        # queue_publish = Queue(name=queue_name, exchange=exchange, routing_key=queue_routing_key, message_ttl=20)

        conn.ensure_connection()
        with Producer(conn) as producer:
            producer.publish(
                json.dumps(message),
                exchange=exchange.name,
                routing_key=routing_key,
                retry=True
            )

    def run(self):
        queue_write_db = Queue(name='dbwriter.request.api_write_db', exchange=self.exchange,
                               routing_key='dbwriter.request.api_write_db', message_ttl=20)
        while 1:
            try:
                self.consumer_connection.ensure_connection(max_retries=1)
                with Consumer(self.consumer_connection, queues=queue_write_db, callbacks=[self.api_write_db]):
                    while True:
                        self.consumer_connection.drain_events()
            except (ConnectionRefusedError, exceptions.OperationalError):
                self.logger.error('Connection to Broker Cloud is lost')
            except self.consumer_connection.connection_errors:
                self.logger.error('Connection to Broker Cloud is error')


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