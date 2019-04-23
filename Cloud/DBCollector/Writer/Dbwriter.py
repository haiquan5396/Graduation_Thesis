from influxdb import InfluxDBClient
import json
import sys
import Logging.config_logging as logging
from Communicator.broker_cloud import BrokerCloudClient

_LOGGER = logging.get_logger(__name__)


class DBwriter:
    def __init__(self, broker_cloud, host_influxdb):
        self.clientDB = InfluxDBClient(host_influxdb, 8086, 'root', 'root', 'Collector_DB')
        self.clientDB.create_database('Collector_DB')

        # self.client_producer_cloud = BrokerCloudClient(broker_cloud, 'IoT', 'direct')
        self.client_consumer_cloud = BrokerCloudClient(broker_cloud, 'IoT', 'direct')

    def write_db(self, data_points):

        _LOGGER.debug("Received data points: {}".format(data_points))
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
            # _LOGGER.info("Updated Database")
        except:
            for record in records:
                try:
                    self.clientDB.write_points([record])
                except:
                    _LOGGER.error("Can't write to database : {}".format(record['measurement']))
                    self.clientDB.drop_measurement(measurement=record['measurement'])
                    _LOGGER.warning("Delete mesurement: {}".format(record['measurement']))
                    self.clientDB.write_points([record])

    def api_write_db(self, body, message):
        data_points = json.loads(body)['body']['data_points']
        self.write_db(data_points)

    def run(self):
        info_consumers = [
            {
                'queue_name': 'dbwriter.request.api_write_db',
                'routing_key': 'dbwriter.request.api_write_db',
                'callback': self.api_write_db
            }
        ]
        self.client_consumer_cloud.subscribe_message(info_consumers)


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
