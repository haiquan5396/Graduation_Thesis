import json
from influxdb import InfluxDBClient
import sys
import Logging.config_logging as logging
from Communicator.broker_cloud import BrokerCloudClient

_LOGGER = logging.get_logger(__name__)


class Dbreader:
    def __init__(self, broker_cloud, host_influxdb):
        self.clientDB = InfluxDBClient(host_influxdb, 8086, 'root', 'root', 'Collector_DB')
        self.clientDB.create_database('Collector_DB')

        self.client_producer_cloud = BrokerCloudClient(broker_cloud, 'IoT', 'direct')
        self.client_consumer_cloud = BrokerCloudClient(broker_cloud, 'IoT', 'direct')

    def api_get_metric(self, body, message):
        _LOGGER.info("API get metric")
        list_metric_id = json.loads(body)['body']["list_metric_id"]
        reply_to = json.loads(body)['header']['reply_to']
        metrics = self.get_data_metric(list_metric_id)
        message_response = {
            'header': {},
            'body': {
                "metrics": metrics
            }
        }
        self.client_producer_cloud.publish_messages(message_response, reply_to)

    def api_get_metric_history(self, body, message):
        _LOGGER.info("API get metric history")
        list_metric_id = json.loads(body)['body']["list_metric_id"]
        reply_to = json.loads(body)['header']['reply_to']
        start_time = json.loads(body)['body']["start_time"]
        end_time = json.loads(body)['body']["end_time"]
        scale = json.loads(body)['body']["scale"]
        metrics = self.get_data_metric_history(list_metric_id, start_time, end_time, scale)
        message_response = {
            'header': {},
            'body':{
                "metrics": metrics
            }
        }

        self.client_producer_cloud.publish_messages(message_response, reply_to)

    def get_data_metric(self, list_metric_id):
        metrics = []
        for metric_id in list_metric_id:
            query_statement = 'SELECT * FROM \"' + metric_id + '\" ORDER BY time DESC LIMIT 1 '
            query_result = self.clientDB.query(query_statement)

            for data_points in query_result:
                metric = {
                    'MetricId': metric_id,
                    'DataPoint': {
                        "DataType": data_points[0]['DataType'],
                        "Value": data_points[0]['Value'],
                        "TimeCollect": data_points[0]['time']
                    }
                }
                metrics.append(metric)
        # print("metrics: {}".format(metrics))
        return metrics

    def get_data_metric_history(self, list_metric_id, start_time, end_time, scale):
        metrics = []
        # print(scale)
        for metric_id in list_metric_id:
            query_statement_type_field = """SHOW FIELD KEYS ON \"Collector_DB\" FROM \"{}\"""".format(metric_id)
            query_result_type_field = self.clientDB.query(query_statement_type_field)
            type_field = list(query_result_type_field.get_points())[0]['fieldType']
            metric = {
                'MetricId': metric_id,
                'history': []
            }
            if scale == "0s":
                if type_field == 'integer' or type_field == 'float':
                    query_statement_history = """SELECT *  FROM \"{}\" where time >= \'{}\' AND time <= \'{}\'""".format(
                        metric_id, start_time, end_time)
                    query_result_history = self.clientDB.query(query_statement_history)
                    query_result_history = list(query_result_history.get_points())

                    query_statement_global = """SELECT MAX(\"Value\") AS max_state, MIN(\"Value\") AS min_state, MEAN(\"Value\") AS average_state FROM \"{}\" where time >= \'{}\' AND time <= \'{}\'""".format(
                        metric_id, start_time, end_time)
                    query_result_global = self.clientDB.query(query_statement_global)
                    query_result_global = list(query_result_global.get_points())
                    # print(query_result_global[0]['max_state'], query_result_global[0]['min_state'],
                    #       query_result_global[0]['average_state'])

                    if len(query_result_history) > 0 and len(query_result_global) > 0:

                        metric['max_global'] = query_result_global[0]['max_state']
                        metric['min_global'] = query_result_global[0]['min_state']
                        metric['average_global'] = query_result_global[0]['average_state']

                        for data_history in query_result_history:
                            point = {
                                'TimeCollect': data_history['time'],
                                'DataType': data_history['DataType'],
                                'Value': data_history['Value']
                            }
                            metric['history'].append(point)
                    metrics.append(metric)
                else:

                    query_statement_history = """SELECT *  FROM \"{}\" where time >= \'{}\' AND time <= \'{}\'""".format(
                        metric_id, start_time, end_time)
                    query_result_history = self.clientDB.query(query_statement_history)
                    query_result_history = list(query_result_history.get_points())

                    if len(query_result_history) > 0:
                        for data_history in query_result_history:
                            point = {
                                'TimeCollect': data_history['time'],
                                'DataType': data_history['DataType'],
                                'Value': data_history['Value']
                            }
                            metric['history'].append(point)
                    metrics.append(metric)
            else:
                if type_field == 'integer' or type_field == 'float':
                    query_statement_history = """SELECT MODE(\"Value\") AS mode_state, MAX(\"Value\") AS max_state, MIN(\"Value\") AS min_state, MEAN(\"Value\") AS average_state FROM \"{}\" WHERE time >= \'{}\' AND time <= \'{}\' GROUP BY time({})""".format(
                        metric_id, start_time, end_time, scale)
                    query_result_history = self.clientDB.query(query_statement_history)
                    query_result_history = list(query_result_history.get_points())

                    query_statement_global = """SELECT MAX(\"Value\") AS max_state, MIN(\"Value\") AS min_state, MEAN(\"Value\") AS average_state FROM \"{}\" where time >= \'{}\' AND time <= \'{}\'""".format(
                        metric_id, start_time, end_time)
                    query_result_global = self.clientDB.query(query_statement_global)
                    query_result_global = list(query_result_global.get_points())

                    if len(query_result_history) > 0 and len(query_result_global) > 0:

                        metric['max_global'] = query_result_global[0]['max_state']
                        metric['min_global'] = query_result_global[0]['min_state']
                        metric['average_global'] = query_result_global[0]['average_state']

                        for data_history in query_result_history:
                            point = {
                                'min_value': data_history['min_state'],
                                'max_value': data_history['max_state'],
                                'average_value': data_history['average_state'],
                                'TimeCollect': data_history['time'],
                                'Value': data_history['mode_state']
                            }
                            metric['history'].append(point)
                    metrics.append(metric)

                else:
                    query_statement_history = """SELECT MODE(\"Value\") AS mode_state FROM \"{}\" WHERE time >= \'{}\' AND time <= \'{}\' GROUP BY time({})""".format(
                        metric_id, start_time, end_time, scale)
                    query_result_history = self.clientDB.query(query_statement_history)
                    query_result_history = list(query_result_history.get_points())

                    if len(query_result_history) > 0:
                        for data_history in query_result_history:
                            point = {
                                'TimeCollect': data_history['time'],
                                'Value': data_history['mode_state']
                            }
                            metric['history'].append(point)
                    metrics.append(metric)

        return metrics

    def run(self):
        info_consumers = [
            {
                'queue_name': 'dbreader.request.api_get_metric',
                'routing_key': 'dbreader.request.api_get_metric',
                'callback': self.api_get_metric
            },
            {
                'queue_name': 'dbreader.request.api_get_metric_history',
                'routing_key': 'dbreader.request.api_get_metric_history',
                'callback': self.api_get_metric_history
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

    db_reader = Dbreader(BROKER_CLOUD, HOST_INFLUXDB)
    db_reader.run()