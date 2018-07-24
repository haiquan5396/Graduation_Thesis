import json
from kombu import Producer, Connection, Consumer, exceptions, Exchange, Queue
from kombu.utils.compat import nested
from influxdb import InfluxDBClient
import sys


class Dbreader:
    def __init__(self, broker_cloud, host_influxdb):
        self.clientDB = InfluxDBClient(host_influxdb, 8086, 'root', 'root', 'Collector_DB')
        self.clientDB.create_database('Collector_DB')

        self.producer_connection = Connection(broker_cloud)
        self.consumer_connection = Connection(broker_cloud)
        self.exchange = Exchange("IoT", type="direct")

    def get_item_state(sefl, list_item_global_id):
        items = []
        for item_global_id in list_item_global_id:
            query_statement = 'SELECT * FROM \"' + item_global_id + '\" ORDER BY time DESC LIMIT 1 '
            query_result = sefl.clientDB.query(query_statement)

            for item in query_result:
                item_state = {
                    'item_global_id': item[0]['item_global_id'],
                    'item_state': item[0]['item_state'],
                    # 'last_changed': covert_time_to_correct_time_zone(item[0]['time']),
                    'last_changed': item[0]['time'],
                    'thing_global_id': item[0]['thing_global_id']
                }
                items.append(item_state)
        return items

    def api_get_item_state(self, body, message):
        print("API get_item_state")
        # Message {'list_item_global_id': [], 'reply_to': " ", }
        list_item_global_id = json.loads(body)["list_item_global_id"]
        reply_to = json.loads(body)['reply_to']
        items = self.get_item_state(list_item_global_id)
        message_response = {
            "items": items
        }
        self.producer_connection.ensure_connection()
        with Producer(self.producer_connection) as producer:
            producer.publish(
                json.dumps(message_response),
                exchange=self.exchange.name,
                routing_key=reply_to,
                retry=True
            )
            # print("Done: {}".format(items))

    def api_get_item_state_history(self, body, message):
        print("API get_item_state_history")
        # Message {'list_item_global_id': [], 'reply_to': " ", }
        list_global_id = json.loads(body)["list_global_id"]
        reply_to = json.loads(body)['reply_to']
        start_time = json.loads(body)["start_time"]
        end_time = json.loads(body)["end_time"]
        scale = json.loads(body)["scale"]
        items = self.get_item_state_history(list_global_id, start_time, end_time, scale)
        message_response = {
            "items": items
        }
        self.producer_connection.ensure_connection()
        with Producer(self.producer_connection) as producer:
            producer.publish(
                json.dumps(message_response),
                exchange=self.exchange.name,
                routing_key=reply_to,
                retry=True
            )

    def get_item_state_history(self, list_global_id, start_time, end_time, scale):
        items = []
        print(scale)
        for global_id in list_global_id:
            item_global_id = global_id['item_global_id']
            thing_global_id = global_id['thing_global_id']
            query_statement_type_field = """SHOW FIELD KEYS ON \"Collector_DB\" FROM \"{}\"""".format(item_global_id)
            query_result_type_field = self.clientDB.query(query_statement_type_field)
            type_field = list(query_result_type_field.get_points())[0]['fieldType']

            if scale == "0s":
                if type_field == 'integer' or type_field == 'float':
                    query_statement_history = """SELECT *  FROM \"{}\" where time >= \'{}\' AND time <= \'{}\'""".format(item_global_id, start_time, end_time)
                    query_result_history = self.clientDB.query(query_statement_history)
                    query_result_history = list(query_result_history.get_points())

                    query_statement_global =  """SELECT MAX(\"item_state\") AS max_state, MIN(\"item_state\") AS min_state, MEAN(\"item_state\") AS average_state FROM \"{}\" where time >= \'{}\' AND time <= \'{}\'""".format(item_global_id, start_time, end_time)
                    query_result_global = self.clientDB.query(query_statement_global)
                    query_result_global = list(query_result_global.get_points())
                    print(query_result_global[0]['max_state'], query_result_global[0]['min_state'], query_result_global[0]['average_state'])
                    if len(query_result_history) > 0 and len(query_result_global) > 0:
                        item = {
                            'item_global_id': item_global_id,
                            'thing_global_id': thing_global_id,
                            'max_global': query_result_global[0]['max_state'],
                            'min_global': query_result_global[0]['min_state'],
                            'average_global': query_result_global[0]['average_state'],
                            'history': []
                        }

                        for item_history in query_result_history:
                            item_state = {
                                'last_changed': item_history['time'],
                                'item_state': item_history['item_state'],
                            }
                            item['history'].append(item_state)
                        items.append(item)
                else:

                    query_statement_history = """SELECT *  FROM \"{}\" where time >= \'{}\' AND time <= \'{}\'""".format(item_global_id, start_time, end_time)
                    query_result_history = self.clientDB.query(query_statement_history)
                    query_result_history = list(query_result_history.get_points())

                    if len(query_result_history) > 0:
                        item = {
                            'item_global_id': item_global_id,
                            'thing_global_id': thing_global_id,
                            'history': []
                        }

                        for item_history in query_result_history:
                            item_state = {
                                'last_changed': item_history['time'],
                                'item_state': item_history['item_state'],
                            }
                            item['history'].append(item_state)
                        items.append(item)
            else:
                if type_field == 'integer' or type_field == 'float':
                    query_statement_history = """SELECT MODE(\"item_state\") AS item_state, MAX(\"item_state\") AS max_state, MIN(\"item_state\") AS min_state, MEAN(\"item_state\") AS average_state FROM \"{}\" WHERE time >= \'{}\' AND time <= \'{}\' GROUP BY time({}), thing_global_id""".format(item_global_id, start_time, end_time, scale)
                    query_result_history = self.clientDB.query(query_statement_history)
                    query_result_history = list(query_result_history.get_points())

                    query_statement_global =  """SELECT MAX(\"item_state\") AS max_state, MIN(\"item_state\") AS min_state, MEAN(\"item_state\") AS average_state FROM \"{}\" where time >= \'{}\' AND time <= \'{}\'""".format(item_global_id, start_time, end_time)
                    query_result_global = self.clientDB.query(query_statement_global)
                    query_result_global = list(query_result_global.get_points())

                    if len(query_result_history) > 0 and len(query_result_global) > 0:
                        item = {
                            'item_global_id': item_global_id,
                            'thing_global_id': thing_global_id,
                            'max_global': query_result_global[0]['max_state'],
                            'min_global': query_result_global[0]['min_state'],
                            'average_global': query_result_global[0]['average_state'],
                            'history': []
                        }
                        for item_history in query_result_history:
                            item_state = {
                                'last_changed': item_history['time'],
                                'item_state': item_history['item_state'],
                                'min_state': item_history['min_state'],
                                'max_state': item_history['max_state'],
                                'average_state': item_history['average_state']
                            }
                            item['history'].append(item_state)
                        items.append(item)
                else:
                    query_statement_history = """SELECT MODE(\"item_state\") AS item_state FROM \"{}\" WHERE time >= \'{}\' AND time <= \'{}\' GROUP BY time({}), thing_global_id""".format(item_global_id, start_time, end_time, scale)
                    query_result_history = self.clientDB.query(query_statement_history)
                    query_result_history = list(query_result_history.get_points())

                    if len(query_result_history) > 0:
                        item = {
                            'item_global_id': item_global_id,
                            'thing_global_id': thing_global_id,
                            'history': []
                        }
                        for item_history in query_result_history:
                            item_state = {
                                'last_changed': item_history['time'],
                                'item_state': item_history['item_state']
                            }
                            item['history'].append(item_state)
                        items.append(item)

        return items

    def run(self):

        queue_get_item_state = Queue(name='dbreader.request.api_get_item_state', exchange=self.exchange,
                                     routing_key='dbreader.request.api_get_item_state', message_ttl=20)
        queue_get_item_state_history = Queue(name='dbreader.request.api_get_item_state_history', exchange=self.exchange,
                                             routing_key='dbreader.request.api_get_item_state_history', message_ttl=20)
        while 1:
            try:
                self.consumer_connection.ensure_connection(max_retries=1)
                with nested(Consumer(self.consumer_connection, queues=queue_get_item_state, callbacks=[self.api_get_item_state],
                                     no_ack=True),
                            Consumer(self.consumer_connection, queues=queue_get_item_state_history,
                                     callbacks=[self.api_get_item_state_history], no_ack=True)):
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

    db_reader = Dbreader(BROKER_CLOUD, HOST_INFLUXDB)
    db_reader.run()