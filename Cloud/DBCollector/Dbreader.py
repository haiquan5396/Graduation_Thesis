import json
from kombu import Producer, Connection, Consumer, exceptions, Exchange, Queue
from kombu.utils.compat import nested
from influxdb import InfluxDBClient
import sys


class Dbreader():
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
        list_item_global_id = json.loads(body)["list_item_global_id"]
        reply_to = json.loads(body)['reply_to']
        start_time = json.loads(body)["start_time"]
        end_time = json.loads(body)["end_time"]
        items = self.get_item_state_history(list_item_global_id, start_time, end_time)
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

    def get_item_state_history(self, list_item_global_id, start_time, end_time):
        items = []
        for item_global_id in list_item_global_id:
            query_statement = """SELECT * FROM \"{}\" where time > \'{}\' AND time < \'{}\'""".format(item_global_id,
                                                                                                      start_time,
                                                                                                      end_time)
            # print(query_statement)
            query_result = self.clientDB.query(query_statement)
            query_result = list(query_result.get_points())
            if len(query_result) > 0:
                item = {
                    'item_global_id': query_result[0]['item_global_id'],
                    'thing_global_id': query_result[0]['thing_global_id'],
                    'history': []
                }

                for item_history in query_result:
                    item_state = {
                        'last_changed': item_history['time'],
                        'item_state': item_history['item_state']
                    }
                    item['history'].append(item_state)
                items.append(item)
        return items

    def run(self):

        queue_get_item_state = Queue(name='dbreader.request.api_get_item_state', exchange=self.exchange,
                                     routing_key='dbreader.request.api_get_item_state')
        queue_get_item_state_history = Queue(name='dbreader.request.api_get_item_state_history', exchange=self.exchange,
                                             routing_key='dbreader.request.api_get_item_state_history')
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