import json
import uuid
import time
import threading
from mysql.connector.pooling import MySQLConnectionPool
from kombu import Producer, Connection, Consumer, exceptions, Exchange, Queue
from kombu.utils.compat import nested
import sys


class Registry():
    def __init__(self, broker_cloud, mode, db_config, time_inactive_platform, time_update_conf, time_check_platform_active):
        self.time_update_conf = time_update_conf
        self.time_check_platform_active = time_check_platform_active
        self.time_inactive_platform = time_inactive_platform
        self.cnxpool = MySQLConnectionPool(pool_name="mypool", pool_size=32, **db_config)
        self.mode = mode

        self.producer_connection = Connection(broker_cloud)
        self.consumer_connection = Connection(broker_cloud)

        self.exchange = Exchange("IoT", type="direct")

    def update_config_changes_by_platform_id(self, platform_id):

        message = {
            'reply_to': 'driver.response.registry.api_check_configuration_changes',
            'platform_id': platform_id
        }

        # send request to Driver
        queue = Queue(name='driver.request.api_check_configuration_changes', exchange=self.exchange,
                      routing_key='driver.request.api_check_configuration_changes')
        routing_key = 'driver.request.api_check_configuration_changes'
        self.producer_connection.ensure_connection()
        with Producer(self.producer_connection) as producer:
            producer.publish(
                json.dumps(message),
                exchange=self.exchange.name,
                routing_key=routing_key,
                declare=[queue],
                retry=True
            )

    def check_platform_active(self):
        # print("Check active platform")
        list_platforms = self.get_list_platforms("active")
        for platform in list_platforms:
            if (time.time() - platform['last_response']) > self.time_inactive_platform:
                # print("Mark inactive platform: {}".format(platform['platform_id']))
                self.mark_inactive(str(platform['platform_id']))
                self.send_notification_to_collector()

        threading.Timer(self.time_check_platform_active, self.check_platform_active).start()

    def update_changes_to_db(self, new_info, platform_id):
        # print("Update change of {} to database".format(platform_id))
        now_info = self.get_things_by_platform_id(platform_id, "all", "all")
        inactive_things = now_info[:]
        new_things = new_info[:]
        cnx_1 = self.get_connection_to_db()
        cursor_1 = cnx_1.cursor()
        for now_thing in now_info:
            for new_thing in new_info:
                if now_thing["thing_global_id"] == new_thing["thing_global_id"]:

                    if (now_thing['thing_name'] != new_thing['thing_name'] \
                                or now_thing['thing_type'] != new_thing['thing_type'] \
                                or now_thing['location'] != new_thing['location']):
                        cursor_1.execute(
                            """UPDATE Thing SET thing_name=%s, thing_type=%s, location=%s, thing_status=%s  WHERE thing_global_id=%s""",
                            (new_thing["thing_name"], new_thing["thing_type"], new_thing["location"], 'active',
                             now_thing["thing_global_id"]))
                    if now_thing['thing_status'] == 'inactive':
                        cursor_1.execute("""UPDATE Thing SET thing_status=%s  WHERE thing_global_id=%s""",
                                         ('active', now_thing["thing_global_id"]))

                    inactive_items = now_thing["items"][:]
                    new_items = new_thing['items'][:]

                    for now_item in now_thing["items"]:
                        for new_item in new_thing["items"]:
                            if now_item["item_global_id"] == new_item["item_global_id"]:
                                if (now_item["item_name"] != new_item["item_name"] or
                                            now_item["item_type"] != new_item["item_type"] or
                                            now_item['can_set_state'] != new_item['can_set_state']):

                                    cursor_1.execute(
                                        """UPDATE Item SET item_name=%s, item_type=%s, can_set_state=%s  WHERE item_global_id=%s""",
                                        (new_item["item_name"], new_item["item_type"], new_item["can_set_state"],
                                         now_item['item_global_id']))
                                if now_item['item_status'] == 'inactive':
                                    cursor_1.execute("""UPDATE Item SET item_status=%s  WHERE item_global_id=%s""",
                                                     ('active', now_item['item_global_id']))

                                inactive_items.remove(now_item)
                                new_items.remove(new_item)
                                break

                    if len(inactive_items) != 0:
                        # Item inactive
                        # print("Item inactive")
                        for item_inactive in inactive_items:
                            cursor_1.execute("""UPDATE Item SET item_status=%s  WHERE item_global_id=%s""",
                                             ("inactive", item_inactive['item_global_id']))
                    if len(new_items) != 0:
                        # print("New Item ")
                        for item in new_items:
                            cursor_1.execute("""INSERT INTO Item VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                                             (item['item_global_id'], new_thing['thing_global_id'], item['item_name'],
                                              item['item_type'], item['item_local_id'], item['can_set_state'],
                                              "active"))
                    inactive_things.remove(now_thing)
                    new_things.remove(new_thing)
                    break
        if len(inactive_things) != 0:
            # Thing inactive
            # print("Thing inactive")
            for thing_inactive in inactive_things:
                cursor_1.execute("""UPDATE Thing SET thing_status=%s  WHERE thing_global_id=%s""",
                                 ("inactive", thing_inactive['thing_global_id']))
                for item_inactive in thing_inactive['items']:
                    cursor_1.execute("""UPDATE Item SET item_status=%s  WHERE item_global_id=%s""",
                                     ("inactive", item_inactive['item_global_id']))

        if len(new_things) != 0:
            # New things

            # print("New Thing")
            for thing in new_things:
                cursor_1.execute("""INSERT INTO Thing VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                                 (thing['thing_global_id'], platform_id, thing['thing_name'],
                                  thing['thing_type'], thing['thing_local_id'], thing['location'], "active"))
                # print('Updated Things')
                for item in thing['items']:
                    # print("{}".format(item['item_global_id']))
                    cursor_1.execute("""INSERT INTO Item VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                                     (item['item_global_id'], thing['thing_global_id'], item['item_name'],
                                      item['item_type'], item['item_local_id'], item['can_set_state'], "active"))
                    # print('Updated Items')
        cnx_1.commit()
        cursor_1.close()
        cnx_1.close()

    def get_things_by_platform_id(self, platform_id, thing_status, item_status):
        # print("Get things in platform_id: {}".format(platform_id))
        things_in_system = self.get_things(thing_status, item_status)
        things_in_platform = []
        for thing in things_in_system:
            if thing['platform_id'] == platform_id:
                things_in_platform.append(thing)
        return things_in_platform

    def handle_configuration_changes(self, body, message):
        cnx_2 = self.get_connection_to_db()
        cursor_2 = cnx_2.cursor()
        body = json.loads(body)
        platform_id = body['platform_id']

        if body['have_change'] == False:
            # print('Platform have Id: {} no changes'.format(platform_id))
            cursor_2.execute("""SELECT platform_status FROM Platform WHERE platform_id=%s""",
                             (str(platform_id),))
            platform_status = cursor_2.fetchone()[0]
            if platform_status == 'active':
                pass
            else:
                new_info = body['new_info']
                self.update_changes_to_db(new_info, platform_id)
                self.send_notification_to_collector()

        else:
            print('Platform have Id: {} changed the configuration file'.format(platform_id))
            new_info = body['new_info']
            self.update_changes_to_db(new_info, platform_id)

        #Update last_response and status of platform
        cursor_2.execute("""UPDATE Platform SET last_response=%s, platform_status=%s WHERE platform_id=%s""",
                         (time.time(), 'active', platform_id))

        cnx_2.commit()
        cursor_2.close()
        cnx_2.close()

    def mark_inactive(self, platform_id):
        print('Mark Thing and Item inactive')
        cnx_1 = self.get_connection_to_db()
        cursor_1 = cnx_1.cursor()
        cursor_1.execute("""SELECT thing_global_id FROM Thing WHERE platform_id = %s""", (str(platform_id),))
        list_thing_global_id = cursor_1.fetchall()
        # print('List_thing {}'.format(list_thing_global_id))
        for thing_global_id in list_thing_global_id:
            # print(thing_global_id[0])
            # thing_global_id[0] để lấy ra kết quả. Còn thing_global_id vẫn là list.
            #  VD: ('d32d30b4-8917-4eb1-a273-17f7f440b240/sensor.humidity',)
            cursor_1.execute("""UPDATE Item SET item_status=%s  WHERE thing_global_id=%s""",
                             ("inactive", str(thing_global_id[0])))

        cnx_1.commit()
        cursor_1.execute("""UPDATE Thing SET thing_status=%s  WHERE platform_id=%s""", ("inactive", str(platform_id)))
        cursor_1.execute("""UPDATE Platform SET platform_status=%s  WHERE platform_id=%s""",
                         ("inactive", str(platform_id)))
        cnx_1.commit()
        cursor_1.close()
        cnx_1.close()

    def update_all_config_changes(self):
        # print('Run Update All Configuration Changes')
        list_platforms = self.get_list_platforms("active")

        for platform in list_platforms:
            self.update_config_changes_by_platform_id(platform['platform_id'])

        threading.Timer(self.time_update_conf, self.update_all_config_changes).start()

    def get_list_platforms(self, platform_status):
        # print('Get list platforms')
        list_platforms = []
        cnx_1 = self.get_connection_to_db()
        cursor_1 = cnx_1.cursor()

        if platform_status == "active":
            cursor_1.execute("""SELECT platform_id, platform_name, host, port, last_response, platform_status
                                FROM Platform WHERE platform_status='active'""")
        elif platform_status == "inactive":
            cursor_1.execute("""SELECT platform_id, platform_name, host, port, last_response, platform_status
                                FROM Platform WHERE platform_status='inactive'""")
        elif platform_status == "all":
            cursor_1.execute("""SELECT platform_id, platform_name, host, port, last_response, platform_status
                                FROM Platform""")
        else:
            return list_platforms

        rows = cursor_1.fetchall()
        for row in rows:
            list_platforms.append({
                "platform_id": row[0],
                "platform_name": row[1],
                "host": row[2],
                "port": row[3],
                "last_response": row[4],
                "platform_status": row[5]
            })
        # print(list_platforms)
        cursor_1.close()
        cnx_1.close()
        return list_platforms

    def api_get_list_platforms(self, body, message):
        print("API get list platform with platform_status")
        platform_status = json.loads(body)['platform_status']
        reply_to = json.loads(body)['reply_to']
        message_response = {
            "list_platforms": self.get_list_platforms(platform_status)
        }
        self.producer_connection.ensure_connection()
        with Producer(self.producer_connection) as producer:
            producer.publish(
                json.dumps(message_response),
                exchange=self.exchange.name,
                routing_key=reply_to,
                retry=True
            )

    def api_add_platform(self, body, message):
        body = json.loads(body)
        host = body['host']
        port = body['port']
        platform_name = body['platform_name']
        cnx_1 = self.get_connection_to_db()
        cursor_1 = cnx_1.cursor()

        if "platform_id" in body:
            platform_id = body['platform_id']
            print("Platform {} have id: {} come back to system".format(platform_name, platform_id))
            cursor_1.execute("""UPDATE Platform SET platform_status=%s, last_response=%s  WHERE platform_id=%s""",
                             ('active', time.time(), platform_id))
        else:
            platform_id = str(uuid.uuid4())
            print('Add {} have address {}:{} to system '.format(platform_name, host, port))
            print('Generate id for this platform : ', platform_id)
            cursor_1.execute("""INSERT INTO Platform VALUES (%s,%s,%s,%s,%s,%s)""",
                             (platform_id, platform_name, host, port, time.time(), "active"))

        message_response = {
            'platform_id': platform_id,
            'host': host,
            'port': port,
            'platform_name': platform_name
        }

        # check connection and publish message
        queue_response = Queue(name='registry.response.driver.api_add_platform', exchange=self.exchange,
                               routing_key='registry.response.driver.api_add_platform')
        routing_key = 'registry.response.driver.api_add_platform'
        self.producer_connection.ensure_connection()
        with Producer(self.producer_connection) as producer:
            producer.publish(
                json.dumps(message_response),
                exchange=self.exchange.name,
                routing_key=routing_key,
                declare=[queue_response],
                retry=True
            )

        cnx_1.commit()
        cursor_1.close()
        cnx_1.close()
        self.send_notification_to_collector()

    def get_things(self, thing_status, item_status):
        cnx_1 = self.get_connection_to_db()
        cursor_1 = cnx_1.cursor()
        query_thing = ""
        query_item = ""
        if thing_status == 'active':
            query_thing = """SELECT Thing.platform_id, Thing.thing_global_id, Thing.thing_name,
                                    Thing.thing_type, Thing.location, Thing.thing_local_id, Thing.thing_status
                              FROM  Thing
                              WHERE Thing.thing_status = 'active'; """
        elif thing_status == 'inactive':
            query_thing = """SELECT Thing.platform_id, Thing.thing_global_id, Thing.thing_name,
                                    Thing.thing_type, Thing.location, Thing.thing_local_id, Thing.thing_status
                              FROM  Thing
                              WHERE Thing.thing_status = 'inactive'; """
        elif thing_status == 'all':
            query_thing = """SELECT Thing.platform_id, Thing.thing_global_id, Thing.thing_name,
                                    Thing.thing_type, Thing.location, Thing.thing_local_id, Thing.thing_status
                              FROM  Thing;"""

        if item_status == 'active':
            query_item = """SELECT Item.thing_global_id, Item.item_global_id, Item.item_name,
                                   Item.item_type, Item.can_set_state, Item.item_local_id, Item.item_status
                              FROM Item 
                              WHERE Item.item_status='active';"""
        elif item_status == 'inactive':
            query_item = """SELECT Item.thing_global_id, Item.item_global_id, Item.item_name,
                                   Item.item_type, Item.can_set_state, Item.item_local_id, Item.item_status
                              FROM Item 
                              WHERE Item.item_status='inactive';"""
        elif item_status == 'all':
            query_item = """SELECT Item.thing_global_id, Item.item_global_id, Item.item_name,
                                   Item.item_type, Item.can_set_state, Item.item_local_id, Item.item_status
                              FROM Item;"""

        cursor_1.execute(query_thing)
        thing_rows = cursor_1.fetchall()

        cursor_1.execute(query_item)
        item_rows = cursor_1.fetchall()
        cursor_1.close()
        cnx_1.close()
        things = []
        for thing in thing_rows:
            temp_thing = {
                'platform_id': thing[0],
                'thing_global_id': thing[1],
                'thing_name': thing[2],
                'thing_type': thing[3],
                'location': thing[4],
                'thing_local_id': thing[5],
                'thing_status': thing[6],
                'items': []
            }

            for item in item_rows:
                if item[0] == thing[1]:
                    temp_item = {
                        'item_global_id': item[1],
                        'item_name': item[2],
                        'item_type': item[3],
                        'can_set_state': item[4],
                        'item_local_id': item[5],
                        'item_status': item[6]
                    }
                    temp_thing['items'].append(temp_item)
            things.append(temp_thing)

        return things

    def get_thing_by_global_id(self, thing_global_id):
        cnx_1 = self.get_connection_to_db()
        cursor_1 = cnx_1.cursor()

        cursor_1.execute("""SELECT Thing.platform_id, Thing.thing_global_id, Thing.thing_name,
                                Thing.thing_type, Thing.location, Thing.thing_local_id, Thing.thing_status
                          FROM  Thing
                          WHERE Thing.thing_global_id=%s; """, (thing_global_id,))
        thing_rows = cursor_1.fetchall()

        cursor_1.execute("""SELECT Item.thing_global_id, Item.item_global_id, Item.item_name,
                                   Item.item_type, Item.can_set_state, Item.item_local_id, Item.item_status
                              FROM Item 
                              WHERE Item.thing_global_id=%s;""", (thing_global_id,))

        item_rows = cursor_1.fetchall()
        cursor_1.close()
        cnx_1.close()
        things = []
        for thing in thing_rows:
            temp_thing = {
                'platform_id': thing[0],
                'thing_global_id': thing[1],
                'thing_name': thing[2],
                'thing_type': thing[3],
                'location': thing[4],
                'thing_local_id': thing[5],
                'thing_status': thing[6],
                'items': []
            }

            for item in item_rows:
                if item[0] == thing[1]:
                    temp_item = {
                        'item_global_id': item[1],
                        'item_name': item[2],
                        'item_type': item[3],
                        'can_set_state': item[4],
                        'item_local_id': item[5],
                        'item_status': item[6]
                    }
                    temp_thing['items'].append(temp_item)
            things.append(temp_thing)

        return things

    def api_get_things(self, body, message):
        print('API Get All Things')
        reply_to = json.loads(body)['reply_to']
        thing_status = json.loads(body)['thing_status']
        item_status = json.loads(body)['item_status']
        things = self.get_things(thing_status, item_status)
        message_response = {
            'things': things
        }
        self.producer_connection.ensure_connection()
        with Producer(self.producer_connection) as producer:
            producer.publish(
                json.dumps(message_response),
                exchange=self.exchange.name,
                routing_key=reply_to,
                retry=True
            )

    def api_get_thing_by_global_id(self, body, message):
        print('API Get Thing by thing_global_id')
        reply_to = json.loads(body)['reply_to']
        thing_global_id = json.loads(body)['thing_global_id']

        things = self.get_thing_by_global_id(thing_global_id)

        message_response = {
            'things': things
        }
        self.producer_connection.ensure_connection()
        with Producer(self.producer_connection) as producer:
            producer.publish(
                json.dumps(message_response),
                exchange=self.exchange.name,
                routing_key=reply_to,
                retry=True
            )

    def api_get_things_by_platform_id(self, body, message):
        print('Get Thing by platform_id')
        reply_to = json.loads(body)['reply_to']
        platform_id = json.loads(body)['platform_id']
        thing_status = json.loads(body)['thing_status']
        item_status = json.loads(body)['item_status']
        things = self.get_things_by_platform_id(platform_id, thing_status, item_status)

        message_response = {
            'things': things
        }
        self.producer_connection.ensure_connection()
        with Producer(self.producer_connection) as producer:
            producer.publish(
                json.dumps(message_response),
                exchange=self.exchange.name,
                routing_key=reply_to,
                retry=True
            )

    def get_connection_to_db(self):
        while True:
            try:
                # print("Get connection DB")
                connection = self.cnxpool.get_connection()
                return connection
            except:
                # print("Can't get connection DB")
                pass

    def send_notification_to_collector(self):
        print('Send notification to Collector')
        message = {
            'notification': 'Have Platform_id change'
        }

        queue = Queue(name='collector.request.notification', exchange=self.exchange,
                      routing_key='collector.request.notification')
        routing_key = 'collector.request.notification'
        self.producer_connection.ensure_connection()
        with Producer(self.producer_connection) as producer:
            producer.publish(
                json.dumps(message),
                exchange=self.exchange.name,
                routing_key=routing_key,
                declare=[queue],
                retry=True
            )

    def run(self):
        queue_get_things = Queue(name='registry.request.api_get_things', exchange=self.exchange,
                                 routing_key='registry.request.api_get_things')
        queue_get_list_platforms = Queue(name='registry.request.api_get_list_platforms', exchange=self.exchange,
                                         routing_key='registry.request.api_get_list_platforms')
        queue_add_platform = Queue(name='registry.request.api_add_platform', exchange=self.exchange,
                                   routing_key='registry.request.api_add_platform')
        queue_check_config = Queue(name='driver.response.registry.api_check_configuration_changes', exchange=self.exchange,
                                   routing_key='driver.response.registry.api_check_configuration_changes')
        queue_get_thing_by_global_id = Queue(name='registry.request.api_get_thing_by_global_id', exchange=self.exchange,
                                             routing_key='registry.request.api_get_thing_by_global_id')
        queue_get_things_by_platform_id = Queue(name='registry.request.api_get_things_by_platform_id',
                                                exchange=self.exchange,
                                                routing_key='registry.request.api_get_things_by_platform_id')

        if self.mode == 'PULL':
            self.update_all_config_changes()

        self.check_platform_active()

        while 1:
            try:
                self.consumer_connection.ensure_connection(max_retries=1)
                with nested(Consumer(self.consumer_connection, queues=queue_get_things_by_platform_id,
                                     callbacks=[self.api_get_things_by_platform_id], no_ack=True),
                            Consumer(self.consumer_connection, queues=queue_get_thing_by_global_id,
                                     callbacks=[self.api_get_thing_by_global_id], no_ack=True),
                            Consumer(self.consumer_connection, queues=queue_add_platform, callbacks=[self.api_add_platform],
                                     no_ack=True),
                            Consumer(self.consumer_connection, queues=queue_get_things, callbacks=[self.api_get_things],
                                     no_ack=True),
                            Consumer(self.consumer_connection, queues=queue_get_list_platforms,
                                     callbacks=[self.api_get_list_platforms], no_ack=True),
                            Consumer(self.consumer_connection, queues=queue_check_config,
                                     callbacks=[self.handle_configuration_changes], no_ack=True)):
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
        BROKER_CLOUD = 'localhost'  # rabbitmq
        MODE = 'PULL'  # or PUSH or PULL

        dbconfig = {
            "database": "Registry_DB",
            "user": "root",
            "host": '0.0.0.0',
            "passwd": "root",
            "autocommit": "True"
        }

        TIME_INACTIVE_PLATFORM = 60     # Time when platform is marked inactive
        TIME_UPDATE_CONF = 2            # Time when registry send request update conf to Driver
        TIME_CHECK_PLATFORM_ACTIVE = 2  # Time when check active_platform in system
    else:
        BROKER_CLOUD = sys.argv[1]  #rabbitmq
        MODE = sys.argv[2] # or PUSH or PULL

        dbconfig = {
          "database": "Registry_DB",
          "user":     "root",
          "host":     sys.argv[3],
          "passwd":   "root",
          "autocommit": "True"
        }

        TIME_INACTIVE_PLATFORM = sys.argv[4]
        TIME_UPDATE_CONF = sys.argv[5]
        TIME_CHECK_PLATFORM_ACTIVE = sys.argv[6]

    registry = Registry(BROKER_CLOUD, MODE, dbconfig, TIME_INACTIVE_PLATFORM, TIME_UPDATE_CONF, TIME_CHECK_PLATFORM_ACTIVE)
    registry.run()