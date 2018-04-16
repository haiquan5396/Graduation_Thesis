import json
import uuid
#import MySQLdb
import ast
import time
import threading
from mysql.connector.pooling import MySQLConnectionPool
from kombu import Producer, Connection, Consumer, exceptions, Exchange, Queue
from kombu.utils.compat import nested

BROKER_CLOUD = "localhost"
TIME_DELETE_PLATFORM = 20

dbconfig = {
  "database": "Registry_DB",
  "user":     "root",
  "host":     "192.168.0.110",
  "passwd":   "root",
  "autocommit": "True"
}

cnxpool = MySQLConnectionPool(pool_name = "mypool", pool_size = 32, **dbconfig)

producer_connection = Connection(BROKER_CLOUD)
consumer_connection = Connection(BROKER_CLOUD)

exchange = Exchange("IoT", type="direct")


def get_connection_to_db():
    while True:
        try:
            print("Get connection DB")
            connection = cnxpool.get_connection()
            return connection
        except:
            print("Can't get connection DB")
            pass


def update_config_changes_by_platform_id(platform_id):
    cnx_1 = get_connection_to_db()
    cursor_1 = cnx_1.cursor()
    cursor_1.execute("""SELECT last_response FROM Platform WHERE platform_id = %s""", (platform_id,))
    last_response = cursor_1.fetchone()
    print("Check Time")
    print(time.time() - last_response[0])
    if (time.time() - last_response[0]) < TIME_DELETE_PLATFORM:
        # Check last response
        message = {
            'reply_to': 'driver.response.registry.api_check_configuration_changes',
            'platform_id': platform_id
        }

        queue = Queue(name='driver.request.api_check_configuration_changes', exchange=exchange, routing_key='driver.request.api_check_configuration_changes')
        routing_key = 'driver.request.api_check_configuration_changes'
        producer_connection.ensure_connection()
        with Producer(producer_connection) as producer:
            producer.publish(
                json.dumps(message),
                exchange=exchange.name,
                routing_key=routing_key,
                declare=[queue],
                retry=True
            )

    else:
        # after TIME_DELETE_PLATFORM without response =>> delete platform
        print("Xoa do het thoi gian {}".format(platform_id))
        delete_old_thing_and_item(str(platform_id))
        cursor_1.execute("""DELETE FROM Platform WHERE platform_id = %s""", (platform_id,))
        print('Delete Platform have Id: ', str(platform_id))
        send_notification_to_collector()

    cnx_1.commit()
    cursor_1.close()
    cnx_1.close()


def handle_configuration_changes(body, message):
    cnx_2 = get_connection_to_db()
    cursor_2 = cnx_2.cursor()
    body = json.loads(body)
    print(body)
    print(body['reply_to'])
    platform_id = body['platform_id']
    cursor_2.execute("""UPDATE Platform SET last_response= %s WHERE platform_id=%s""", (time.time(), platform_id))
    cnx_2.commit()
    if body['have_change'] == False:
        print('Platform have Id: {} no changes'.format(platform_id))

    else:
        print('Platform have Id: {} changed the configuration file'.format(platform_id))
        now_info = body['now_info']
        delete_old_thing_and_item(str(platform_id))
        for thing in now_info:
            cursor_2.execute("""INSERT INTO Thing VALUES (%s,%s,%s,%s,%s,%s)""", (
            thing['thing_global_id'], platform_id, thing['thing_name'], thing['thing_type'], thing['thing_local_id'],
            thing['location']))
            print('Updated Things')
            for item in thing['items']:
                print(item)
                print("{}".format(item['item_global_id']))
                cursor_2.execute("""INSERT INTO Item VALUES (%s,%s,%s,%s,%s,%s)""", (
                item['item_global_id'], thing['thing_global_id'], item['item_name'], item['item_type'],
                item['item_local_id'], item['can_set_state']))
                print('Updated Items')
    message.ack()
    cnx_2.commit()
    cursor_2.close()
    cnx_2.close()


def delete_old_thing_and_item(platform_id):

    print ('Delete Old Thing and Item')
    cnx_1 = get_connection_to_db()
    cursor_1 = cnx_1.cursor()
    cursor_1.execute("""SELECT thing_global_id FROM Thing WHERE platform_id = %s""", (str(platform_id),))
    list_thing_global_id = cursor_1.fetchall()
    # print('List_thing {}'.format(list_thing_global_id))
    for thing_global_id in list_thing_global_id:
        # print(thing_global_id[0])
        #thing_global_id[0] để lấy ra kết quả. Còn thing_global_id vẫn là list. VD: ('d32d30b4-8917-4eb1-a273-17f7f440b240/sensor.humidity',)
        cursor_1.execute("""DELETE FROM Item WHERE thing_global_id = %s""", (str(thing_global_id[0]),))

    cnx_1.commit()
    cursor_1.execute("""DELETE FROM Thing WHERE platform_id = %s""", (str(platform_id),))
    cnx_1.commit()
    cursor_1.close()
    cnx_1.close()


def update_all_config_changes():
    print('Run Update All Configuration Changes')
    cnx_1 = get_connection_to_db()
    cursor_1 = cnx_1.cursor()
    cursor_1.execute("""SELECT platform_id FROM Platform""")
    rows = cursor_1.fetchall()
    cursor_1.close()
    cnx_1.close()
    print (rows)
    for row in rows:
        update_config_changes_by_platform_id(row[0])

    threading.Timer(2, update_all_config_changes).start()


def send_notification_to_collector():
    print('Send notification to Collector')
    message = {
        'notification': 'Have Platform_id change'
    }

    queue = Queue(name='collector.request.notification', exchange=exchange,
                                     routing_key='collector.request.notification')
    routing_key = 'collector.request.notification'
    producer_connection.ensure_connection()
    with Producer(producer_connection) as producer:
        producer.publish(
            json.dumps(message),
            exchange=exchange.name,
            routing_key=routing_key,
            declare=[queue],
            retry=True
        )

def api_get_list_platforms(body, message):
    print('Get list platforms')
    cnx_1 = get_connection_to_db()
    cursor_1 = cnx_1.cursor()
    cursor_1.execute("""SELECT platform_id FROM Platform""")
    rows = cursor_1.fetchall()
    list_platforms = []
    for row in rows:
        list_platforms.append(row[0])
    print(list_platforms)
    cursor_1.close()
    cnx_1.close()
    reply_to = json.loads(body)['reply_to']
    producer_connection.ensure_connection()
    with Producer(producer_connection) as producer:
        producer.publish(
            str(list_platforms),
            exchange=exchange.name,
            routing_key=reply_to,
            retry=True
        )
    message.ack()

def api_add_platform(body, message):
    body = json.loads(body)
    platform_id = str(uuid.uuid4())
    print(body)
    host = body['host']
    port = body['port']
    platform_name = body['platform']

    print ('Add {} have address {}:{} to system '.format(platform_name, host, port))
    print ('Generate id for this platform : ', platform_id)

    message_response = {
        'platform_id': platform_id,
        'host': host,
        'port': port,
        'platform': platform_name
    }

    # check connection and publish message
    queue_response = Queue(name='registry.response.driver.api_add_platform', exchange=exchange, routing_key='registry.response.driver.api_add_platform')
    routing_key = 'registry.response.driver.api_add_platform'
    producer_connection.ensure_connection()
    with Producer(producer_connection) as producer:
        producer.publish(
            json.dumps(message_response),
            exchange=exchange.name,
            routing_key=routing_key,
            declare=[queue_response],
            retry=True
        )
        print("Publish")
    message.ack()
    # writer database
    cnx_1 = get_connection_to_db()
    cursor_1 = cnx_1.cursor()
    cursor_1.execute("""USE Registry_DB""")
    cursor_1.execute("""INSERT INTO Platform VALUES (%s,%s,%s,%s,%s)""", (platform_id, platform_name, host, port, time.time()))
    cnx_1.commit()
    cursor_1.close()
    cnx_1.close()
    send_notification_to_collector()


def api_get_things(body, message):
    print('Get All Things')
    caller = message.properties['reply_to']
    cnx_1 = get_connection_to_db()
    cursor_1 = cnx_1.cursor()
    cursor_1.execute("""SELECT Thing.platform_id, Thing.thing_global_id, Thing.thing_name,
                            Thing.thing_type, Thing.location, Thing.thing_local_id
                      FROM  Thing; """)
    thing_rows = cursor_1.fetchall()

    cursor_1.execute("""SELECT Item.thing_global_id, Item.item_global_id, 
                             Item.item_name, Item.item_type, Item.can_set_state, Item.item_local_id
                      FROM Item ;""")
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
            'thing_location': thing[4],
            'thing_local_id': thing[5],
            'items': []
        }

        for item in item_rows:
            if item[0] == thing[1]:
                temp_item = {
                    'item_global_id': item[1],
                    'item_name': item[2],
                    'item_type': item[3],
                    'can_set_state': item[4],
                    'item_local_id': item[5]
                }
                temp_thing['items'].append(temp_item)
        things.append(temp_thing)

    producer_connection.ensure_connection()
    with Producer(producer_connection) as producer:
        producer.publish(
            str(things),
            exchange=exchange.name,
            routing_key=message.properties['reply_to'],
            retry=True
        )
    message.ack()



queue_get_things = Queue(name='registry.request.api_get_things', exchange=exchange, routing_key='registry.request.api_get_things')
queue_get_list_platforms = Queue(name='registry.request.api_get_list_platforms', exchange=exchange, routing_key='registry.request.api_get_list_platforms')
queue_add_platform = Queue(name='registry.request.api_add_platform', exchange=exchange, routing_key='registry.request.api_add_platform')
queue_check_config = Queue(name='driver.response.registry.api_check_configuration_changes', exchange=exchange, routing_key='driver.response.registry.api_check_configuration_changes')

update_all_config_changes()

while 1:
    try:
        consumer_connection.ensure_connection(max_retries=1)
        with nested (Consumer(consumer_connection, queues=queue_add_platform, callbacks=[api_add_platform]),
                     Consumer(consumer_connection, queues=queue_get_things, callbacks=[api_get_things]),
                     Consumer(consumer_connection, queues=queue_get_list_platforms, callbacks=[api_get_list_platforms]),
                     Consumer(consumer_connection, queues=queue_check_config, callbacks=[handle_configuration_changes])):
            while True:
                consumer_connection.drain_events()
    except (ConnectionRefusedError, exceptions.OperationalError):
        print('Connection lost')
    except consumer_connection.connection_errors:
        print('Connection error')
