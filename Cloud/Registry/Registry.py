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
TIME_DELETE_PLATFORM = 60
MODE = 'PUSH' # or PUSH or PULL

dbconfig = {
  "database": "Registry_DB",
  "user":     "root",
  "host":     "0.0.0.0",
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

        #send request to Driver
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
        # after TIME_DELETE_PLATFORM without response =>> mark platform is inactive
        print("Mark inactive platform: {}".format(platform_id))
        mark_inactive(str(platform_id))

        send_notification_to_collector()

    cnx_1.commit()
    cursor_1.close()
    cnx_1.close()


def update_changes_to_db(new_info, platform_id):
    print("Update change of {} to database".format(platform_id))
    now_info = get_things_by_platform_id(platform_id, "all", "all")
    inactive_things = now_info[:]
    new_things = new_info[:]
    cnx_1 = get_connection_to_db()
    cursor_1 = cnx_1.cursor()
    for now_thing in now_info:
        for new_thing in new_info:
            if now_thing["thing_global_id"] == new_thing["thing_global_id"]:

                if (now_thing['thing_name'] != new_thing['thing_name']\
                        or now_thing['thing_type'] != new_thing['thing_type']\
                        or now_thing['location'] != new_thing['location']):

                    cursor_1.execute("""UPDATE Thing SET thing_name=%s, thing_type=%s, location=%s, thing_status=%s  WHERE thing_global_id=%s""",
                                     (new_thing["thing_name"], new_thing["thing_type"], new_thing["location"], 'active', now_thing["thing_global_id"]))
                if now_thing['thing_status'] == 'inactive':
                    cursor_1.execute("""UPDATE Thing SET thing_status=%s  WHERE thing_global_id=%s""", ('active', now_thing["thing_global_id"]))

                inactive_items = now_thing["items"][:]
                new_items = new_thing['items'][:]

                for now_item in now_thing["items"]:
                    for new_item in new_thing["items"]:
                        if now_item["item_global_id"] == new_item["item_global_id"]:
                            if (now_item["item_name"] != new_item["item_name"]
                                 or now_item["item_type"] != new_item["item_type"]
                                 or now_item['can_set_state'] != new_item['can_set_state']):

                                cursor_1.execute("""UPDATE Item SET item_name=%s, item_type=%s, can_set_state=%s  WHERE item_global_id=%s""",
                                                 (new_item["item_name"], new_item["item_type"], new_item["can_set_state"], now_item['item_global_id']))
                            if now_item['item_status'] == 'inactive':
                                cursor_1.execute("""UPDATE Item SET item_status=%s  WHERE item_global_id=%s""",('active', now_item['item_global_id']))

                            inactive_items.remove(now_item)
                            new_items.remove(new_item)
                            break

                if len(inactive_items) != 0:
                    # Item inactive
                    print("Item inactive")
                    for item_inactive in inactive_items:
                        cursor_1.execute("""UPDATE Item SET item_status=%s  WHERE item_global_id=%s""",
                                         ("inactive", item_inactive['item_global_id']))
                if len(new_items) != 0:
                    print("New Item ")
                    for item in new_items:
                        cursor_1.execute("""INSERT INTO Item VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                                         (item['item_global_id'], new_thing['thing_global_id'], item['item_name'],
                                          item['item_type'], item['item_local_id'], item['can_set_state'], "active"))
                inactive_things.remove(now_thing)
                new_things.remove(new_thing)
                break
    if len(inactive_things) != 0:
        # Thing inactive
        print("Thing inactive")
        for thing_inactive in inactive_things:
            cursor_1.execute("""UPDATE Thing SET thing_status=%s  WHERE thing_global_id=%s""",
                             ("inactive", thing_inactive['thing_global_id']))
            for item_inactive in thing_inactive['items']:
                cursor_1.execute("""UPDATE Item SET item_status=%s  WHERE item_global_id=%s""",
                                 ("inactive", item_inactive['item_global_id']))

    if len(new_things) != 0:
        # New things

        print("New Thing")
        for thing in new_things:
            cursor_1.execute("""INSERT INTO Thing VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                             (thing['thing_global_id'], platform_id, thing['thing_name'],
                              thing['thing_type'], thing['thing_local_id'], thing['location'], "active"))
            print('Updated Things')
            for item in thing['items']:
                print(item)
                print("{}".format(item['item_global_id']))
                cursor_1.execute("""INSERT INTO Item VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                                 (item['item_global_id'], thing['thing_global_id'], item['item_name'],
                                  item['item_type'], item['item_local_id'], item['can_set_state'], "active"))
                print('Updated Items')
    cnx_1.commit()
    cursor_1.close()
    cnx_1.close()


def get_things_by_platform_id(platform_id, thing_status, item_status):
    print("Get things in platform_id: {}".format(platform_id))
    things_in_system = get_things(thing_status, item_status)
    things_in_platform = []
    for thing in things_in_system:
        if thing['platform_id'] == platform_id:
            things_in_platform.append(thing)
    return things_in_platform


def handle_configuration_changes(body, message):
    cnx_2 = get_connection_to_db()
    cursor_2 = cnx_2.cursor()
    body = json.loads(body)
    platform_id = body['platform_id']
    # update last_response and platform_status
    cursor_2.execute("""UPDATE Platform SET last_response=%s, platform_status=%s WHERE platform_id=%s""", (time.time(), 'active', platform_id))
    cnx_2.commit()

    if body['have_change'] == False:
        print('Platform have Id: {} no changes'.format(platform_id))

    else:
        print('Platform have Id: {} changed the configuration file'.format(platform_id))
        new_info = body['new_info']
        update_changes_to_db(new_info, platform_id)

    cursor_2.close()
    cnx_2.close()


def mark_inactive(platform_id):
    print ('Mark Thing and Item inactive')
    cnx_1 = get_connection_to_db()
    cursor_1 = cnx_1.cursor()
    cursor_1.execute("""SELECT thing_global_id FROM Thing WHERE platform_id = %s""", (str(platform_id),))
    list_thing_global_id = cursor_1.fetchall()
    # print('List_thing {}'.format(list_thing_global_id))
    for thing_global_id in list_thing_global_id:
        # print(thing_global_id[0])
        # thing_global_id[0] để lấy ra kết quả. Còn thing_global_id vẫn là list.
        #  VD: ('d32d30b4-8917-4eb1-a273-17f7f440b240/sensor.humidity',)
        cursor_1.execute("""UPDATE Item SET item_status=%s  WHERE thing_global_id=%s""", ("inactive", str(thing_global_id[0])))

    cnx_1.commit()
    cursor_1.execute("""UPDATE Thing SET thing_status=%s  WHERE platform_id=%s""", ("inactive", str(platform_id)))
    cursor_1.execute("""UPDATE Platform SET platform_status=%s  WHERE platform_id=%s""", ("inactive", str(platform_id)))
    cnx_1.commit()
    cursor_1.close()
    cnx_1.close()


def update_all_config_changes():
    print('Run Update All Configuration Changes')
    list_platforms = get_list_platforms("active")

    for platform in list_platforms:
        update_config_changes_by_platform_id(platform['platform_id'])

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


def get_list_platforms(platform_status):
    print('Get list platforms')
    list_platforms = []
    cnx_1 = get_connection_to_db()
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
    print(list_platforms)
    cursor_1.close()
    cnx_1.close()
    return list_platforms


def api_get_list_platforms(body, message):
    print("API get list platform with platform_status")
    platform_status = json.loads(body)['platform_status']
    reply_to = json.loads(body)['reply_to']
    message_response = {
        "list_platforms": get_list_platforms(platform_status)
    }
    producer_connection.ensure_connection()
    with Producer(producer_connection) as producer:
        producer.publish(
            json.dumps(message_response),
            exchange=exchange.name,
            routing_key=reply_to,
            retry=True
        )


def api_add_platform(body, message):
    body = json.loads(body)
    print(body)
    host = body['host']
    port = body['port']
    platform_name = body['platform_name']
    cnx_1 = get_connection_to_db()
    cursor_1 = cnx_1.cursor()

    if "platform_id" in body:
        platform_id = body['platform_id']
        print("Platform {} have id: {} come back to system".format(platform_name, platform_id))
        cursor_1.execute("""UPDATE Platform SET platform_status=%s, last_response=%s  WHERE platform_id=%s""",
                         ('active', time.time(), platform_id))
    else:
        platform_id = str(uuid.uuid4())
        print ('Add {} have address {}:{} to system '.format(platform_name, host, port))
        print ('Generate id for this platform : ', platform_id)
        cursor_1.execute("""INSERT INTO Platform VALUES (%s,%s,%s,%s,%s,%s)""",
                         (platform_id, platform_name, host, port, time.time(), "active"))

    message_response = {
        'platform_id': platform_id,
        'host': host,
        'port': port,
        'platform_name': platform_name
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
    # writer database


    cnx_1.commit()
    cursor_1.close()
    cnx_1.close()
    send_notification_to_collector()


def get_things(thing_status, item_status):
    cnx_1 = get_connection_to_db()
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


def get_thing_by_global_id(thing_global_id):
    cnx_1 = get_connection_to_db()
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


def api_get_things(body, message):
    print('Get All Things')
    reply_to = json.loads(body)['reply_to']
    thing_status = json.loads(body)['thing_status']
    item_status = json.loads(body)['item_status']
    things = get_things(thing_status, item_status)
    message_response ={
        'things': things
    }
    producer_connection.ensure_connection()
    with Producer(producer_connection) as producer:
        producer.publish(
            json.dumps(message_response),
            exchange=exchange.name,
            routing_key=reply_to,
            retry=True
        )


def api_get_thing_by_global_id(body, message):
    print('Get Thing by thing_global_id')
    reply_to = json.loads(body)['reply_to']
    thing_global_id = json.loads(body)['thing_global_id']

    things = get_thing_by_global_id(thing_global_id)

    message_response ={
        'things': things
    }
    producer_connection.ensure_connection()
    with Producer(producer_connection) as producer:
        producer.publish(
            json.dumps(message_response),
            exchange=exchange.name,
            routing_key=reply_to,
            retry=True
        )


def api_get_things_by_platform_id(body, message):
    print('Get Thing by platform_id')
    reply_to = json.loads(body)['reply_to']
    platform_id = json.loads(body)['platform_id']
    thing_status = json.loads(body)['thing_status']
    item_status = json.loads(body)['item_status']
    things = get_things_by_platform_id(platform_id, thing_status, item_status)

    message_response = {
        'things': things
    }
    producer_connection.ensure_connection()
    with Producer(producer_connection) as producer:
        producer.publish(
            json.dumps(message_response),
            exchange=exchange.name,
            routing_key=reply_to,
            retry=True
        )


queue_get_things = Queue(name='registry.request.api_get_things', exchange=exchange, routing_key='registry.request.api_get_things')
queue_get_list_platforms = Queue(name='registry.request.api_get_list_platforms', exchange=exchange, routing_key='registry.request.api_get_list_platforms')
queue_add_platform = Queue(name='registry.request.api_add_platform', exchange=exchange, routing_key='registry.request.api_add_platform')
queue_check_config = Queue(name='driver.response.registry.api_check_configuration_changes', exchange=exchange, routing_key='driver.response.registry.api_check_configuration_changes')
queue_get_thing_by_global_id = Queue(name='registry.request.api_get_thing_by_global_id', exchange=exchange, routing_key='registry.request.api_get_thing_by_global_id')
queue_get_things_by_platform_id = Queue(name='registry.request.api_get_things_by_platform_id', exchange=exchange, routing_key='registry.request.api_get_things_by_platform_id')


if MODE == 'PULL':
    update_all_config_changes()

while 1:
    try:
        consumer_connection.ensure_connection(max_retries=1)
        with nested (Consumer(consumer_connection, queues=queue_get_things_by_platform_id, callbacks=[api_get_things_by_platform_id], no_ack=True),
                     Consumer(consumer_connection, queues=queue_get_thing_by_global_id, callbacks=[api_get_thing_by_global_id], no_ack=True),
                     Consumer(consumer_connection, queues=queue_add_platform, callbacks=[api_add_platform], no_ack=True),
                     Consumer(consumer_connection, queues=queue_get_things, callbacks=[api_get_things], no_ack=True),
                     Consumer(consumer_connection, queues=queue_get_list_platforms, callbacks=[api_get_list_platforms], no_ack=True),
                     Consumer(consumer_connection, queues=queue_check_config, callbacks=[handle_configuration_changes], no_ack=True)):
            while True:
                consumer_connection.drain_events()
    except (ConnectionRefusedError, exceptions.OperationalError):
        print('Connection lost')
    except consumer_connection.connection_errors:
        print('Connection error')
