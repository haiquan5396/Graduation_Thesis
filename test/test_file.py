# from influxdb import InfluxDBClient
# from mysql.connector.pooling import MySQLConnectionPool
# clientDB = InfluxDBClient('localhost', 8086, 'root', 'root', 'Collector_DB')
# clientDB.create_database('Collector_DB')
# item_global_id = "bf74f877-b97f-4c03-b8fb-9bbb029e4ce8/sensor.light/sensor.light"
# temp = 'SELECT * FROM \"' + item_global_id + '\" ORDER BY time DESC LIMIT 2'
# query_result = clientDB.query(temp)
# print("{}".format(query_result))
# for temp in list(query_result)[0]:
#     print(temp)
#
# query_thing = """SELECT Thing.platform_id, Thing.thing_global_id, Thing.thing_name,
#                         Thing.thing_type, Thing.location, Thing.thing_local_id
#                   FROM  Thing;"""

# dbconfig = {
#   "database": "Registry_DB",
#   "user":     "root",
#   "host":     "192.168.0.110",
#   "passwd":   "root",
#   "autocommit": "True"
# }
#
# cnxpool = MySQLConnectionPool(pool_name = "mypool", pool_size = 32, **dbconfig)
#
# def get_connection_to_db():
#     while True:
#         try:
#             print("Get connection DB")
#             connection = cnxpool.get_connection()
#             return connection
#         except:
#             print("Can't get connection DB")
#             pass
#
# def get_things(thing_status, item_status):
#     cnx_1 = get_connection_to_db()
#     cursor_1 = cnx_1.cursor()
#     query_thing = ""
#     query_item = ""
#     if thing_status == 'active':
#         query_thing = """SELECT Thing.platform_id, Thing.thing_global_id, Thing.thing_name,
#                                 Thing.thing_type, Thing.location, Thing.thing_local_id, Thing.thing_status
#                           FROM  Thing
#                           WHERE Thing.thing_status = 'active'; """
#     elif thing_status == 'inactive':
#         query_thing = """SELECT Thing.platform_id, Thing.thing_global_id, Thing.thing_name,
#                                 Thing.thing_type, Thing.location, Thing.thing_local_id, Thing.thing_status
#                           FROM  Thing
#                           WHERE Thing.thing_status = 'inactive'; """
#     elif thing_status == 'all':
#         query_thing = """SELECT Thing.platform_id, Thing.thing_global_id, Thing.thing_name,
#                                 Thing.thing_type, Thing.location, Thing.thing_local_id, Thing.thing_status
#                           FROM  Thing;"""
#
#     if item_status == 'active':
#         query_item = """SELECT Item.thing_global_id, Item.item_global_id, Item.item_name,
#                                Item.item_type, Item.can_set_state, Item.item_local_id, Item.item_status
#                           FROM Item
#                           WHERE Item.item_status='active';"""
#     elif item_status == 'inactive':
#         query_item = """SELECT Item.thing_global_id, Item.item_global_id, Item.item_name,
#                                Item.item_type, Item.can_set_state, Item.item_local_id, Item.item_status
#                           FROM Item
#                           WHERE Item.item_status='inactive';"""
#     elif item_status == 'all':
#         query_item = """SELECT Item.thing_global_id, Item.item_global_id, Item.item_name,
#                                Item.item_type, Item.can_set_state, Item.item_local_id, Item.item_status
#                           FROM Item;"""
#
#     cursor_1.execute(query_thing)
#     thing_rows = cursor_1.fetchall()
#
#     cursor_1.execute(query_item)
#     item_rows = cursor_1.fetchall()
#     cursor_1.close()
#     cnx_1.close()
#     things = []
#     for thing in thing_rows:
#         temp_thing = {
#             'platform_id': thing[0],
#             'thing_global_id': thing[1],
#             'thing_name': thing[2],
#             'thing_type': thing[3],
#             'location': thing[4],
#             'thing_local_id': thing[5],
#             'thing_staus': thing[6],
#             'items': []
#         }
#
#         for item in item_rows:
#             if item[0] == thing[1]:
#                 temp_item = {
#                     'item_global_id': item[1],
#                     'item_name': item[2],
#                     'item_type': item[3],
#                     'can_set_state': item[4],
#                     'item_local_id': item[5],
#                     'item_status': item[6]
#                 }
#                 temp_thing['items'].append(temp_item)
#         things.append(temp_thing)
#
#     return things
#
# platform_id = 'bf74f877-b97f-4c03-b8fb-9bbb029e4ce8'
# things_in_system = get_things("inactive", "active")
# things_in_platform = []
# for thing in things_in_system:
#     if thing['platform_id'] == platform_id:
#         things_in_platform.append(thing)
# print(things_in_system)


# def api_get_list_platforms():
#     print('Get list platforms')
#     platform_id = '4db78c62-a726-4cae-a0f1-b9cd855c6ab2'
#     cnx_1 = get_connection_to_db()
#     cursor_1 = cnx_1.cursor()
#     cursor_1.execute("""SELECT platform_status FROM Platform WHERE platform_id=%s""", (platform_id,))
#     rows = cursor_1.fetchall()[0][0]
#     print(rows)
#     # list_platforms = []
#     # for row in rows:
#     #     list_platforms.append({
#     #         "platform_id": row[0],
#     #         "platform_name": row[1],
#     #         "host": row[2],
#     #         "port": row[3],
#     #         "last_response": row[4],
#     #         "platform_status": row[5]
#     #     })
#     # print(list_platforms)
#     cursor_1.close()
#     cnx_1.close()

# api_get_list_platforms()
# import time
# while True:
#     print("Th
# is prints once a minute.")
#     time.sleep(2)
# a = None
# print(a is  None)

# import configparser
# config = configparser.ConfigParser()
# # print(config.read('inaaait.cfg'))
# print(len(config.read('config/init.cfg')))
# print(config.sections())
# print(config['BROKER']['host'])
#
# if 'platform' not in config['mosquitto']:
# with open('config/init.cfg', 'w') as configfile:
#     print(config)
#     print(configfile)
#     # config.write(configfile)

# class Parent:
#     def __init__(self, lala):
#         self.test = 2
#         self.lala = lala
#
#     def parent_method(self):
#         self.child_method()
#
#
#     def child_method(self):
#         print("child_method Parent")
#
#
# class Child(Parent):
#     def __init__(self):
#         Parent.__init__(self, 3)
#
#     def child_method(self):
#         print("child method child")
#
# Child().parent_method()
hii = 0
if 1 is None and 1 == 1:
    hii =3
print(hii)