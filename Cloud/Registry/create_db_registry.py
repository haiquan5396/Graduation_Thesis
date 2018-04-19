import MySQLdb

db = MySQLdb.connect(host="0.0.0.0", user="root", passwd="root")

# Create a Cursor object to execute queries.
cursor = db.cursor()

cursor.execute("""CREATE DATABASE IF NOT EXISTS Registry_DB""")

cursor.execute("""USE Registry_DB""")

cursor.execute("""CREATE TABLE IF NOT EXISTS Platform(
    platform_id VARCHAR(50) PRIMARY KEY,
    platform_name VARCHAR(30) ,
    host VARCHAR(30) ,
    port INT,
    last_response DOUBLE,
    platform_status VARCHAR(20)) """)


# #create exam table
cursor.execute("""CREATE TABLE IF NOT EXISTS Thing(
    thing_global_id VARCHAR(100) PRIMARY KEY,
    platform_id VARCHAR(50),
    thing_name VARCHAR(50),
    thing_type VARCHAR(20),
    thing_local_id VARCHAR(50),
    location VARCHAR(30),
    thing_status VARCHAR(20),
    FOREIGN KEY(platform_id) REFERENCES Platform(platform_id))""")

cursor.execute("""CREATE TABLE IF NOT EXISTS Item(
    item_global_id VARCHAR(150) PRIMARY KEY,
    thing_global_id VARCHAR(100),
    item_name VARCHAR(50),
    item_type VARCHAR(20),
    item_local_id VARCHAR(50),
    can_set_state VARCHAR(5),
    item_status VARCHAR(20),
    FOREIGN KEY(thing_global_id) REFERENCES Thing(thing_global_id))""")


db.commit()
cursor.close()