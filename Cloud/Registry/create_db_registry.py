import MySQLdb

db = MySQLdb.connect(host="172.17.0.4", user="root", passwd="root")

# Create a Cursor object to execute queries.
cursor = db.cursor()

cursor.execute("""CREATE DATABASE IF NOT EXISTS Registry""")

cursor.execute("""USE Registry""")


cursor.execute("""CREATE TABLE IF NOT EXISTS Platform(
    PlatformId VARCHAR(50) PRIMARY KEY,
    PlatformName VARCHAR(30) ,
    PlatformType VARCHAR(30) ,
    PlatformHost VARCHAR(30) ,
    PlatformPort INT,
    PlatformStatus VARCHAR(20),
    LastResponse DOUBLE)""")

cursor.execute("""CREATE TABLE IF NOT EXISTS IoTResource(
    ResourceId VARCHAR(50) PRIMARY KEY,
    EndPoint VARCHAR(150),
    ResourceStatus VARCHAR(20),
    Description VARCHAR(200),
    ResourceType VARCHAR(20),
    Label VARCHAR(100),
    PlatformId VARCHAR(50),
    LocalId VARCHAR (50),
    FOREIGN KEY(PlatformId) REFERENCES Platform(PlatformId))""")


cursor.execute("""CREATE TABLE IF NOT EXISTS Thing(
    ThingGlobalId VARCHAR(50) PRIMARY KEY,
    ThingName VARCHAR(50),
    FOREIGN KEY(ThingGlobalId) REFERENCES IoTResource(ResourceId))""")


cursor.execute("""CREATE TABLE IF NOT EXISTS Metric(
    MetricId VARCHAR(150) PRIMARY KEY,
    ResourceId VARCHAR(100),
    MetricName VARCHAR(50),
    MetricType VARCHAR(20),
    Unit VARCHAR(20),
    MetricDomain VARCHAR(20),
    MetricStatus VARCHAR(20),
    MetricLocalId VARCHAR (50),
    FOREIGN KEY(ResourceId) REFERENCES IoTResource(ResourceId))""")


db.commit()
cursor.close()