import paho.mqtt.client as mqtt
import json
from random import randint
import time


broker_fog = '0.0.0.0'
clientMQTT_1 = mqtt.Client()
clientMQTT_1.connect(broker_fog, 1884)

clientMQTT_2 = mqtt.Client()
clientMQTT_2.connect(broker_fog, 1885)

clientMQTT_3 = mqtt.Client()
clientMQTT_3.connect(broker_fog, 1886)


def on_disconnect(self, client, userdata, rc):
    if rc != 0:
        print("disconnect to Mosquitto.")


def on_connect(self, client, userdata, flags, rc):
    print("connected to Mosquitto")


clientMQTT_1.on_connect = on_connect
clientMQTT_1.on_disconnect = on_disconnect
clientMQTT_2.on_connect = on_connect
clientMQTT_2.on_disconnect = on_disconnect
clientMQTT_3.on_connect = on_connect
clientMQTT_3.on_disconnect = on_disconnect

while 1:
    for i in range(1, 25):
        message = {'temperature': randint(0, 100)}
        clientMQTT_1.publish('sensor_' + str(i), json.dumps(message))
        # clientMQTT_2.publish('sensor_' + str(i), json.dumps(message))
        # clientMQTT_3.publish('sensor_' + str(i), json.dumps(message))

    time.sleep(2)
