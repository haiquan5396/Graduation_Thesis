import paho.mqtt.client as mqtt
import socket

BROKER_FOG = 'localhost'
client_fog = mqtt.Client()
client_fog.connect(BROKER_FOG)


def on_disconnect(client, userdata, rc):
    if rc != 0:
        print("disconnect to Mosquitto.")


def on_connect(client, userdata, flags, rc):
    print("connect to Mosquitto")
    client_fog.publish("hihi", 'connect')

client_fog.on_connect = on_connect
client_fog.on_disconnect = on_disconnect
