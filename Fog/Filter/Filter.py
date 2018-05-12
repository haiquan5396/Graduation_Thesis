import json
import paho.mqtt.client as mqtt
import sys
filter_topic_sub = 'driver/response/filter/api_get_states'

filter_topic_pub = 'filter/response/forwarder/api_get_states'

# BROKER_FOG = sys.argv[1]
BROKER_FOG='localhost'
client = mqtt.Client()

client.connect(BROKER_FOG)


def on_connect(client, userdata, flags, rc):
    print("Connected to Mosquitto")

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(filter_topic_sub)

# The callback for when a PUBLISH message is received from the server.


def filter_message(client, userdata, msg):
    print(msg.payload.decode("utf-8"))
    data = json.loads(msg.payload.decode('utf-8'))
    data = json.dumps(data)
    client.publish(filter_topic_pub, data)

client.on_connect = on_connect
client.on_message = filter_message

client.loop_forever()
