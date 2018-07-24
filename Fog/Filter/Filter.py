import json
import paho.mqtt.client as mqtt
import sys
from Performance_Monitoring.message_monitor import MessageMonitor


class Filter():
    def __init__(self, broker_fog):
        self.client = mqtt.Client()
        self.client.connect(broker_fog)
        self.message_monitor = MessageMonitor('0.0.0.0', 8086)

    def on_connect(self, client, userdata, flags, rc):
        print("Connected to Mosquitto")
        filter_topic_sub = 'driver/response/filter/api_get_states'
        self.client.subscribe(filter_topic_sub)

    def filter_message(self, client, userdata, msg):
        filter_topic_pub = 'filter/response/forwarder/api_get_states'
        print(msg.payload.decode("utf-8"))
        data = json.loads(msg.payload.decode('utf-8'))
        data['message_monitor'] = self.message_monitor.monitor(data, 'filter', 'filter_message')
        data = json.dumps(data)
        self.client.publish(filter_topic_pub, data)

    def run(self):
        self.client.on_connect = self.on_connect
        self.client.on_message = self.filter_message
        self.client.loop_forever()

if __name__ == '__main__':
    # MODE_CODE = 'Develop'
    MODE_CODE = 'Deploy'

    if MODE_CODE == 'Develop':
        BROKER_FOG = 'localhost'
    else:
        BROKER_FOG = sys.argv[1]

    filter_fog = Filter(BROKER_FOG)
    filter_fog.run()

