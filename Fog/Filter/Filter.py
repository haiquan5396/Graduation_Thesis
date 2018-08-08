import json
import paho.mqtt.client as mqtt
import sys
import copy


class Filter:
    def __init__(self, broker_fog):
        self.client = mqtt.Client()
        self.client.connect(broker_fog)
        self.now_state = {}         # {global_id : value_state}

    def on_connect(self, client, userdata, flags, rc):
        print("Connected to Mosquitto")
        filter_topic_sub = 'driver/response/filter/api_get_states'
        self.client.subscribe(filter_topic_sub)

    def filter_message(self, client, userdata, msg):
        print("Filter message")
        filter_topic_pub = 'filter/response/forwarder/api_get_states'
        message = json.loads(msg.payload.decode('utf-8'))
        print("Before: {}".format(message))
        received_state = message['body']['states']
        filter_states = copy.deepcopy(received_state)
        for state in received_state:
            if 'MetricId' in state:
                if state['MetricId'] in self.now_state:
                    if self.now_state[state['MetricId']] != state["DataPoint"]["Value"]:
                        # metric change state value
                        self.now_state[state['MetricId']] = state["DataPoint"]["Value"]
                    else:
                        # metric don't change state value
                        if message['header']['mode'] == 'PUSH':
                            filter_states.remove(state)
                else:
                    # metric has registered and this is the first time it is passed to filter
                    self.now_state[state['MetricId']] = state["DataPoint"]["Value"]
            else:
                # metric don't have MetricId =>> Metric hasn't registered
                filter_states.remove(state)
        if len(filter_states) > 0:
            message['body']['states'] = filter_states
            #data['message_monitor'] = self.message_monitor.monitor(data, 'filter', 'filter_message')
            print("After: {}".format(message))
            self.client.publish(filter_topic_pub, json.dumps(message))
        else:
            print("Loc Het")

    def run(self):
        self.client.on_connect = self.on_connect
        self.client.on_message = self.filter_message
        self.client.loop_forever()

if __name__ == '__main__':
    MODE_CODE = 'Develop'
    # MODE_CODE = 'Deploy'

    if MODE_CODE == 'Develop':
        BROKER_FOG = 'localhost'
    else:
        BROKER_FOG = sys.argv[1]

    filter_fog = Filter(BROKER_FOG)
    filter_fog.run()

