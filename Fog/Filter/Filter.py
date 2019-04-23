import json
import sys
import copy
from Communicator.broker_fog import BrokerFogClient
import Logging.config_logging as logging

_LOGGER = logging.get_logger(__name__)


class Filter:
    def __init__(self, broker_fog):
        # create connection to Broker_Fog
        self.client_fog = BrokerFogClient(broker_fog)
        self.now_state = {}         # {global_id : value_state}

    def filter_message(self, client, userdata, msg):
        filter_topic_pub = 'filter/response/forwarder/api_get_states'
        message = json.loads(msg.payload.decode('utf-8'))
        # self.logger.debug("Meassage before: {}".format(message))

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
            _LOGGER.info("Send filtered metric to Collector")
            self.client_fog.publish_message(filter_topic_pub, json.dumps(message))
        else:
            _LOGGER.debug("Filter the messages and don't send to Collector ")

    def run(self):
        info_subscribers = [
            {
                'topic': 'driver/response/filter/api_get_states',
                'callback': self.filter_message
            }
        ]
        self.client_fog.subscribe_message(info_subscribers)
        self.client_fog.loop(loop_forever=True)


if __name__ == '__main__':
    MODE_CODE = 'Develop'
    # MODE_CODE = 'Deploy'

    if MODE_CODE == 'Develop':
        BROKER_FOG = 'localhost'
    else:
        BROKER_FOG = sys.argv[1]

    filter_fog = Filter(BROKER_FOG)
    filter_fog.run()

