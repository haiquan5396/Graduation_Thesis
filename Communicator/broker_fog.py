import paho.mqtt.client as mqtt
import Logging.config_logging as logging

_LOGGER = logging.get_logger(__name__)


class BrokerFogClient:

    def __init__(self, broker_fog):
        self.client = mqtt.Client()
        self.client.connect(broker_fog, keepalive=20)
        self.info_subscribers = []

    def on_disconnect(self, client, userdata, rc):
        if rc != 0:
            _LOGGER.warning("Disconnected to Broker Fog.")

    def on_connect(self, client, userdata, flags, rc):
        _LOGGER.info("Connected to Broker Fog")
        for info in self.info_subscribers:
            self.client.subscribe(info['topic'])
            self.client.message_callback_add(info['topic'], info['callback'])

    def publish_message(self, topic, message, qos=2):
        _LOGGER.debug('topic: {}'.format(topic))
        _LOGGER.debug('message: {}'.format(message))
        self.client.publish(topic, message, qos=qos)

    def subscribe_message(self, info_subscribers):
        self.info_subscribers = info_subscribers
        for info in self.info_subscribers:
            self.client.subscribe(info['topic'])
            self.client.message_callback_add(info['topic'], info['callback'])
        self.client.on_disconnect = self.on_disconnect
        self.client.on_connect = self.on_connect

    def unsubscribe(self, topics):
        _LOGGER.info("Unsubscribe topics: {}".format(topics))
        for topic in topics:
            self.client.unsubscribe(topic)
            self.client.message_callback_remove(topic)

    def loop(self, loop_forever=True):
        if loop_forever is True:
            self.client.loop_forever()
        else:
            self.client.loop_start()

    def loop_one_time(self):
        self.client.loop()
