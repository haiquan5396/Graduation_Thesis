from kombu import Connection, Queue, Exchange, Consumer, Producer, exceptions
from kombu.utils.compat import nested
import json
import socket
import Logging.config_logging as logging
import uuid


_LOGGER = logging.get_logger(__name__)


class BrokerCloudClient:
    def __init__(self, broker_cloud, exchange_name, exchange_type='direct'):
        self.rabbitmq_connection = Connection(broker_cloud)
        self.exchange = Exchange(exchange_name, type=exchange_type)

    def create_queue(self, queue_name, routing_key):
        return Queue(name=queue_name, exchange=self.exchange, routing_key=routing_key, message_ttl=20)

    def create_consumer(self, queue_object, callback):
        return Consumer(self.rabbitmq_connection, queues=queue_object, callbacks=[callback], no_ack=True)

    def subscribe_message(self, info_consumers):
        consumers = []
        for info in info_consumers:
            queue_object = self.create_queue(info['queue_name'], info['routing_key'])
            new_consumer = self.create_consumer(queue_object, info['callback'])
            consumers.append(new_consumer)

        while 1:
            try:
                self.rabbitmq_connection.ensure_connection(max_retries=3)
                with nested(*consumers):
                    while True:
                        self.rabbitmq_connection.drain_events()
            except (ConnectionRefusedError, exceptions.OperationalError):
                _LOGGER.error('Connection to Broker Cloud is lost')
            except self.rabbitmq_connection.connection_errors:
                _LOGGER.error('Connection to Broker Cloud is error')

    def publish_messages(self, message, queue_name, routing_key=None, queue_routing_key=None):
        _LOGGER.debug("queue_name: {}".format(queue_name))
        _LOGGER.debug("message: {}".format(message))
        if queue_routing_key is None:
            queue_routing_key = queue_name
        if routing_key is None:
            routing_key = queue_name

        self.rabbitmq_connection.ensure_connection()
        try:
            with Producer(self.rabbitmq_connection) as producer:
                producer.publish(
                    json.dumps(message),
                    exchange=self.exchange.name,
                    routing_key=routing_key,
                    retry=True
                )
        except:
            queue_publish = self.create_queue(queue_name, queue_routing_key)
            with Producer(self.rabbitmq_connection) as producer:
                producer.publish(
                    json.dumps(message),
                    exchange=self.exchange.name,
                    routing_key=routing_key,
                    declare=[queue_publish],
                    retry=True
                )

    def request_service(self, message_request, request_routing_key):
        id_response = str(uuid.uuid4())
        queue_response = Queue(name=id_response, exchange=self.exchange, routing_key=id_response, exclusive=True, auto_delete=True)
        message_request['header']['reply_to'] = id_response
        self.rabbitmq_connection.ensure_connection()
        with Producer(self.rabbitmq_connection) as producer:
            producer.publish(
                json.dumps(message_request),
                exchange=self.exchange.name,
                routing_key=request_routing_key,
                declare=[queue_response],
                retry=True
            )

        message_response = None

        def on_response(body, message):
            nonlocal message_response
            message_response = json.loads(body)
        try:

            with Consumer(self.rabbitmq_connection, queues=queue_response, callbacks=[on_response], no_ack=True):
                try:
                    while message_response is None:
                        self.rabbitmq_connection.drain_events(timeout=10)
                except socket.timeout:
                    _LOGGER.error("Request timeout")
                    return {
                        'error': 'Can not connect to service'
                    }
        except Exception:
            _LOGGER.error("Cannot create Consumer in queue_name: " + request_routing_key)
            return {
                'error': 'Cannot create Consumer '
            }

        return message_response

