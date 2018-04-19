#!/usr/bin/env python
from __future__ import absolute_import, unicode_literals

from kombu import Connection, Producer, Consumer, Queue, uuid, Exchange

import json

exchange = Exchange("IoT", type="direct")
class FibonacciRpcClient(object):

    def __init__(self, connection):
        self.connection = connection
        hihi = uuid()
        self.callback_queue = Queue(hihi, exchange=exchange, routing_key=hihi, exclusive=True, auto_delete=True)

    def on_response(self, message):
        print("Nhan")
        self.response = json.loads(message.payload)["items"]

    def call(self, n):
        self.response = None
        self.correlation_id = uuid()

        message_request = {'list_item_global_id': ['9e9b2777-33bd-4128-8ddc-b8b7e6ab6100/sensor.humidity/sensor.humidity'],
                           'reply_to': self.callback_queue.name}
        with Producer(self.connection) as producer:
            producer.publish(
                json.dumps(message_request),
                exchange='IoT',
                routing_key='dbreader.request.api_get_item_state',
                declare=[self.callback_queue],
                reply_to=self.callback_queue.name,
                correlation_id=self.correlation_id,
            )
        with Consumer(self.connection,
                      on_message=self.on_response,
                      queues=[self.callback_queue], no_ack=True):
            while self.response is None:
                self.connection.drain_events()
        return self.response
    #
    # def call_2(self, n):
    #     self.response = None
    #     self.correlation_id = uuid()
    #     with Producer(self.connection) as producer:
    #         producer.publish(
    #             {'n': n},
    #             exchange='',
    #             routing_key='rpc_queue_2',
    #             declare=[self.callback_queue],
    #             reply_to=self.callback_queue.name,
    #             correlation_id=self.correlation_id,
    #         )
    #     with Consumer(self.connection,
    #                   on_message=self.on_response,
    #                   queues=[self.callback_queue], no_ack=True):
    #         while self.response is None:
    #             self.connection.drain_events()
    #     return self.response


def main(broker_url):
    connection = Connection(broker_url)
    fibonacci_rpc = FibonacciRpcClient(connection)
    print(' [x] Requesting fib(30)')
    # while True:
    response = fibonacci_rpc.call(30)
    print(' [.] Got {0!r}'.format(response))
    # response = fibonacci_rpc.call_2(30)
    # print(' [.] Got {0!r}'.format(response))
    # while True:
    #     continue


if __name__ == '__main__':
    main('localhost')