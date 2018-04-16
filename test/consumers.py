#!/usr/bin/env python
from __future__ import absolute_import, unicode_literals

from kombu import Connection, Queue
from kombu.mixins import ConsumerProducerMixin
import threading
rpc_queue_1 = Queue('rpc_queue_1')
rpc_queue_2 = Queue('rpc_queue_2')


def fib(n):
    # if n == 0:
    #     return 0
    # elif n == 1:
    #     return 1
    # else:
        return 2


class Worker(ConsumerProducerMixin, threading.Thread):

    def __init__(self, connection, thread_id):
        threading.Thread.__init__(self)
        self.connection = connection
        self.thread_id = thread_id

    def get_consumers(self, Consumer, channel):
        return [Consumer(
            queues=[rpc_queue_1],
            on_message=self.on_request_1,
            accept={'application/json'},
            prefetch_count=1,
        ), Consumer(
            queues=[rpc_queue_2],
            on_message=self.on_request_2,
            accept={'application/json'},
            prefetch_count=1,
        )]

    def on_request_1(self, message):
        n = message.payload['n']
        print(' [.] fib({0}) in thread_id: {1} '.format(n, self.thread_id))
        result = fib(n)

        self.producer.publish(
            {'result': 'on_request_1'},
            exchange='', routing_key=message.properties['reply_to'],
            correlation_id=message.properties['correlation_id'],
            serializer='json',
            retry=True,
        )
        message.ack()

    def on_request_2(self, message):
        n = message.payload['n']
        print(' [.] fib({0})'.format(n))
        result = fib(n)
        # tạo một Producer và publish message
        self.producer.publish(
            {'result': 'on_request_2'},
            exchange='', routing_key=message.properties['reply_to'],
            correlation_id=message.properties['correlation_id'],
            serializer='json',
            retry=True,
        )
        message.ack()


def start_worker(broker_url):
    connection = Connection(broker_url)
    print(' [x] Awaiting RPC requests')
    worker = Worker(connection, 1)
    worker.start()
    # worker_2 = Worker(connection, 2)
    # worker_2.start()
    print("after run")


if __name__ == '__main__':
    try:
        start_worker('localhost')
    except KeyboardInterrupt:
        pass