from pycurl import global_cleanup

import paho.mqtt.client as mqtt
import json
from kombu import Connection, Queue, Exchange, Consumer, exceptions, uuid
from kombu.utils.compat import nested
import threading

global response
connection = Connection("localhost")
connection.connect()

def callback_threading(body, message):
        print("threading callback")
        response = 1

def call(channel):
    queue_threading = Queue("threading", exclusive=True, auto_delete=True)
    Consumer(channel,
                  callbacks=[callback_threading],
                  queues=[queue_threading], no_ack=True).consume()
    print("Het")
        # while True:
        #     connection_threading.drain_events()
    # threading.Timer(5, call, args=(connection_threading,)).start()



def main_callback(body, message):
    print("main_callback")



main_queue = Queue("main", exclusive=True, auto_delete=True)
channel_1 = connection.channel()
threading.Thread(target=call, args=(channel_1,)).start()
# call(connection)
channel_2 = connection.channel()
Consumer(channel_2,
              callbacks=[main_callback],
              queues=[main_queue], no_ack=True).consume()
while True:
    connection.drain_events()