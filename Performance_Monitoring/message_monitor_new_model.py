import time
import uuid
from influxdb import InfluxDBClient


class MessageMonitor:
    def __init__(self, host_influxdb, port_influxdb):
        self.clientDB = InfluxDBClient('0.0.0.0', '8086', 'root', 'root', 'Message_Monitor')
        self.clientDB.create_database('Message_Monitor')

    def start_message(self, header, name_service, name_function):
        start_time = time.time()
        if 'message_monitor' not in header:
            message_monitor = {
                'start_time': start_time,
                'from_service': name_service,
                'from_function': name_function,
                'id': str(uuid.uuid4())
            }
        else:
            id_message = header['message_monitor']['id']
            message_monitor = {
                'start_time': start_time,
                'from_service': name_service,
                'from_function': name_function,
                'id': id_message
            }

        return message_monitor

    def end_message(self, header, name_service, name_function):
        if 'message_monitor' in header:
            end_time = time.time()
            start_time = header['message_monitor']['start_time']

            header['message_monitor']['end_time'] = end_time
            header['message_monitor']['to_service'] = name_service
            header['message_monitor']['process_time'] = end_time - start_time
            print(end_time - start_time)
            header['message_monitor']['to_function'] = name_function

            #print("write db monitor {}".format(header['message_monitor']))
            message_monitor = header['message_monitor']
            record = [{
                'measurement': 'message_time',
                'tags': {
                    'from_service': message_monitor['from_service'],
                    'from_function': message_monitor['from_function'],
                    'to_service': message_monitor['to_service'],
                    'to_function': message_monitor['to_function'],
                    'id_message': message_monitor['id']
                },
                'fields': {
                    'start_time': message_monitor['start_time'],
                    'end_time': message_monitor['end_time'],
                    'process_time': message_monitor['process_time']
                }
            }]
            #print(record)
            self.clientDB.write_points(record)

    def monitor(self, header, name_service, name_function):
        self.end_message(header, name_service, name_function)
        return self.start_message(header, name_service, name_function)
