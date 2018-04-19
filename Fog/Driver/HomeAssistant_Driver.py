import json
import requests
import hashlib
import time
from Fog.Driver.Driver_Base import Driver


class HomeAssistant(Driver):
    def __init__(self, config_path, mode):
        self.now_info = []
        Driver.__init__(self, config_path, mode)

    def get_states(self):
        print('Get state of all things')

        url = 'http://' + self.host + ':' + self.port + '/api/states'
        response = self.connect_platform(url)
        list_thing = {
            'platform_id': str(self.platform_id),
            'things': []
        }

        for thing in response:
            # print(thing['entity_id'])
            thing_type = thing['entity_id'].split(".")[0]
            if thing_type != 'group' and thing_type != 'automation':
                # Trong HomeAssistant do quản lý chỉ đến mức Thing nên ta coi các item của 1 thing là chính nó.
                # VD: đèn là một thing và có item chính là bản thân cái đèn đó
                item_type = thing_type
                item_state = thing['state']
                # if item_type == 'sensor' and thing['entity_id'] == 'sensor.temperature':
                #     item_state = int(thing['state'])
                # else:
                #     item_state = thing['state']

                thing_temp = {
                    'thing_type': thing_type,
                    'thing_name': thing['attributes']['friendly_name'],
                    'thing_global_id': self.platform_id + '-' + thing['entity_id'],
                    'thing_local_id': thing['entity_id'],
                    'location': self.get_location_of_thing(response, thing['entity_id']),
                    'items': [
                        {
                            'item_type': item_type,
                            'item_name': thing['attributes']['friendly_name'],
                            'item_global_id': self.platform_id + '-' + thing['entity_id'] + '-' + thing['entity_id'],
                            'item_local_id': thing['entity_id'],
                            'item_state': item_state,
                            'can_set_state': self.check_can_set_state(item_type),

                        }
                    ]
                }
                list_thing['things'].append(thing_temp)

        return list_thing

    def check_configuration_changes(self):
        print('Check for changes')

        url = 'http://' + self.host + ':' + self.port + '/api/states'
        response = self.connect_platform(url)

        new_info = []

        for thing in response:
            # print(thing['entity_id'])
            thing_type = thing['entity_id'].split(".")[0]
            if thing_type != 'group' and thing_type != 'automation':
                # Trong HomeAssistant do quản lý chỉ đến mức Thing nên ta coi các item của 1 thing là chính nó.
                # VD: đèn là một thing và có item chính là bản thân cái đèn đó

                thing_temp = {
                    'thing_type': thing_type,
                    'thing_name': thing['attributes']['friendly_name'],
                    'platform_id': str(self.platform_id),
                    'thing_global_id': self.platform_id + '-' + thing['entity_id'],
                    'thing_local_id': thing['entity_id'],
                    'location': self.get_location_of_thing(response, thing['entity_id']),
                    'items': [
                        {
                            'item_type': thing_type,
                            'item_name': thing['attributes']['friendly_name'],
                            'item_global_id': self.platform_id + '-' + thing['entity_id'] + '-' + thing['entity_id'],
                            'item_local_id': thing['entity_id'],
                            'can_set_state': self.check_can_set_state(thing_type),

                        }
                    ]
                }
                new_info.append(thing_temp)

        hash_now = hashlib.md5(str(new_info).encode())
        hash_pre = hashlib.md5(str(self.now_info).encode())
        if hash_now.hexdigest() == hash_pre.hexdigest():
            return {
                'have_change': False,
                'new_info': None,
                'platform_id': self.platform_id,
            }

        else:
            self.now_info = new_info
            return {
                'have_change': True,
                'new_info': new_info,
                'platform_id': self.platform_id,
            }

    def connect_platform(self, url):
        while True:
            try:
                # url = 'http://' + host_homeAssistant + ':' + port_homeAssistant + '/api/states'
                response = requests.get(url).json()
                return response
            except:
                print("Error connect to Platform")
                time.sleep(2)
                continue

    def check_can_set_state(self, item_type):
        url = 'http://' + self.host + ':' + self.port + '/api/services'
        response = self.connect_platform(url)
        for service in response:
            if service['domain'] == item_type:
                return "yes"
        return "no"

    def get_location_of_thing(self, list_things, thing_id):
        for temp in list_things:
            if temp['entity_id'].split(".")[0] == 'group':
                for thing_in_group in temp['attributes']['entity_id']:
                    if thing_in_group == thing_id:
                        return temp['entity_id'].split(".")[1]
        return None

    def set_state(self, thing_type, thing_local_id, thing_location, thing_name,
                  item_type, item_local_id, item_name, new_state):
        print('Set sate of {} to {}'.format(thing_local_id, new_state))
        if item_type == 'light':
            print('Call Service ')
            if new_state == "ON":
                url = 'http://' + self.host + ':' + self.port + '/api/services/light/turn_on'
                data = {"entity_id": item_local_id}
                response = requests.post(url, json.dumps(data))
            else:
                url = 'http://' + self.host + ':' + self.port + '/api/services/light/turn_off'
                data = {"entity_id": item_local_id}
                response = requests.post(url, json.dumps(data))
        else:
            print('Type are not support')

if __name__ == '__main__':
    CONFIG_PATH = "config/homeassistant.ini"
    MODE = 'PUSH'
    home_assistant = HomeAssistant(CONFIG_PATH, MODE)
    home_assistant.run()
