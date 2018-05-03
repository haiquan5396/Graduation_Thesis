####################################################################################
######## This part for the docs of this program ####################################
'''
.
. 	thing_global_id = platform_id + thing_local_id
. 	item_global_id  = platform_id + thing_local_id + item_local_id
. 	A item without things seemly as a thing
.
'''

import paho.mqtt.client as mqtt
import json
from requests import get
import hashlib
from openhab import openHAB
import urllib
import numpy as np

import hashlib
import time
from Fog.Driver.Driver_Base import Driver
# from Driver_Base import Driver


class OpenHAB(Driver):
    def __init__(self, config_path, mode):
        Driver.__init__(self, config_path, mode)

        base_url = "http://" + self.host + ":" + self.port + "/rest"
        self.openhab = openHAB(base_url)
        self.pre_info = None
        self.now_info = []


    def get_things_from_openhab(self):
        while True:
            try:
                thing_url = "http://" + self.host + ":" + self.port + "/rest/things"
                things = get(thing_url).json()
                return things
            except:
                print ("Error connect to OpenHAB")
                time.sleep(2)


    def get_states(self):
        things = self.get_things_from_openhab()

        states = []
        item_of_thing_list = []

        # Get all items in openHAB
        items = self.openhab.fetch_all_items()  # dict of openHAB items
        # items = items.items()                   # convert dict into list
        # items = np.asarray(items)

        # items_list = list(items[:, 0])
        items_list = list(items)
        # print (items_list)



        # Get all things and items of things in openHAB
        for thing in things:
            thing_type = thing["thingTypeUID"]
            thing_name = thing["label"]
            thing_local_id = thing["UID"]
            thing_global_id = self.platform_id + "-" + thing_local_id
            thing_location = thing["location"]
            linked_items = thing["channels"]
            items_state = []

            for linked_item in linked_items:
                item_type = linked_item["itemType"]
                item_name = linked_item["linkedItems"][0]
                item_of_thing_list.append(item_name)  # Get all item of things.
                item_local_id = item_name
                item_global_id = self.platform_id + "-" + thing_local_id + "-" + item_local_id
                item_url = "http://" + self.host + ":" + self.port + "/rest/items?recursive=false"
                item = self.openhab.get_item(item_name)
                # item_state = item.state
                if (item_type == "Number"):
                    item_state = int(item['state'])
                else:
                    item_state = str(item['state'])

                can_set_state = True
                items_state.append({
                    'item_type': str(item_type),
                    'item_name': str(item_name),
                    'item_global_id': str(item_global_id),
                    'item_local_id': str(item_local_id),
                    'item_state': item_state,
                    'can_set_state': str(can_set_state)
                }
                )

            state = {
                'thing_type': str(thing_type),
                'thing_name': str(thing_name),
                'thing_global_id': str(thing_global_id),
                'thing_local_id': str(thing_local_id),
                'location': str(thing_location),
                'items': items_state
            }
            states.append(state)

        # Get items not belong thing
        item_of_thing_list = np.asarray(item_of_thing_list)
        # print (items_list)
        # print (item_of_thing_list)

        if (item_of_thing_list != []):
            remain_item_list = list(set(items_list) - set(item_of_thing_list))
        else:
            remain_item_list = list(set(items_list))

        # Those items above be converted to things
        for item_to_thing in remain_item_list:
            item = self.openhab.get_item_raw(item_to_thing)

            item_type = item['type']
            thing_type = item_type
            item_name = item['name']
            thing_name = item_name
            # item_state = item['state']
            if (item_type == "Number"):
                item_state = int(item['state'])
            else:
                item_state = str(item['state'])

            item_local_id = item_name
            thing_local_id = thing_name
            thing_global_id = self.platform_id + '-' + thing_local_id
            item_global_id = self.platform_id + '-' + thing_local_id + '-' + item_local_id
            location = 'unknown'
            can_set_state = True

            items_state = [
                {
                    'item_type': str(item_type),
                    'item_name': str(item_name),
                    'item_global_id': str(item_global_id),
                    'item_local_id': str(item_local_id),
                    'item_state': item_state,
                    'can_set_state': str(can_set_state)
                }
            ]

            state = {
                'thing_type': str(thing_type),
                'thing_name': str(thing_name),
                'thing_global_id': str(thing_global_id),
                'thing_local_id': str(thing_local_id),
                'location': str(location),
                'items': items_state
            }

            states.append(state)

        list_thing = {
            'platform_id': self.platform_id,
            'things': states
        }

        return list_thing




    def check_configuration_changes(self):
        print('Check for changes')
        item_of_thing_list = []
        now_info = []

        things = self.get_things_from_openhab()
        # print (things)


        # Get all items in openHAB
        items = self.openhab.fetch_all_items()  # dict of openHAB items
        #items = items.items()  # convert dict into list
        #items = np.asarray(items)
        #print (items)

        # items_list = list(items[:, 0])
        items_list = list(items)

        for thing in things:
            thing_type = thing["thingTypeUID"]
            thing_name = thing["label"]
            thing_local_id = thing["UID"]
            thing_global_id = self.platform_id + "-" + thing_local_id
            thing_location = thing["location"]
            linked_items = thing["channels"]
            items_state = []

            for linked_item in linked_items:
                item_type = linked_item["itemType"]
                item_name = linked_item["linkedItems"][0]
                # item_local_id  = linked_item["uid"]
                item_of_thing_list.append(item_name)  # Get all item of things.
                item_local_id = item_name
                item_global_id = self.platform_id + "-" + thing_local_id + "-" + item_local_id
                item_url = "http://" + self.host + ":" + self.port + "/rest/items?recursive=false"
                item = self.openhab.get_item(item_name)
                item_state = item.state
                can_set_state = True
                items_state.append(
                    {
                        'item_type': str(item_type),
                        'item_name': str(item_name),
                        'item_global_id': str(item_global_id),
                        'item_local_id': str(item_local_id),
                        'can_set_state': str(can_set_state)
                    }
                )

            state = {
                'thing_type': str(thing_type),
                'thing_name': str(thing_name),
                'thing_global_id': str(thing_global_id),
                'thing_local_id': str(thing_local_id),
                'location': str(thing_location),
                'platform_id': str(self.platform_id),
                'items': items_state
            }
            now_info.append(state)

        if (item_of_thing_list != []):
            remain_item_list = list(set(items_list) - set(item_of_thing_list))
        else:
            remain_item_list = list(set(items_list))
        # print (remain_item_list)

        # Those items above be converted to things
        for item_to_thing in remain_item_list:
            item = self.openhab.get_item_raw(item_to_thing)

            item_type = item['type']
            thing_type = item_type
            item_name = item['name']
            thing_name = item_name
            item_state = item['state']
            item_local_id = item_name
            thing_local_id = thing_name
            thing_global_id = self.platform_id + '-' + thing_local_id
            item_global_id = self.platform_id + '-' + thing_local_id + '-' + item_local_id
            location = 'unknow'
            can_set_state = True

            items_state = [
                {
                    'item_type': str(item_type),
                    'item_name': str(item_name),
                    'item_global_id': str(item_global_id),
                    'item_local_id': str(item_local_id),
                    'can_set_state': str(can_set_state)
                }
            ]

            state = {
                'thing_type': str(thing_type),
                'thing_name': str(thing_name),
                'thing_global_id': str(thing_global_id),
                'thing_local_id': str(thing_local_id),
                'location': str(location),
                'platform_id': str(self.platform_id),
                'items': items_state
            }

            now_info.append(state)

        # print (now_info)

        hash_now = hashlib.sha256(str(now_info).encode())
        hash_pre = hashlib.sha256(str(self.pre_info).encode())

        print (now_info)

        if hash_now.hexdigest() == hash_pre.hexdigest():
            return {
                'have_change': False,
                'new_info': None,
                'platform_id': self.platform_id,
            }
        else:
            pre_info = now_info
            return {
                'have_change': True,
                'new_info': now_info,
                'platform_id': self.platform_id,
            }

    def set_states(self, thing_local_id, item_local_id, thing_type, item_type, state):
        item = self.openhab.get_item(item_local_id)
        item.command(state)



if __name__ == '__main__':
    CONFIG_PATH = "config/openhab.ini"
    MODE = 'PULL'
    openHAB = OpenHAB(CONFIG_PATH, MODE)
    openHAB.run()