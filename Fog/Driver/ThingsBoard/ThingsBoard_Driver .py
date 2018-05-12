import http.client
import json
import paho.mqtt.client as mqtt
import hashlib
from Driver_Base import Driver
import time

class ThingsBoard(Driver):
	def __init__(self, config_path, mode):
		self.now_info = []
		Driver.__init__(self, config_path, mode)

	def get_authorization(self):
		conn = http.client.HTTPConnection(self.host + ':' + self.port)
		headers = {
			'Accept': "application/json",
			'Content-Type': "application/json"
		}

		body = '{"username":"tenant@thingsboard.org", "password":"tenant"}'

		conn.request("POST", "/api/auth/login", body = body, headers = headers)
		response_data = conn.getresponse().read()
		response_json = json.loads(response_data.decode("utf-8"))

		return response_json

	def connect(self):
		while True:
			try:
				conn = http.client.HTTPConnection(self.host + ':' + self.port)
				authorization = self.get_authorization()
				headers = {
					'Accept': "application/json",
					'X-Authorization': "Bearer " + authorization["token"],
				}
				return [conn, headers]
			except:
				print("Error connect to Platform")
				time.sleep(2)
				continue

	def get_list_device_on_customes(self):
		print("get list")
		result = self.connect()
		conn = result[0]
		headers = result[1]

		conn.request("GET", "/api/customer/3f4fd570-4ed4-11e8-a082-9dc4b7fcfa12/devices?limit=111", headers = headers)
		data = conn.getresponse().read()
		json_data = json.loads(data.decode("utf-8"))
		device_list = json_data['data']

		return device_list

	def get_access_token_device(self, thing_local_id):
		print("Get Access token device: ")
		result = self.connect()
		conn = result[0]
		headers = result[1]

		conn.request("GET", "/api/device/" + thing_local_id + "/credentials", headers = headers)
		data = conn.getresponse().read()
		json_data = json.loads(data.decode("utf-8"))
		
		return json_data['credentialsId']

	def get_telemetry_keys(self, thing_local_id):
		telemetries = ""

		result = self.connect()
		conn = result[0]
		headers = result[1]

		url = "/api/plugins/telemetry/DEVICE/" + thing_local_id + "/keys/timeseries"
		conn.request("GET", url, headers = headers)
		data = conn.getresponse().read()
		json_data = json.loads(data.decode("utf-8"))

		for i, telemetry in enumerate(json_data):
			if i == len(json_data) - 1:
				telemetries = telemetries + telemetry
			else:
				telemetries = telemetries + telemetry + ","

		return [json_data, telemetries]

	def get_states(self):
		print("get states")
		list_thing = {
			'platform_id': str(self.platform_id),
			'things': []
    	}

		states = []
		device_list = self.get_list_device_on_customes()

		result = self.connect()
		conn = result[0]
		headers = result[1]

		for device in device_list:
			result_telemetry_keys = self.get_telemetry_keys(device["id"]["id"])
			keys_telemetry_list = result_telemetry_keys[0]
			telemetries = result_telemetry_keys[1]

			url = "/api/plugins/telemetry/DEVICE/" + device["id"]["id"] + "/values/timeseries?keys=" + telemetries
			
			conn.request("GET", url, headers = headers)
			response_data = conn.getresponse().read()
			response_json = json.loads(response_data.decode("utf-8"))

			state = {
				'thing_type': device["type"],
				'thing_name': device["name"],
	            'thing_global_id': self.platform_id + '/' + device["id"]["id"],
	            'thing_local_id': device["id"]["id"],
	            'location': "null",
	            'items': []
	        }

			for telemetry in keys_telemetry_list:
				if device["id"]["id"] == "bb12cda0-4f80-11e8-a082-9dc4b7fcfa12":
					item_state = int(response_json[telemetry][0]["value"])
				else:
					item_state = response_json[telemetry][0]["value"]
					
				item = {
					'item_type': device["id"]["entityType"],
					'item_name': telemetry,
		            'item_global_id': self.platform_id + '/' + device["id"]["id"] + telemetry,
		            'item_local_id': device["id"]["id"] + telemetry,
		            'item_state': item_state,
	            	'can_set_state': self.check_can_set_state(device["type"])
				}
				state['items'].append(item)

			states.append(state)

		list_thing['things'] = states
		print(list_thing)
		return list_thing

	def check_configuration_changes(self):
		new_info = []
		device_list = self.get_list_device_on_customes()
		result = self.connect()
		conn = result[0]
		headers = result[1]

		for device in device_list:
			result_telemetry_keys = self.get_telemetry_keys(device["id"]["id"])
			keys_telemetry_list = result_telemetry_keys[0]
			telemetries = result_telemetry_keys[1]

			url = "/api/plugins/telemetry/DEVICE/" + device["id"]["id"] + "/values/timeseries?keys=" + telemetries
			
			conn.request("GET", url, headers = headers)
			response_data = conn.getresponse().read()
			response_json = json.loads(response_data.decode("utf-8"))

			state = {
	            'thing_type': device["type"],
	            'thing_name': device["name"],
	            'platform_id': str(self.platform_id),
	            'thing_global_id': self.platform_id + '/' + device["id"]["id"],
	            'thing_local_id': device["id"]["id"],
	            'location': "null",
	            'items': []
	        }


			for telemetry in keys_telemetry_list:
				item = {
					'item_type': device["id"]["entityType"],
					'item_name': telemetry,
		            'item_global_id': self.platform_id + '/' + device["id"]["id"] + telemetry,
		            'item_local_id': device["id"]["id"] + telemetry,
		            'item_state': response_json[telemetry][0]["value"],
	            	'can_set_state': self.check_can_set_state(device["type"])
				}
				state['items'].append(item)

			new_info.append(state)

		hash_now = hashlib.md5(str(new_info).encode())
		hash_pre = hashlib.md5(str(self.now_info).encode())
		if hash_now.hexdigest() == hash_pre.hexdigest():
			return {
				'have_change': False,
				'new_info': None,
				'platform_id': str(self.platform_id)
			}
		else:
			self.now_info = new_info
			return {
				'have_change': True,
				'new_info': new_info,
				'platform_id': str(self.platform_id)
			}

	def check_can_set_state(self, thing_type):
		if thing_type == "led":
			return "yes"
		return "no"


	def set_state(self, thing_type, thing_local_id, new_state):
		print("Set state {} into {}" . format(thing_local_id, new_state))

		result = self.connect()
		conn = result[0]
		headers = result[1]
		access_token = self.get_access_token_device(thing_local_id)

		if thing_type == "led":
			if new_state == "ON":
				body = '{"LED": "ON"}'
				conn.request("POST", "/api/v1/" + access_token + "/telemetry", body = body, headers = headers)
			elif new_state == "OFF":
				body = '{"LED": "OFF"}'
				conn.request("POST", "/api/v1/" + access_token + "/telemetry", body = body, headers = headers)
			else:
				print("Error set state")
		else:
			print("Type not support set state")

if __name__ == '__main__':
    CONFIG_PATH = "config/thingsboard.ini"
    MODE = 'PULL'
    things_board = ThingsBoard(CONFIG_PATH, MODE)
    things_board.run()
