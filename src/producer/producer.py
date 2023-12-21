import json
from temperature import Temperature
from humidity import Humidity
from charging_station import Charging_Station
from datetime import datetime
from random import uniform
from kafka import KafkaProducer

with open("app/producer/sensors.json", "r") as json_file:
    sensor_data = json.load(json_file)

sensors = []

for sensor_type, sensor_list in sensor_data.items():
    for sensor_info in sensor_list:
        if sensor_type == "temperature_list":
            print ("sono qua")
            sensor = Temperature(**sensor_info)
        elif sensor_type == "humidity_list":
            sensor = Humidity(**sensor_info)
        elif sensor_type == "charging_station_list":
            sensor = Charging_Station(**sensor_info)

        sensor.start()
        sensors.append(sensor)

for sensor in sensors:
    sensor.join()