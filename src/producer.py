# main.py
from temperature import Temperature
from humidity import Humidity
from datetime import datetime
from random import uniform
from kafka import KafkaProducer

while True:
    print("Script is being run directly.")
    num_sensors = 5  # Numero di sensori
    sensors_temp = []
    sensors_hum = []

    for i in range(num_sensors):
        id_string = f"tem_sensor{i+1}"
        sensor = Temperature(sensor_id=id_string, topic="city_topic")
        sensor.start()  # Avvia il thread del sensore
        sensors_temp.append(sensor)
        
    for i in range(num_sensors):
        id_string = f"hum_sensor{i+1}"
        sensor = Humidity(sensor_id=id_string, topic="city_topic")
        sensor.start()  # Avvia il thread del sensore
        sensors_hum.append(sensor)

    for sensor in sensors_temp:
        sensor.join()

    for sensor in sensors_hum:
        sensor.join()