# main.py
from temperature import Temperature
from datetime import datetime
from random import uniform
from kafka import KafkaProducer

while True:
    print("Script is being run directly.")
    num_sensors = 5  # Numero di sensori
    sensors = []

    for i in range(num_sensors):
        id_string = f"sensor{i+1}"
        sensor = Temperature(sensor_id=id_string, topic="test_topic")
        sensor.start()  # Avvia il thread del sensore
        sensors.append(sensor)

    for sensor in sensors:
        sensor.join()