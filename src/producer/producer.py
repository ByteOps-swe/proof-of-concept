#producer.py

from temperature import Temperature
from humidity import Humidity
from charging_station import Charging_Station
from datetime import datetime
from random import uniform
from kafka import KafkaProducer

if __name__ == "__main__":
    
    print("Script is being run directly.")
    num_sensors = 5  # Numero di sensori
    sensors_temp = []
    sensors_hum = []
    sensors_charg = []
    latitudes = [45.406435, 45.5454787, 45.4408474, 45.4383842, 45.666889]
    longitudes = [11.876761, 11.5354214, 12.3155151, 10.9916215, 12.243044]

    for i in range(num_sensors):
        id_string_temp = f"tem_sensor{i+1}"
        sensor_temp = Temperature(sensor_id=id_string_temp, latitude=latitudes[i], longitude=longitudes[i], topic="city_topic")
        sensor_temp.start()
        sensors_temp.append(sensor_temp)
        
    for i in range(num_sensors):
       id_string = f"hum_sensor{i+1}"
       sensor = Humidity(sensor_id=id_string, topic="city_topic", latitude=latitudes[i], longitude=longitudes[i])
       sensor.start()  # Avvia il thread del sensore
       sensors_hum.append(sensor)

    for i in range(num_sensors):
       id_string = f"charging_stat{i+1}"
       sensor = Charging_Station(sensor_id=id_string, topic="city_topic", latitude=latitudes[i], longitude=longitudes[i])
       sensor.start()  # Avvia il thread del sensore
       sensors_charg.append(sensor)

    for sensor in sensors_temp:
        sensor.join()
    for sensor in sensors_hum:
        sensor.join()
    for sensor in sensors_charg:
        sensor.join()