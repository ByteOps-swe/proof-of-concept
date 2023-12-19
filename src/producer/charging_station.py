#charging_station.py

from threading import Thread
from sensore import Sensore
from datetime import datetime
from time import sleep
import random

class Charging_Station(Sensore):
    def __init__(self, sensor_id, latitude, longitude, topic):
        super().__init__(sensor_id, "Charging Station", latitude, longitude, topic)
        self.current_state = True   #inizializzato a true -> funziona 


    def run(self):
        while True:
            self.send_message()
            sleep(10)


    def send_message(self):
        self.update_state()
        data = {
            "sensor_id": self.sensor_id,
            "type": self.sensor_type,
            "state": self.current_state,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "timestamp": str(datetime.now())
        }
        print(f"Sending station data from {self.sensor_id}: {data}")
        self.producer.send(self.topic, data)
        self.producer.flush()


    def update_state(self):
        if(random.random() > 0.9):
            self.current_state = not self.current_state
