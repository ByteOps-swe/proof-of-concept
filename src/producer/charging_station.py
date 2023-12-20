#charging_station.py

from threading import Thread
from sensore import Sensore
from datetime import datetime
from time import sleep
import random

class Charging_Station(Sensore):
    def __init__(self, sensor_id, sensor_city, sensor_cell, sensor_type, latitude, longitude, topic, state):
        super().__init__(
            sensor_id,
            sensor_city,
            sensor_cell,
            sensor_type,
            latitude,
            longitude,
            topic)
        self.state = state
        
    def run(self):
        while True:
            self.send_message()
            sleep(10)

    def send_message(self):
        self.update_state()
        data = {
            "sensor_id": self.sensor_id,
            "state": self.sensor_state,
            "sensor_city": self.sensor_city,
            "sensor_cell": self.sensor_cell,
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
