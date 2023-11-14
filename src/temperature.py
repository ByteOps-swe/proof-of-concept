from sensore import Sensore
from datetime import datetime
from time import sleep
from random import uniform

class Temperature(Sensore):
    def __init__(self, sensor_id, topic):
        super().__init__(sensor_id, "Temperature Sensore", topic)

    def run(self):
        while True:
            self.send_message()
            sleep(5)

    def send_message(self):
        temperature = round(uniform(20.0, 30.0), 2)
        data = {
            "sensor_id": self.sensor_id,
            "type": self.sensor_type,
            "temperature": temperature,
            "timestamp": str(datetime.now())
        }
        print(f"Sending temperature data from {self.sensor_id}: {data}")
        self.producer.send(self.topic, data)
        self.producer.flush()