from sensore import Sensore
from datetime import datetime
from time import sleep
from random import uniform

class Humidity(Sensore):

    def __init__(self, sensor_id, topic, initial_humidity=30.0):
        super().__init__(sensor_id, "Humidity Sensor", topic)                
        self.current_humidity = initial_humidity

    def run(self):
        while True:
            self.send_message()
            sleep(5)
        
    def get_humidity_range(self):
        return (0.0, 100.0)        
    
    def send_message(self):
        humidity_change = round(uniform(-10.0, 10.0), 2)  # Cambiamento casuale tra -10% e 10%
        self.current_humidity += humidity_change
        self.current_humidity = max(0.0, min(100.0, self.current_humidity))
        data = {
            "sensor_id": self.sensor_id,
            "type": self.sensor_type,
            "humidity": f"{round(self.current_humidity, 2)}%",
            "timestamp": str(datetime.now())
        }
        print(f"Sending humidity data from {self.sensor_id}: {data}")
        self.producer.send(self.topic, data)
        self.producer.flush()