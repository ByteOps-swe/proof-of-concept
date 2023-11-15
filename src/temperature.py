from sensore import Sensore
from datetime import datetime
from time import sleep
from random import uniform

class Temperature(Sensore):

    def __init__(self, sensor_id, topic, initial_temperature=25.0):
        super().__init__(sensor_id, "Temperature Sensor", topic)
        self.current_temperature = initial_temperature

    def run(self):
        while True:
            self.send_message()
            sleep(5)

    def send_message(self):
        # Logica per generare la nuova temperatura basata sulla precedente
        temperature_change = round(uniform(-3.0, 3.0), 2)  # Cambiamento casuale tra -1 e 1 gradi
        self.current_temperature += temperature_change
        self.current_temperature = max(20.0, min(30.0, self.current_temperature))
        data = {
            "sensor_id": self.sensor_id,
            "type": self.sensor_type,
            "temperature": f"{round(self.current_temperature, 2)}C",
            "timestamp": str(datetime.now())
        }
        print(f"Sending temperature data from {self.sensor_id}: {data}")
        self.producer.send(self.topic, data)
        self.producer.flush()
