# humidity.py

from sensore import Sensore
from datetime import datetime
from time import sleep
from random import uniform

max_humidity_change = 5.0

class Humidity(Sensore):
    shared_humidity = None
    last_sensor_readings = {}

    def __init__(self, sensor_id, sensor_city, sensor_cell, sensor_type, latitude, longitude, topic, initial_humidity=30.0):
        super().__init__(
            sensor_id,
            sensor_city,
            sensor_cell,
            sensor_type,
            latitude,
            longitude,
            topic)
        self.current_humidity = initial_humidity
        Humidity.shared_humidity = self.current_humidity

    def run(self):
        while True:
            self.send_message()
            sleep(10)

    def send_message(self):
        base_humidity_change = round(uniform(-max_humidity_change, max_humidity_change), 2)

        # Controllo della differenza tra la temperatura minima e la nuova temperatura calcolata
        new_humidity = Humidity.shared_humidity + base_humidity_change
        new_humidity = max(0.0, min(100.0, new_humidity))

        humidity_diff = new_humidity - Humidity.shared_humidity
        if humidity_diff > 4.0:
            new_humidity = Humidity.shared_humidity + 4.0

        # Controllo della differenza tra le letture successive dello stesso sensore
        if self.sensor_id in Humidity.last_sensor_readings:
            last_reading = Humidity.last_sensor_readings[self.sensor_id]
            diff = new_humidity - last_reading
            if abs(diff) > max_humidity_change:
                new_humidity = last_reading + (max_humidity_change if diff > 0 else -max_humidity_change)

        Humidity.last_sensor_readings[self.sensor_id] = new_humidity

        # Aggiornamento umidità con controlli effettuati
        Humidity.shared_humidity = new_humidity
        self.current_humidity = new_humidity

        data = {
            "sensor_id": self.sensor_id,
            "sensor_city": self.sensor_city,
            "sensor_cell": self.sensor_cell,
            "type": self.sensor_type,
            "humidity": f"{round(self.current_humidity, 2)}%",
            "latitude": self.latitude,
            "longitude": self.longitude,
            "timestamp": str(datetime.now())
        }

        print(f"Sending humidity data from {self.sensor_id}: {data}")
        self.producer.send(self.topic, data)
        self.producer.flush()