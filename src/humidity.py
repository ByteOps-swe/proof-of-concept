# humidity.py

from sensore import Sensore
from datetime import datetime
from time import sleep
from random import uniform

MAX_HUMIDITY_CHANGE = 5.0

class Humidity(Sensore):
    shared_humidity = None
    min_shared_humidity = 100.0  # Inizializzato con un valore massimo
    last_sensor_readings = {}  # Dizionario per tenere traccia delle ultime letture dei sensori

    def __init__(self, sensor_id, topic, latitude, longitude, initial_humidity=30.0):
        super().__init__(sensor_id, "Humidity Sensor", latitude, longitude, topic)
        self.current_humidity = None
        self.set_initial_humidity(initial_humidity)
        if Humidity.shared_humidity is None:
            Humidity.shared_humidity = self.current_humidity

    def run(self):
        while True:
            self.send_message()
            sleep(5)

    def set_initial_humidity(self, initial_humidity):
        self.current_humidity = initial_humidity

    def send_message(self):
        base_humidity_change = round(uniform(-MAX_HUMIDITY_CHANGE, MAX_HUMIDITY_CHANGE), 2)

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
            if abs(diff) > MAX_HUMIDITY_CHANGE:
                new_humidity = last_reading + (MAX_HUMIDITY_CHANGE if diff > 0 else -MAX_HUMIDITY_CHANGE)

        Humidity.last_sensor_readings[self.sensor_id] = new_humidity

        # Aggiornamento umidità con controlli effettuati
        Humidity.shared_humidity = new_humidity
        self.current_humidity = new_humidity

        data = {
            "sensor_id": self.sensor_id,
            "type": self.sensor_type,
            "humidity": f"{round(self.current_humidity, 2)}%",
            "latitude": self.latitude,
            "longitude": self.longitude,
            "timestamp": str(datetime.now())
        }

        print(f"Sending humidity data from {self.sensor_id}: {data}")
        self.producer.send(self.topic, data)
        self.producer.flush()