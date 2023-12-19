#temperature.py

from threading import Thread
from sensore import Sensore
from datetime import datetime
from time import sleep
from random import uniform

MAX_TEMPERATURE_CHANGE = 2.0

class Temperature(Sensore):

    shared_temperature = None
    min_shared_temperature = float('inf')  # Inizializzato con un valore massimo
    last_sensor_readings = {}  # Dizionario per tenere traccia delle ultime letture dei sensori, chiave=sensor_id e valore=temperature

    def __init__(self, sensor_id, latitude, longitude, topic):
        super().__init__(sensor_id, "Temperature Sensor", latitude, longitude, topic)
        self.current_temperature = None
        self.current_season = None
        self.set_initial_temperature()
        if Temperature.shared_temperature is None:
            Temperature.shared_temperature = self.set_initial_temperature()


    def run(self):
        while True:
            self.send_message()
            sleep(10)


    def set_initial_temperature(self):
        current_month = datetime.now().month
        if 3 <= current_month <= 5:
            self.current_season = "Spring"
            return 10.0
        elif 6 <= current_month <= 8:
            self.current_season = "Summer"
            return 25.0
        elif 9 <= current_month <= 11:
            self.current_season = "Autumn"
            return 10.0
        else:
            self.current_season = "Winter"
            return 0.0


    def update_season(self):
        current_month = datetime.now().month
        if 3 <= current_month <= 5:
            self.current_season = "Spring"
        elif 6 <= current_month <= 8:
            self.current_season = "Summer"
        elif 9 <= current_month <= 11:
            self.current_season = "Autumn"
        else:
            self.current_season = "Winter"

    def send_message(self):
        self.update_season()
        base_temperature_change = round(uniform(-MAX_TEMPERATURE_CHANGE, MAX_TEMPERATURE_CHANGE), 2)

        if self.current_season == "Autumn":
            min_temp, max_temp = 5.0, 25.0
        elif self.current_season == "Spring":
            min_temp, max_temp = 5.0, 25.0
        elif self.current_season == "Summer":
            min_temp, max_temp = 15.0, 42.0
        else:  # Winter
            min_temp, max_temp = -15.0, 15.0

        # Controllo della differenza tra la temperatura minima e la nuova temperatura calcolata
        new_temperature = Temperature.shared_temperature + base_temperature_change
        new_temperature = max(min_temp, min(max_temp, new_temperature))

        temperature_diff = new_temperature - Temperature.shared_temperature
        if temperature_diff > 4.0:
            new_temperature = Temperature.shared_temperature + 4.0

        # Controllo della differenza tra le letture successive dello stesso sensore
        if self.sensor_id in Temperature.last_sensor_readings:
            last_reading = Temperature.last_sensor_readings[self.sensor_id]
            diff = new_temperature - last_reading
            if abs(diff) > MAX_TEMPERATURE_CHANGE:
                new_temperature = last_reading + (MAX_TEMPERATURE_CHANGE if diff > 0 else -MAX_TEMPERATURE_CHANGE)

        Temperature.last_sensor_readings[self.sensor_id] = new_temperature

        # Aggiornamento temperatura con controlli effettuati
        Temperature.shared_temperature = new_temperature
        self.current_temperature = new_temperature
        
        data = {
            "sensor_id": self.sensor_id,
            "type": self.sensor_type,
            "temperature": f"{round(self.current_temperature, 2)}C",
            "season": self.current_season,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "timestamp": str(datetime.now())
        }
        
        print(f"Sending temperature data from {self.sensor_id}: {data}")
        self.producer.send(self.topic, data)
        self.producer.flush()