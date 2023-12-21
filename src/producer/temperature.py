from threading import Thread
from sensore import Sensore
from datetime import datetime
from time import sleep
from random import uniform

max_temperature_change = 2.0

class Temperature(Sensore):

    shared_temperature = None
    last_sensor_readings = {}

    def __init__(self, sensor_id, sensor_city, sensor_cell, sensor_type, latitude, longitude, topic):
        super().__init__(
            sensor_id,
            sensor_city,
            sensor_cell,
            sensor_type,
            latitude,
            longitude,
            topic)
        self.current_temperature = None
        self.current_season = None
        self.set_initial_temperature()
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
        base_temperature_change = round(uniform(-max_temperature_change, max_temperature_change), 2)

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
            if abs(diff) > max_temperature_change:
                new_temperature = last_reading + (max_temperature_change if diff > 0 else -max_temperature_change)

        Temperature.last_sensor_readings[self.sensor_id] = new_temperature

        # Aggiornamento temperatura con controlli effettuati
        Temperature.shared_temperature = new_temperature
        self.current_temperature = new_temperature
        
        data = {
            "sensor_id": self.sensor_id,
            "sensor_city": self.sensor_city,
            "sensor_cell": self.sensor_cell,
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