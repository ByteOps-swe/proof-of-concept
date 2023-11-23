from sensore import Sensore
from datetime import datetime
from time import sleep
from random import uniform

class Temperature(Sensore):

    def __init__(self, sensor_id, topic):
        super().__init__(sensor_id, "Temperature Sensor", topic)
        self.current_temperature = None
        self.current_season = None
        self.set_initial_temperature()


    def set_initial_temperature(self):
        # Imposta la temperatura iniziale in base alla stagione corrente
        current_month = datetime.now().month
        if 3 <= current_month <= 5:
            self.current_season = "Spring"
            self.current_temperature = 10.0
        elif 6 <= current_month <= 8:
            self.current_season = "Summer"
            self.current_temperature = 25.0
        elif 9 <= current_month <= 11:
            self.current_season = "Autumn"
            self.current_temperature = 10.0
        else:
            self.current_season = "Winter"
            self.current_temperature = 0.0


    def run(self):
        while True:
            self.send_message()
            sleep(5)


    def update_season(self):
        # Aggiorna la stagione ogni mese
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

        # Logica per generare la nuova temperatura basata sulla precedente e sulla stagione
        base_temperature_change = round(uniform(-3.0, 3.0), 2)
        self.current_temperature += base_temperature_change
        self.current_temperature = max(-20.0, min(50.0, self.current_temperature))

        data = {
            "sensor_id": self.sensor_id,
            "type": self.sensor_type,
            "temperature": f"{round(self.current_temperature, 2)}C",
            "season": self.current_season,
            "timestamp": str(datetime.now())
        }

        print(f"Sending temperature data from {self.sensor_id}: {data}")
        self.producer.send(self.topic, data)
        self.producer.flush()