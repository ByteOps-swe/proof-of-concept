import json
from datetime import datetime
from time import sleep
from random import uniform
from kafka import KafkaProducer
from threading import Thread, Lock

class Sensore(Thread):
    kafka_server = ["kafka:9092"]  # Server Kafka condiviso tra tutti i sensori

    def __init__(self, sensor_id, topic):
        super().__init__()
        self.sensor_id = sensor_id
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_server,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        self._stop_event = False
        self._lock = Lock()

    def send_message(self):
        raise NotImplementedError("Subclasses must implement this method.")

    def stop(self):
        with self._lock:
            self._stop_event = True

    def run(self):
        while True:
            with self._lock:
                if self._stop_event:
                    break
            self.send_message()
            sleep(5)

if __name__ == "__main__":
    print("Script is being run directly.")

    class SensorePersonalizzato(Sensore):
        def send_message(self):
            temperature = round(uniform(20.0, 30.0), 2)
            data = {
                "sensor_id": self.sensor_id,
                "temperature": temperature,
                "timestamp": str(datetime.now())
            }
            print(f"Sending temperature data from {self.sensor_id}: {data}")
            self.producer.send(self.topic, data)
            self.producer.flush()

    num_sensors = 5  # Numero di sensori
    sensors = []

    for i in range(num_sensors):
        id_string = f"sensor{i+1}"
        sensor = SensorePersonalizzato(sensor_id=id_string, topic="test_topic")
        sensor.start()  # Avvia il thread del sensore
        sensors.append(sensor)

    # Aggiungi un ritardo prima di fermare i sensori (per scopi dimostrativi)
    sleep(20)

    for sensor in sensors:
        sensor.stop()

    for sensor in sensors:
        sensor.join()