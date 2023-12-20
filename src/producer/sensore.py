#sensore.py

import json
from kafka import KafkaProducer
from threading import Thread


class Sensore(Thread):
    kafka_server = ["kafka:9092"] 

    def __init__(self, sensor_id, sensor_city, sensor_cell, sensor_type, latitude, longitude, topic):
        super().__init__()
        self.sensor_id = sensor_id
        self.sensor_city = sensor_city
        self.sensor_cell = sensor_cell
        self.sensor_type = sensor_type  
        self.topic = topic
        self.latitude = latitude
        self.longitude = longitude
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_server,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def run(self):
        pass

    def send_message(self):
        pass