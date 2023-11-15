import json
from kafka import KafkaConsumer, KafkaProducer

kafka_server = ["kafka:9092"]  # Use the name of the Kafka service as the hostname

topic = "city_topic"

consumer = KafkaConsumer(
    bootstrap_servers=kafka_server,
    value_deserializer=json.loads,
    auto_offset_reset="latest",
)

consumer.subscribe(topic)

while True:
    data = next(consumer)
    print(data)
    print(data.value)