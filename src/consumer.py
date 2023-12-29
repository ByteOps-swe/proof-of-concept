#consumer.py

import json
from kafka import KafkaConsumer
from clickhouse_driver import Client
import datetime  
import logging

# Configura il logger
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

kafka_server = ["kafka:9092"]
topic = "city_topic"

# Creazione del consumatore Kafka
consumer = KafkaConsumer(
    bootstrap_servers=kafka_server,
    value_deserializer=json.loads,
    auto_offset_reset="latest",
)

consumer.subscribe([topic])

# Connessione al client ClickHouse
clickhouse_client = Client(host='city_clickhouse')

def convert_timestamp(timestamp):
    timestamp_value = datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')
    return timestamp_value.strftime('%Y-%m-%d %H:%M:%S')


def insert_temperature_data(clickhouse_client, data):
    temperature_value = float(data['temperature'].rstrip('C'))
    timestamp_formatted = convert_timestamp(data['timestamp'])
    clickhouse_client.execute(
        f"INSERT INTO temperature_table VALUES ('{data['sensor_id']}', "
        f"'{data['sensor_city']}', '{data['sensor_cell']}', '{data['type']}', {temperature_value}, '{data['season']}', '{data['latitude']}' , '{data['longitude']}' , '{timestamp_formatted}')"
    )
    logger.info("Dati sulla temperatura inseriti correttamente.")


def insert_humidity_data(clickhouse_client, data):
    humidity_value = float(data['humidity'].rstrip('%'))
    timestamp_formatted = convert_timestamp(data['timestamp'])
    clickhouse_client.execute(
        f"INSERT INTO humidity_table VALUES ('{data['sensor_id']}', "
        f"'{data['sensor_city']}', '{data['sensor_cell']}', '{data['type']}', {humidity_value}, {data['latitude']}, {data['longitude']}, '{timestamp_formatted}')"
    )
    logger.info("Dati sull'umidità inseriti correttamente.")


def insert_charging_data(clickhouse_client, data):
    charging_value = float(data['state'])
    timestamp_formatted = convert_timestamp(data['timestamp'])
    clickhouse_client.execute(
        f"INSERT INTO charging_station_table VALUES ('{data['sensor_id']}', "
        f"'{data['sensor_city']}', '{data['sensor_cell']}', '{data['type']}', {charging_value}, {data['latitude']}, {data['longitude']}, '{timestamp_formatted}')"
    )
    logger.info("Dati sulla colonnina di ricarica inseriti correttamente.")


for message in consumer:
    try:
        data = message.value
        logger.debug("Received data from Kafka: %s", data)
        data_type = data['type'].lower()
        if data_type == 'temperature':
            insert_temperature_data(clickhouse_client, data)
        elif data_type == 'humidity':
            insert_humidity_data(clickhouse_client, data)
        elif data_type == 'charging station':
            insert_charging_data(clickhouse_client, data)
        else:
            logger.warning(f"Tipo non riconosciuto: {data['type']}")
            continue
    except Exception as e:
        logger.error(f"Errore durante l'inserimento dei dati: {e}. Dati ricevuti: {data}")


# Chiudi la connessione ClickHouse alla fine
clickhouse_client.disconnect()