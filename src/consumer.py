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

# Connessione al client ClickHouse
clickhouse_client = Client(host='city_clickhouse')

# Creazione delle tabelle se non esistono
# Creazione della tabella dynamic_table_temperature
clickhouse_client.execute('''
    CREATE TABLE IF NOT EXISTS dynamic_table_temperature
    (
        `sensor_id` String,
        `type` String,
        `temperature` Decimal(4, 2),
        `season` String,
        `latitude` Float64,
        `longitude` Float64,
        `timestamp` DateTime
    )
    ENGINE = MergeTree
    ORDER BY timestamp
''')

# Creazione della tabella dynamic_table_humidity
clickhouse_client.execute('''
    CREATE TABLE IF NOT EXISTS dynamic_table_humidity
    (
        `sensor_id` String,
        `type` String,
        `humidity` Decimal(5, 2),
        `latitude` Float64,
        `longitude` Float64,
        `timestamp` DateTime   
    )
    ENGINE = MergeTree
    ORDER BY timestamp
''')


# Creazione della tabella dynamic_table_charging
clickhouse_client.execute('''
    CREATE TABLE IF NOT EXISTS dynamic_table_charging
    (
        `sensor_id` String,
        `type` String,
        `state` Bool,
        `latitude` Float64,
        `longitude` Float64,
        `timestamp` DateTime   
    )
    ENGINE = MergeTree
    ORDER BY timestamp
''')

# Chiudi la connessione ClickHouse alla fine
clickhouse_client.disconnect()

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
        f"INSERT INTO dynamic_table_temperature VALUES ('{data['sensor_id']}', "
        f"'{data['type']}', {temperature_value}, '{data['season']}', '{data['latitude']}' , '{data['longitude']}' , '{timestamp_formatted}')"
    )
    logger.info("Dati sulla temperatura inseriti correttamente.")


def insert_humidity_data(clickhouse_client, data):
    humidity_value = float(data['humidity'].rstrip('%'))
    timestamp_formatted = convert_timestamp(data['timestamp'])
    clickhouse_client.execute(
        f"INSERT INTO dynamic_table_humidity VALUES ('{data['sensor_id']}', "
        f"'{data['type']}', {humidity_value}, {data['latitude']}, {data['longitude']}, '{timestamp_formatted}')"
    )
    logger.info("Dati sull'umidità inseriti correttamente.")


def insert_charging_data(clickhouse_client, data):
    charging_value = float(data['state'])
    timestamp_formatted = convert_timestamp(data['timestamp'])
    clickhouse_client.execute(
        f"INSERT INTO dynamic_table_charging VALUES ('{data['sensor_id']}', "
        f"'{data['type']}', {charging_value}, {data['latitude']}, {data['longitude']}, '{timestamp_formatted}')"
    )
    logger.info("Dati sulla colonnina di ricarica inseriti correttamente.")

for message in consumer:
    try:
        data = message.value
        logger.debug("Received data from Kafka: %s", data)
        data_type = data['type'].lower()
        if data_type == 'temperature sensor':
            insert_temperature_data(clickhouse_client, data)
        elif data_type == 'humidity sensor':
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