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

# Creazione della tabella se non esiste
clickhouse_client.execute('''
    CREATE TABLE IF NOT EXISTS dynamic_table_temperature
    (
        `sensor_id` String,
        `type` String,
        `temperature` Decimal(4, 2),
        `season` String,
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

for message in consumer:
    try:
        data = message.value
        print("Received data from Kafka:", data)

        # Conversione del formato della temperatura in un valore numerico
        temperature_value = float(data['temperature'].rstrip('C'))

        # Formattazione della data e ora nel formato richiesto da ClickHouse
        timestamp_value = datetime.datetime.strptime(data['timestamp'], '%Y-%m-%d %H:%M:%S.%f')

        # Modifica del formato della data e ora per renderlo compatibile con ClickHouse
        timestamp_formatted = timestamp_value.strftime('%Y-%m-%d %H:%M:%S')

        # Esecuzione della query di inserimento
        clickhouse_client.execute(
            f"INSERT INTO dynamic_table_temperature VALUES ('{data['sensor_id']}', "
            f"'{data['type']}', {temperature_value}, '{data['season']}', '{timestamp_formatted}')"
        )
        
        logger.info("Dati inseriti correttamente.")
        
    except Exception as e:
        logger.error(f"Errore durante l'inserimento dei dati: {e}")


# Chiudi la connessione ClickHouse alla fine
clickhouse_client.disconnect()
