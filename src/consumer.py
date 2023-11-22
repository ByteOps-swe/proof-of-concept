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

        if data['type'] == 'Temperature Sensor':
            # Conversione del formato della temperatura in un valore numerico
            temperature_value = float(data['temperature'].rstrip('C'))

            # Formattazione della data e ora nel formato richiesto da ClickHouse
            timestamp_value = datetime.datetime.strptime(data['timestamp'], '%Y-%m-%d %H:%M:%S.%f')

            # Modifica del formato della data e ora per renderlo compatibile con ClickHouse
            timestamp_formatted = timestamp_value.strftime('%Y-%m-%d %H:%M:%S')

            # Esecuzione della query di inserimento per i dati sulla temperatura
            clickhouse_client.execute(
                f"INSERT INTO dynamic_table_temperature VALUES ('{data['sensor_id']}', "
                f"'{data['type']}', {temperature_value}, '{data['season']}', '{timestamp_formatted}')"
            )
            
            logger.info("Dati sulla temperatura inseriti correttamente.")

        elif data['type'] == 'Humidity Sensor':
            # Conversione del formato dell'umidità in un valore numerico
            humidity_value = float(data['humidity'].rstrip('%'))

            # Formattazione della data e ora nel formato richiesto da ClickHouse
            timestamp_value = datetime.datetime.strptime(data['timestamp'], '%Y-%m-%d %H:%M:%S.%f')

            # Modifica del formato della data e ora per renderlo compatibile con ClickHouse
            timestamp_formatted = timestamp_value.strftime('%Y-%m-%d %H:%M:%S')

            # Esecuzione della query di inserimento per i dati sull'umidità
            clickhouse_client.execute(
                f"INSERT INTO dynamic_table_humidity VALUES ('{data['sensor_id']}', "
                f"'{data['type']}', {humidity_value}, '{timestamp_formatted}')"
            )

            logger.info("Dati sull'umidità inseriti correttamente.")
        
    except Exception as e:
        logger.error(f"Errore durante l'inserimento dei dati: {e}. Dati ricevuti: {data}")

# Chiudi la connessione ClickHouse alla fine
clickhouse_client.disconnect()
