from kafka import KafkaConsumer
from clickhouse_driver import Client
import json

# Configurazione Kafka
kafka_server = "kafka:9092"  # Utilizza il nome del servizio Kafka come hostname
topic = "test_topic"

# Configurazione ClickHouse
clickhouse_host = "clickhouse"  # Utilizza il nome del servizio ClickHouse come hostname
clickhouse_port = 9000
clickhouse_database = "your_database"
clickhouse_table = "your_table"

# Crea il consumatore Kafka
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=kafka_server,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

# Crea il client ClickHouse
clickhouse_client = Client(host=clickhouse_host, port=clickhouse_port, database=clickhouse_database)

# Funzione per inserire dati in ClickHouse
def insert_to_clickhouse(data):
    query = f"INSERT INTO {clickhouse_table} (column1, column2, ...) VALUES (%s, %s, ...)"
    # Sostituisci "column1", "column2", ... con i nomi delle colonne effettive
    clickhouse_client.execute(query, (data["value"], data["timestamp"]))

# Funzione per consumare e processare i messaggi da Kafka
def consume_and_insert():
    for message in consumer:
        value = message.value
        print("Received message:", value)
        insert_to_clickhouse(value)

if __name__ == "__main__":
    consume_and_insert()
