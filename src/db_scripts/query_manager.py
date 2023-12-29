#query_manager.py

from clickhouse_driver import Client

# Connessione al client ClickHouse
clickhouse_client = Client(host='city_clickhouse')

# Funzione per eseguire il contenuto di un file SQL
def execute_sql_from_file(file_path):
    with open(file_path, 'r') as file:
        sql_queries = file.read().split(';')
        for sql_query in sql_queries:
            # Rimuove spazi bianchi e newline dalle estremità della query
            sql_query = sql_query.strip()
            if sql_query:  # Ignora le stringhe vuote
                clickhouse_client.execute(sql_query + ';')

sql_create_table_path = 'app/db_scripts/queries/create_tables.sql'
execute_sql_from_file(sql_create_table_path)

# Chiudi la connessione ClickHouse alla fine
clickhouse_client.disconnect()