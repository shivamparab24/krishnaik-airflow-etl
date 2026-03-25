from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = 'postgres_default'

def load_weather_data(records):
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS weather_archive (
        latitude FLOAT,
        longitude FLOAT,
        temperature FLOAT,
        timestamp TIMESTAMP,
        PRIMARY KEY (latitude, longitude, timestamp)
    );
    """)

    insert_sql = """
    INSERT INTO weather_archive (
        latitude, longitude, temperature, timestamp
    )
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (latitude, longitude, timestamp) DO NOTHING;
    """

    rows = [
        (r['latitude'], r['longitude'], r['temperature'], r['timestamp'])
        for r in records
    ]

    cursor.executemany(insert_sql, rows)

    conn.commit()
    cursor.close()