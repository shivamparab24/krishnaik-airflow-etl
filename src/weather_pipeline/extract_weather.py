from airflow.providers.http.hooks.http import HttpHook

API_CONN_ID = 'archive_open_meteo_api'

def fetch_weather_data(latitude, longitude, execution_date):
    date = execution_date.strftime('%Y-%m-%d')

    http_hook = HttpHook(
        http_conn_id=API_CONN_ID,
        method='GET'
    )
    # https://archive-api.open-meteo.com/v1/archive?latitude=52.52&longitude=13.41&start_date=2026-03-08&end_date=2026-03-22&hourly=temperature_2m

    endpoint = (
        f'v1/archive?latitude={latitude}'
        f'&longitude={longitude}'
        f'&start_date={date}'
        f'&end_date={date}'
        f'&hourly=temperature_2m'
    )

    response = http_hook.run(endpoint)

    if response.status_code != 200:
        raise Exception(f"API failed: {response.status_code}, {response.text}")

    return response.json()