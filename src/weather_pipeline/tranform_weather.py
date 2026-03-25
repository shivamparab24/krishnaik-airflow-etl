from datetime import datetime

def transform_weather_data(weather_data, latitude, longitude):
    hourly = weather_data.get('hourly')

    if not hourly:
        raise ValueError("Missing 'hourly' key in response")

    times = hourly.get('time')
    temps = hourly.get('temperature_2m')

    if not times:
        raise ValueError("Missing 'time' data")

    if not temps:
        raise ValueError("Missing 'temperature_2m' data")

    if len(times) != len(temps):
        raise ValueError(
            f"Length mismatch: time={len(times)}, temp={len(temps)}"
        )

    records = []

    for ts, temp in zip(times, temps):
        try:
            parsed_ts = datetime.fromisoformat(ts)
        except Exception:
            raise ValueError(f"Invalid timestamp format: {ts}")

        records.append({
            'latitude': latitude,
            'longitude': longitude,
            'temperature': temp,
            'timestamp': parsed_ts
        })

    return records