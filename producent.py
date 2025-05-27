import json
import requests
from time import sleep
from kafka import KafkaProducer
 
SERVER = "broker:9092"
TOPIC = "sobota"
URL = "https://data.sensor.community/airrohr/v1/filter/country=PL"
 
def sensor_data_generator(url=URL):
    response = requests.get(url)
 
    if response.status_code != 200:
        raise RuntimeError(f"Błąd podczas pobierania danych: {response.status_code}")
 
    raw_data = response.json()
 
    all_value_types = set()
    for entry in raw_data:
        for value in entry.get('sensordatavalues', []):
            all_value_types.add(value['value_type'])
 
    for entry in raw_data:
        record = {
            'timestamp': entry.get('timestamp'),
            'sensor_id': entry.get('sensor', {}).get('id'),
            'sensor_type': entry.get('sensor', {}).get('sensor_type', {}).get('name'),
            'latitude': entry.get('location', {}).get('latitude'),
            'longitude': entry.get('location', {}).get('longitude')
        }
 
        for value_type in all_value_types:
            record[value_type] = None
 
        for value in entry.get('sensordatavalues', []):
            vt = value['value_type']
            try:
                record[vt] = float(value['value'])
            except ValueError:
                record[vt] = value['value']
 
        yield record
 
if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers=[SERVER],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )
 
    try:
        while True:
            for message in sensor_data_generator():
                producer.send(TOPIC, value=message)
                print(f"Wysłano wiadomość: {message}")
                sleep(1)
    except KeyboardInterrupt:
        producer.close()
        print("Producent zatrzymany.")
