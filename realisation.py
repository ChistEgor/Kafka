import requests
import json
from kafka import KafkaProducer


def get_url() -> str:
    city = input('Enter your city: ')
    open_weather_link = 'https://api.openweathermap.org/data/2.5/weather'
    open_weather_api = 'db71095002de213ae977f3d6cd10ed4f'
    response = requests.get(url=open_weather_link, params={'q': city, 'appid': open_weather_api, 'units': 'metric'})
    return response.url


def write_in_topic(topic, message):
    producer = KafkaProducer(acks=0,
                             bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'))
    producer.send(topic, value=message)
    producer.close()


def get_weather(url) -> str:
    response = requests.get(url)
    if response.status_code == 200:
        response = json.loads(response.content)
        sky = response['weather'][0]['description']
        wind = response['wind']['speed']
        temp = round(response['main']['temp'])
        feels_temp = round(response['main']['feels_like'])
        country = response['sys']['country']
        city = response['name']
        return f'The weather today in {city} {country} - \nsky: {sky}; \nwind: {wind} m/s;' \
               f' \ntemp: {temp} C; \nfeels_temp: {feels_temp} C' \
               f'\n------------------------------'
    return 'Incorrect name of city'


# def unix_converter_date():


if __name__ == '__main__':
    url = get_url()
    write_in_topic('site', url)
    weather = get_weather(url)
    write_in_topic('weather', weather)
''