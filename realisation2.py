import json
from kafka import KafkaProducer

import requests


def make_json(city):
    url = "https://visual-crossing-weather.p.rapidapi.com/history"
    headers = {'x-rapidapi-key': "90a1537972msh98b26d4cb863bccp1f9a63jsn177de09e506f",
               'x-rapidapi-host': "visual-crossing-weather.p.rapidapi.com"}
    start_date_time = input('Enter the start day / Example 2019-12-31: ')
    end_date_time = input('Enter the end day / Example 2019-12-31: ')
    querystring = {"startDateTime": start_date_time, "aggregateHours": "24", "location": city,
                   "endDateTime": end_date_time, "unitGroup": "metric", "contentType": "json", "shortColumnNames": "0"}
    response = requests.request("GET", url, headers=headers, params=querystring)  # str
    # response = response.text
    response = json.loads(response.content)  # making class dict

    return response


def write_in_topic(topic, message):
    producer = KafkaProducer(acks=0,
                             bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'))
    producer.send(topic, value=message)
    producer.close()


def get_data(json_file, city):
    days = json_file['locations'][city]['values']
    date = [day['datetimeStr'] for day in days]
    date = [day[:-15] for day in date]
    temperature = [temp['temp'] for temp in days]
    day_temp_dict = dict(zip(date, temperature))
    return day_temp_dict


def write_to_file(json_file, city):
    with open('weather.txt', 'w') as f:
        f.write(f'{city}\n')
        for key, value in json_file.items():
            f.write(f'Day - {key}   Temp - {value}\n')


if __name__ == '__main__':
    city = input('Enter your city: ')
    json_file = make_json(city)
    write_in_topic('json_file', json_file)
    clear_file = get_data(json_file, city)
    write_in_topic('weather_file', clear_file)
    write_to_file(clear_file, city)
