# pip install beautifulsoup4 requests

from time import sleep
from json import dumps
from kafka import KafkaProducer
from bs4 import BeautifulSoup
import requests
from datetime import datetime

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}

def find_weather(city_name):

   city_name = city_name.replace(" ", "+")
   weather_info = {}

   try:
       res = requests.get(
           f'https://www.google.com/search?q={city_name}+weather&oq={city_name}&aqs=chrome.0.69i59j0i512l9.3330j1j9&sourceid=chrome&ie=UTF-8', headers=headers)

       soup = BeautifulSoup(res.text, 'html.parser')
    #    location = soup.select('#wob_loc')[0].getText().strip()
       time = str(soup.select('#wob_dts')[0].getText().strip())
       info = soup.select('#wob_dc')[0].getText().strip()
       temperature = soup.select('#wob_tm')[0].getText().strip()
       precipitation = soup.select('#wob_pp')[0].getText().strip()
       humidity = soup.select('#wob_hm')[0].getText().strip()
       wind = soup.select('#wob_ws')[0].getText().strip()

       weather_info['location'] = city_name.title()
       weather_info['time'] = time
       weather_info['info'] = info
       weather_info['temperature'] = temperature
       weather_info['precipitation'] = precipitation
       weather_info['humidity'] = humidity
       weather_info['wind'] = wind

       return weather_info

   except:
       raise Exception("Please enter a valid city name")


kafka_producer = KafkaProducer(bootstrap_servers = ['kafka-broker-1:9092'], value_serializer = lambda x:dumps(x).encode('utf-8'))

while True:
    # city_name = input("Enter City Name: ")
    city_name = "Naperville"
    weather_info = find_weather(city_name)
    message_sent_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    payload = {'data': weather_info, 'message_sent_at': message_sent_at}
    print(payload)
    kafka_producer.send(topic = 'weather_test_v1', value = payload)
    sleep(6)
