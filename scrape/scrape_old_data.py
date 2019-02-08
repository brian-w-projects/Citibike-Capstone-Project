import os
import boto3
from botocore import UNSIGNED
from botocore.client import Config
import zipfile
import requests
import json
import csv
from datetime import datetime, timedelta
from dateutil import tz
import re


def scrape_ride_data():
    """
    Scrapes Citi Bike public S3 bucket and stores .csv files locally for final ingesting
    """

    path = os.path.join(os.getcwd(), 'raw-data')
    zip_path = os.path.join(path, 'zip')
    s3 = boto3.resource('s3', config=Config(signature_version=UNSIGNED))
    bucket = s3.Bucket('tripdata')

    if not os.path.exists(path):
        os.mkdir(path)
    if not os.path.exists(zip_path):
        os.mkdir(zip_path)
    os.chdir(zip_path)

    for file in bucket.objects.filter(Delimiter='/'):
        if file.key[:6] in ['201308', '201309']:
            print('Downloading: ' + file.key)
            bucket.download_file(file.key, file.key)
            print('Unzipping: ' + file.key)
            with zipfile.ZipFile(os.path.join(zip_path, file.key)) as zip_ref:
                zip_ref.extractall(path)
            print('Cleaning Up: ' + file.key)
            os.remove(file.key)

    os.chdir(os.path.join('..', '..'))
    os.rmdir(zip_path)
    print('Finished')


def scrape_station_data():
    """
    Scrapes Citi Bike station information from public API
    """

    url = 'https://gbfs.citibikenyc.com/gbfs/es/station_information.json'
    r = requests.get(url, 'json')
    path = os.path.join(os.getcwd(), 'stations')

    if not os.path.exists(path):
        os.mkdir(path)
    os.chdir(path)

    print('Downloading Station List')
    with open('stations.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        for s in json.loads(r.content)['data']['stations']:
            writer.writerow([s['station_id'], s['lat'], s['lon'], s['name']])
    os.chdir('..')
    print('Finished')


def scrape_weather(time=datetime(2013, 1, 1, 0, 0, 0, tzinfo=tz.gettz('America/New_York')), count=900):
    """
    Scrapes Weather From API
    """

    path = os.path.join(os.getcwd(), 'weather')
    key = ''
    lat = '40.730610'
    lon = '-73.935242'

    if not os.path.exists(path):
        os.mkdir(path)
    os.chdir(path)

    with open('weather.csv', 'a', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        for day_count in range(count):
            print(time.strftime("%Y-%m-%dT%H:%M:%S%z"))
            url = 'https://api.darksky.net/forecast/{}/{},{},{}?exclude=currently,flags'.format(
                key, lat, lon, time.strftime("%Y-%m-%dT%H:%M:%S%z"))
            r = requests.get(url, 'json')
            for hour in scrape_day(json.loads(r.content)):
                writer.writerow(hour)
            time += timedelta(days=1)
    os.chdir('..')

def scrape_day(day):
    from_zone = tz.gettz('UTC')
    to_zone = tz.gettz('America/New_York')
    sunrise = day['daily']['data'][0]['sunriseTime']
    sunset = day['daily']['data'][0]['sunsetTime']

    for ele in day['hourly']['data']:
        hourly = list()
        hourly.append(
            datetime.utcfromtimestamp(ele['time']).replace(tzinfo=from_zone).astimezone(to_zone).strftime(
                '%Y-%m-%d %H:%M:%S')
        )
        if sunrise <= ele['time'] <= sunset:
            hourly.append(1)
        else:
            hourly.append(0)
        hourly.append(re.sub('(-night)|(-day)', '', ele['icon'].lower()))
        hourly.append(ele['precipProbability'])
        hourly.append(ele['apparentTemperature'])
        hourly.append(ele['humidity'])
        hourly.append(ele['windSpeed'])
        yield hourly


# scrape_weather(time=datetime(2013, 1, 11, 0, 0, 0, tzinfo=tz.gettz('America/New_York')), count=900)
