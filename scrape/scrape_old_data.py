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
import argparse
from shutil import copyfile


def scrape_ride_data():
    """
    Scrapes Citi Bike public S3 bucket and stores .csv files locally for final ingesting
    """

    # path = os.path.join(os.getcwd(), 'raw-data')
    path = os.path.join('D:\\', 'raw-data')
    zip_path = os.path.join(path, 'zip')
    format_path = os.path.join(os.getcwd(), 'formatted-data')
    s3 = boto3.resource('s3', config=Config(signature_version=UNSIGNED))
    bucket = s3.Bucket('tripdata')

    if not os.path.exists(path):
        os.mkdir(path)
    if not os.path.exists(zip_path):
        os.mkdir(zip_path)
    if not os.path.exists(format_path):
        os.mkdir(format_path)
    os.chdir(zip_path)

    for file in bucket.objects.filter(Delimiter='/'):
        if re.match(r'^\d{6}-[^0-9]', str(file.key)):
            print('Downloading: ' + file.key)
            bucket.download_file(file.key, file.key)
            print('Unzipping: ' + file.key)
            with zipfile.ZipFile(os.path.join(zip_path, file.key)) as zip_ref:
                zip_ref.extractall(path)
            print('Cleaning Up: ' + file.key)
            os.remove(file.key)
        else:
            print('Skipping: ' + file.key)

    os.chdir(os.path.join('..', '..'))

    for file in os.listdir('./raw-data'):
        inpath = './raw-data/' + file
        outpath = './formatted-data/' + file
        with open(inpath) as infile:
            with open(outpath, 'w', newline='') as outfile:
                print('Formatting: ' + file)
                reader = csv.reader(infile, delimiter=',')
                writer = csv.writer(outfile, delimiter=',')
                writer.writerow(next(reader))
                for row in reader:
                    try:
                        row[1] = str(datetime.strptime(row[1], '%m/%d/%Y %H:%M:%S'))
                        row[2] = str(datetime.strptime(row[2], '%m/%d/%Y %H:%M:%S'))
                        writer.writerow(row)
                    except:
                        print('Properly Formatted')
                        copyfile(inpath, outpath)
                        break

    os.chdir(path)
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

    print('Downloading Current Station List')
    with open('stations.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        for s in json.loads(r.content)['data']['stations']:
            writer.writerow([s['station_id'], s['lat'], s['lon'], s['name']])
    os.chdir('..')
    print('Finished')


def scrape_weather(date=datetime(2013, 1, 1, 0, 0, 0, tzinfo=tz.gettz('America/New_York')), count=1, key=None):
    """
    Scrapes Weather From API
    """

    path = os.path.join(os.getcwd(), 'weather')
    lat = '40.730610'
    lon = '-73.935242'

    if not os.path.exists(path):
        os.mkdir(path)
    os.chdir(path)

    with open('weather_new.csv', 'a', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        for day_count in range(count):
            print('Downloading: ' + date.strftime("%Y-%m-%dT%H:%M:%S%z"))
            url = 'https://api.darksky.net/forecast/{}/{},{},{}?exclude=currently,flags'.format(
                key, lat, lon, date.strftime("%Y-%m-%dT%H:%M:%S%z"))
            r = requests.get(url, 'json')
            for hour in scrape_day(json.loads(r.content)):
                writer.writerow(hour)
            date += timedelta(days=1)
    os.chdir('..')
    print('Finished')


def scrape_day(day):
    """
    Scrapes weather for one day and yield's information one hour at a time
    """

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
        hourly.append(ele.get('precipProbability', 0))
        hourly.append(ele.get('apparentTemperature', 0))
        hourly.append(ele.get('humidity', 0))
        hourly.append(ele.get('windSpeed', 0))
        yield hourly


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape Citi Bike and related data')
    parser.add_argument('data', help='Data to download. One of: "ride", "station", "weather"')
    parser.add_argument('-d', dest='date', help='Req Weather: Date to start scraping YYYY-MM-DD')
    parser.add_argument('-c', dest='count', help='Req Weather: Number of consecutive days to scrape.')
    parser.add_argument('-k', dest='key', help='Req Weather: API Key')

    results = parser.parse_args()
    if results.data == 'ride':
        scrape_ride_data()
    elif results.data == 'station':
        scrape_station_data()
    elif results.data == 'weather':
        if results.date is None or results.count is None or results.key is None:
            raise ValueError('Must include date, count and key to scrape weather')
        date = datetime.strptime(str(results.date), '%Y-%m-%d').replace(tzinfo=tz.gettz('America/New_York'))
        count = int(results.count)
        key = results.key
        scrape_weather(date=date, count=count, key=key)
    else:
        raise ValueError('Must be "ride", "station" or "weather"')
