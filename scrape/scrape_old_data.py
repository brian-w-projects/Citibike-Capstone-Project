import os
import boto3
from botocore import UNSIGNED
from botocore.client import Config
import zipfile
import requests
import json
import csv


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
