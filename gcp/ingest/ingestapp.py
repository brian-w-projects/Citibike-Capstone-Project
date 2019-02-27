from flask import Flask
from datetime import datetime, timedelta
import os
import ingest_to_pubsub
import pytz

app = Flask(__name__)


@app.route('/')
def main():
    return "<html><h1>WELCOME</h1></html>"


@app.route('/stream-data')
def stream_data():
    date = datetime.now(pytz.timezone("America/New_York")) - timedelta(days=365)
    ingest_to_pubsub.stream_data(date)
    return '200'


@app.route('/stream-weather')
def stream_weather():
    date = datetime.now(pytz.timezone("America/New_York")) - timedelta(days=364)
    ingest_to_pubsub.scrape_weather(date, key=os.environ['KEY'])
    return '200'
