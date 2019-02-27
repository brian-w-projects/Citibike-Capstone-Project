import os
import requests
import json
from datetime import datetime, timedelta
from dateutil import tz
from google.cloud import pubsub_v1
import sqlalchemy


def stream_data(working_time=datetime(2018, 1, 2, 8, 10, 0)):
    publisher = pubsub_v1.PublisherClient()
    topic_name = 'projects/{project_id}/topics/{topic}'.format(
                    project_id='capstone-231016',
                    topic='rides',
    )

    cloud_sql_connection_name = 'capstone-231016:us-west2:streaming-data'

    db = sqlalchemy.create_engine(
        sqlalchemy.engine.url.URL(
            drivername='mysql+pymysql',
            username=os.environ.get('USERNAME'),
            password=os.environ.get('PASSWORD'),
            database=os.environ.get('NAME'),
            query={
                'unix_socket': '/cloudsql/{}'.format(cloud_sql_connection_name)
            }
        )
    )

    # db = sqlalchemy.create_engine(
    #     sqlalchemy.engine.url.URL(
    #         drivername='mysql+pymysql',
    #         username='root',
    #         password='cosmic joke',
    #         database='citibike'
    #     )
    # )

    with db.connect() as conn:
        value = conn.execute("SELECT * FROM data WHERE starttime BETWEEN '{}' AND '{}'".format(working_time - timedelta(minutes=5), working_time)).fetchall()
        for val in value:
            send = ','.join(map(str, list(val))).encode('utf-8')
            publisher.publish(topic_name, send)


def scrape_weather(date=datetime(2013, 1, 1, 0, 0, 0, tzinfo=tz.gettz('America/New_York')), key=None):
    """
    Scrapes Weather From API
    """

    path = os.path.join(os.getcwd(), 'weather')
    lat = '40.730610'
    lon = '-73.935242'

    publisher = pubsub_v1.PublisherClient()
    topic_name = 'projects/{project_id}/topics/{topic}'.format(
        project_id='capstone-231016',
        topic='weather',
    )

    if not os.path.exists(path):
        os.mkdir(path)
    os.chdir(path)

    url = 'https://api.darksky.net/forecast/{}/{},{},{}?exclude=currently,flags'.format(
        key, lat, lon, date.strftime("%Y-%m-%dT%H:%M:%S%z"))
    r = requests.get(url, 'json')
    for hour in scrape_day(json.loads(r.content)):
        send = ','.join(hour).encode('utf-8')
        publisher.publish(topic_name, send)

    os.chdir('..')


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
            hourly.append('1')
        else:
            hourly.append('0')
        hourly.append(str(ele['precipProbability']))
        hourly.append(str(ele['apparentTemperature']))
        hourly.append(str(ele['humidity']))
        hourly.append(str(ele['windSpeed']))
        yield hourly
