import mysql.connector
import csv
import os
import argparse


def load_data(password=None):
    mydb = mysql.connector.connect(
        host='localhost',
        user='root',
        password=password,
    )

    mycursor = mydb.cursor()

    mycursor.execute("DROP DATABASE IF EXISTS citibike")
    mycursor.execute("CREATE DATABASE IF NOT EXISTS citibike")
    mycursor.execute("USE citibike")
    mycursor.execute("SET FOREIGN_KEY_CHECKS=0")

    mycursor.execute("CREATE TABLE IF NOT EXISTS rides ("
                     "id INT AUTO_INCREMENT, "
                     "start DATETIME NOT NULL, "
                     "end DATETIME NOT NULL, "
                     "start_station INT NOT NULL, "
                     "end_station INT NOT NULL, "
                     "bikeid INT NOT NULL, "
                     "usertype VARCHAR(100) NOT NULL, "
                     "birthyear INT, "
                     "gender INT NOT NULL, "
                     "PRIMARY KEY (id), "
                     "FOREIGN KEY (start_station) REFERENCES stations(station_id), "
                     "FOREIGN KEY (end_station) REFERENCES stations(station_id) "
                     ");"
                     )

    mycursor.execute("CREATE TABLE IF NOT EXISTS stations ("
                     "id INT AUTO_INCREMENT, "
                     "station_id INT NOT NULL UNIQUE, "
                     "lat FLOAT NOT NULL, "
                     "lon FLOAT NOT NULL, "
                     "name VARCHAR(255) NOT NULL, "
                     "active INT NOT NULL, "
                     "PRIMARY KEY (id) "
                     ");"
                     )

    rides_statement = "INSERT INTO rides (start, end, start_station, end_station, bikeid, usertype, birthyear, gender) " \
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"

    station_statement = "INSERT IGNORE INTO stations (station_id, lat, lon, name, active) VALUES (%s, %s, %s, %s, %s);"

    path = os.path.join(os.getcwd(), 'stations')
    for file in os.listdir(path):
        with open(os.path.join(path, file), 'r') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                row.append(1)
                mycursor.execute(station_statement, row)
        mydb.commit()
    print('Stations Finished')

    mycursor.execute("SELECT DISTINCT(station_id) FROM stations")
    active_stations = list(mycursor.fetchall())

    print('Starting Rides')
    path = os.path.join(os.getcwd(), 'raw-data')
    for file in os.listdir(path):
        with open(os.path.join(path, file), 'r') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)
            for row in reader:
                ride_query = [row[i] if row[i] != '\\N' else None for i in [1, 2, 3, 7, 11, 12, 13, 14]]
                mycursor.execute(rides_statement, ride_query)
                if row[3] not in active_stations:
                    station_query = [row[i] for i in [3, 5, 6, 4]]
                    station_query.append(0)
                    mycursor.execute(station_statement, station_query)
                if row[7] not in active_stations:
                    station_query = [row[i] for i in [7, 9, 10, 8]]
                    station_query.append(0)
                    mycursor.execute(station_statement, station_query)
        mydb.commit()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Load scraped data into local database for testing')
    parser.add_argument('password', help='Password for localhost')

    results = parser.parse_args()
    load_data(results.password)
