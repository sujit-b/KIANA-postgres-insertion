import psycopg2
import csv
import re
import datetime
import time
# Connect to the Postgres database
conn = psycopg2.connect(
    host="localhost",
    database="kiana",
    user="postgres",
    password="GETIN@postgres1209"
)

# cursor object to execute SQL queries
cur = conn.cursor()


with open('output.csv', 'r') as file:
    reader = csv.reader(file)
    row_counter = 0
    rows_to_insert = []
    for row in reader:
        row[1] = re.sub(r'\W+', '', row[1]) # remove non alphanumeric characters
        row.append(datetime.datetime.now())
        rows_to_insert.append(row)
        row_counter += 1
        if row_counter % 80 == 0:
            sql = "INSERT INTO Stimuli_devices (Mac_id, floor_level, Lat, Lng, state, time_stamp) VALUES (%s, %s, %s, %s, %s, %s)"
            for row in rows_to_insert:
                cur.execute(sql, row)
            conn.commit()
            rows_to_insert = []
            time.sleep(15)
    if rows_to_insert:
        sql = "INSERT INTO Stimuli_devices (Mac_id, floor_level, Lat, Lng, state, time_stamp) VALUES (%s, %s, %s, %s, %s, %s)"
        for row in rows_to_insert:
            cur.execute(sql, row)
            conn.commit()
conn.close()