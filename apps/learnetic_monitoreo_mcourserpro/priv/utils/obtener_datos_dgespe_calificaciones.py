# -*- coding: utf-8 -*-
import sys
import csv
import datetime
import yaml
import pymysql.cursors

def get_config():
    with open('../config/database.yaml') as f:
        return yaml.load(f.read())

def get_connection(config):
    db = config['db']
    return pymysql.connect(db=db['name'],
                           user=db['user'],
                           password=db['password'],
                           host=db['host'],
                           charset='utf8mb4')
                           #cursorclass=pymysql.cursors.DictCursor)

with open('dgespe_calificaciones.csv', 'wb') as f:
    writer = csv.writer(f)
    writer.writerow(('username', 'unidad', 'calificacion'))
    config = get_config()
    conn = get_connection(config)
    with conn.cursor() as cursor:
        cursor.execute((
            'SELECT '
            'username,'
            'unidad,'
            'calificacion '
            'FROM dgespe_calificaciones'
        ))
        writer.writerows(cursor.fetchall())
        