# -*- coding: utf-8 -*-
import sys
import os
import struct
import yaml
import pymysql.cursors
import erlang


def get_config():
    with open('config/database.yaml') as f:
        return yaml.load(f.read())

def send(term, stream):
    """Write an Erlang term to an output stream."""
    payload = erlang.term_to_binary(term)
    header = struct.pack('!I', len(payload))
    stream.write(header)
    stream.write(payload)
    stream.flush()

def get_connection(config):
    db = config['db']
    return pymysql.connect(db=db['name'],
                           user=db['user'],
                           password=db['password'],
                           host=db['host'],
                           charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)

if __name__ == '__main__':
    config = get_config()
    conn = get_connection(config)
    output = os.fdopen(4, 'wb')
    with conn.cursor() as cursor:
        cursor.execute('SELECT username FROM durango_alumnos WHERE username NOT LIKE "multigrado%" LIMIT 2')
        results = cursor.fetchall()
        for row in results:
            send(row['username'], output)
    conn.close()
