# -*- coding: utf-8 -*-
import sys
import csv
import yaml
import pymysql.cursors


def get_config():
    with open('config/database.yaml') as f:
        return yaml.load(f.read())

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
    with open('alumnos.csv', 'rb') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            username = row[0].strip() # username
            print(username)
            params = []
            params.append(username)
            params.append((row[1] + ' ' + row[2] + ' ' + row[3] + ' ' + row[4]).upper().strip()) # participante
            params.append(row[5].upper().strip()) # genero
            fecha_nacimiento = row[6].strip()
            if fecha_nacimiento == '':
                params.append('0000-00-00') 
            else:
                params.append(fecha_nacimiento)
            params.append(row[7]) # correo_electronico
            params.append(row[8].upper().strip()) # escuela_normal
            params.append(row[9].upper()) # semestre
            params.append(row[10].upper()) # clase
            params.append(row[11].upper().strip()) # tutor
            params.append(row[12].upper().strip()) # rol
            params.append(row[13]) # latitud
            params.append(row[14]) # longitud
            params.append(row[15].capitalize()) # municipio
            params.append(row[16].capitalize()) # estado
            params.append(row[17].upper()) # pais
            with conn.cursor() as cursor:
                cursor.execute('DELETE FROM durango_alumnos WHERE username=%s LIMIT 1',(username,))
                cursor.execute((
                    'INSERT INTO durango_alumnos SET '
                    'username=%s,'
                    'participante=%s,'
                    'genero=%s,'
                    'fecha_nacimiento=%s,'
                    'correo_electronico=%s,'
                    'escuela_normal=%s,'
                    'semestre=%s,'
                    'clase=%s,'
                    'tutor=%s,'
                    'rol=%s,'
                    'latitud=%s,'
                    'longitud=%s,'
                    'municipio=%s,'
                    'estado=%s,'
                    'pais=%s'), tuple(params))
    conn.commit()
    conn.close()
