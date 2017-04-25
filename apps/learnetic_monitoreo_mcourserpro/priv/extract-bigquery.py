# -*- coding: utf-8 -*-
import sys
import yaml
import pymysql.cursors
from google.cloud import bigquery

def get_config():
    with open('config/database.yaml') as f:
        return yaml.load(f.read())

def create_unit(unit_name, exercises, username, db, index):
    for exercise in exercises:
        record = create_record(username, unit_name, exercise)
        db.append(record)
        index[unit_name+exercise] = record

def create_record(username, unit, exercise):
    return {
        'username': username,
        'unit': unit,
        'exercise': exercise,
        'modified_date': '2016-01-01 00:00:00 UTC',
        'score': 0,
        'total_time': 0,
        'mistake_count': 0
    }

def initialize_units(username, db, index):
    # UNIDAD 1: Educación multigrado
    create_unit(u'unidad 1', [u'ejercicio 2', u'ejercicio 3', u'ejercicio 5a', u'ejercicio 5b', u'ejercicio 5c',
                             u'reflexión inicial', u'ejercicio 1', u'ejercicio 6', u'ejercicio 7', u'evaluación final', u'foro de discusión'], username, db, index)
    # UNIDAD 2: Educación multigrado
    create_unit(u'unidad 2', [u'ejercicio 3', u'ejercicio 4', u'ejercicio 5',
                             u'ejercicio 6', u'ejercicio 7', u'ejercicio 9', u'evaluación final', u'foro de discusión'], username, db, index)
    # UNIDAD 3: Educación multigrado
    create_unit(u'unidad 3', [u'ejercicio 1', u'ejercicio 3',
                             u'ejercicio 2', u'ejercicio 4', u'ejercicio 5', u'evaluación final', u'foro de discusión'], username, db, index)

def get_connection(config):
    db = config['db']
    return pymysql.connect(db=db['name'],
                           user=db['user'],
                           password=db['password'],
                           host=db['host'],
                           charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)

def invalid_unit(unit_name, exercise):
    if ((unit_name == u'unidad 1' and exercise == u'ejercicio 4')
    or (unit_name == u'unidad 2' and exercise == u'ejercicio 1') 
    or (unit_name == u'unidad 2' and exercise == u'ejercicio 2')
    or (unit_name == u'unidad 2' and exercise == u'ejercicio 8')
    or (unit_name == u'unidad 3' and exercise == u'ejercicio 1')):
        True
    else:
        False

if __name__ == '__main__':
    # Inicializacion
    username = sys.argv[1] # demoalumno2
    db = []
    index = {}
    initialize_units(username, db, index)
    config = get_config()

    # Obtener los datos almacenados en la base de datos y modificar las bases de datos actuales en memoria
    conn = get_connection(config)

    with conn.cursor() as cursor:
        cursor.execute((
            'SELECT unit, exercise, modified_date, score, total_time, mistake_count '
            'FROM durango_datos_bigquery '
            'WHERE username = %s'), username)
        results = cursor.fetchall()
        for row in results:
            record = index[row['unit']+row['exercise']]
            record['modified_date'] = row['modified_date']
            record['score'] = row['score']
            record['total_time'] = row['total_time']
            record['mistake_count'] = row['mistake_count']
    conn.close()

    # Obtener los ejercicios que se califican en automático y actualizar cuando sea necesario las db en memoria
    client = bigquery.Client()
    query = (
        'SELECT modified_date, score, total_time, lesson_title, page_score, mistake_count '
        'FROM `mcourser-mexico-he.scores.scores_2017*` '
        'WHERE username = "{}"').format(username)
    query_results = client.run_sync_query(query)
    query_results.use_legacy_sql = False
    query_results.run()
    if query_results.complete:
        for row in query_results.rows:
            lesson_title = row[3]
            if lesson_title == None:
                continue
            modified_date = row[0].strftime('%Y-%m-%d %H:%M:%S UTC')
            score = row[1]
            total_time = row[2]
            page_score = row[4]
            mistake_count = row[5]
            tmp = lesson_title.split(':')
            if isinstance(tmp, list) and len(tmp) == 2:
                unit_name = tmp[0].lower().strip()
                exercise = tmp[1].lower().strip()
            else:
                tmp = lesson_title.split('.')
                unit_name = tmp[0].lower().strip()
                exercise = tmp[1].lower().strip()
            if lesson_title in [u'UNIDAD 1: Educación multigrado', u'UNIDAD 2: Educación multigrado', u'UNIDAD 3: Educación multigrado'] and len(page_score) > 0:
                for ps in page_score:
                    exercise = (ps['page_name']).lower().strip()
                    if invalid_unit(unit_name, exercise):
                        continue
                    try:
                        record = index[unit_name+exercise]
                        if modified_date > record['modified_date']:
                            record['modified_date'] = modified_date
                            record['score'] = ps['score']
                            record['total_time'] = ps['total_time']
                            record['mistake_count'] = ps['mistake_count']
                    except:
                        pass
            elif lesson_title not in [u'UNIDAD 1: Educación multigrado', u'UNIDAD 2: Educación multigrado', u'UNIDAD 3: Educación multigrado']:
                if invalid_unit(unit_name, exercise):
                    continue
                record = index[unit_name+exercise]
                if total_time == 0:
                    continue
                elif modified_date > record['modified_date']:
                    record['modified_date'] = modified_date
                    record['score'] = score
                    record['total_time'] = total_time
                    record['mistake_count'] = mistake_count
    elif query_results.errors:
        print(str(query_results.errors))
        sys.exit(1)

    # Actualizar la informacion en la base de datos
    conn = get_connection(config)
    with conn.cursor() as cursor:
        for rec in db:
            cursor.execute(('SELECT 1 FROM durango_datos_bigquery '
                            'WHERE username=%s AND unit=%s AND exercise=%s'),
                            (rec['username'], rec['unit'], rec['exercise']))
            operation = ''
            where = ''
            operation_params = []
            params = [
                rec['modified_date'],
                rec['score'],
                rec['total_time'],
                rec['mistake_count']
            ]
            if cursor.rowcount == 0:
                # Actualización
                operation = 'INSERT INTO durango_datos_bigquery SET username=%s,unit=%s,exercise=%s,'
                params = [rec['username'], rec['unit'], rec['exercise']] + params
            else:
                # Inserción
                operation = 'UPDATE durango_datos_bigquery SET '
                where = ' WHERE username=%s AND unit=%s AND exercise=%s'
                params = params + [rec['username'], rec['unit'], rec['exercise']]
            sql = operation+(
                'modified_date=%s,'
                'score=%s,'
                'total_time=%s,'
                'mistake_count=%s')+where
            cursor.execute(sql, tuple(params))
            conn.commit()
    conn.close()
