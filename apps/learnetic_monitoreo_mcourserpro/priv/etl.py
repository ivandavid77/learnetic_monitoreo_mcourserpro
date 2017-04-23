# -*- coding: utf-8 -*-
import pymysql.cursors
import yaml

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
    with conn.cursor() as cursor:
        cursor.execute((
            'SELECT '
            'ddd.username,'
            'ddd.unidad_1_reflexion_inicial,'
            'ddd.unidad_1_ejercicio_1,'
            'ddd.unidad_1_ejercicio_6,'
            'ddd.unidad_1_ejercicio_7,'
            'ddd.unidad_1_evaluacion_final,'
            'ddd.unidad_1_foro_de_discusion,'
            'ddd.unidad_2_ejercicio_6,'
            'ddd.unidad_2_ejercicio_7,'
            'ddd.unidad_2_ejercicio_9,'
            'ddd.unidad_2_evaluacion_final,'
            'ddd.unidad_2_foro_de_discusion,'
            'ddd.unidad_3_ejercicio_2,'
            'ddd.unidad_3_ejercicio_4,'
            'ddd.unidad_3_ejercicio_5,'
            'ddd.unidad_3_evaluacion_final,'
            'ddd.unidad_3_foro_de_discusion '
            'FROM durango_datos_drive AS ddd '
            'INNER JOIN durango_alumnos AS da ON da.username = ddd.username'))
        results = cursor.fetchall()
        for row in results:
            username = row['username']
            calificaciones = {
                'unidad_1': (row['unidad_1_reflexion_inicial'] +
                             row['unidad_1_ejercicio_1'] +
                             row['unidad_1_ejercicio_6']  +
                             row['unidad_1_evaluacion_final'])/4*0.4 +
                            row['unidad_1_ejercicio_7']*0.3 +
                            row['unidad_1_foro_de_discusion']*0.2,
                'unidad_2': (row['unidad_2_ejercicio_6'] +
                             row['unidad_2_ejercicio_9'] +
                             row['unidad_2_evaluacion_final'])/3*0.4 +
                            row['unidad_2_ejercicio_7']*0.3 +
                            row['unidad_2_foro_de_discusion']*0.2,
                'unidad_3': row['unidad_3_evaluacion_final']*0.4 +
                            (row['unidad_3_ejercicio_2'] +
                             row['unidad_3_ejercicio_4'] +
                             row['unidad_3_ejercicio_5'])/3*0.3 +
                            row['unidad_3_foro_de_discusion']*0.2
            }
            tiempos = {
                'unidad_1': 0,
                'unidad_2': 0,
                'unidad_3': 0
            }
            equivocaciones = {
                'unidad_1': 0,
                'unidad_2': 0,
                'unidad_3': 0
            }
            cursor.execute((
                'SELECT '
                'unit,'
                'exercise,'
                'score,'
                'total_time,'
                'mistake_count '
                'FROM durango_datos_bigquery '
                'WHERE username=%s'),(row['username'],))
            exercises = cursor.fetchall()
            tmp_unidad_1 = 0
            tmp_unidad_2 = 0
            tmp_unidad_3 = 0
            for ex in exercises:
                if ex['unit'] == u'unidad 1':
                    tiempos['unidad_1'] += ex['total_time']
                    equivocaciones['unidad_1'] += ex['mistake_count']
                    if ex['exercise'] in [u'ejercicio 2', u'ejercicio 3', u'ejercicio 5a', u'ejercicio 5b', u'ejercicio 5c']:
                        tmp_unidad_1 += ex['score']*10
                elif ex['unit'] == u'unidad 2':
                    tiempos['unidad_2'] += ex['total_time']
                    equivocaciones['unidad_2'] += ex['mistake_count']
                    if ex['exercise'] in [u'ejercicio 3', u'ejercicio 4', u'ejercicio 5']:
                        tmp_unidad_2 += ex['score']*10
                elif ex['unit'] == u'unidad 3':
                    tiempos['unidad_3'] += ex['total_time']
                    equivocaciones['unidad_3'] += ex['mistake_count']
                    if ex['exercise'] in [u'ejercicio 1', u'ejercicio 3']:
                        tmp_unidad_3 += ex['score']*10
            calificaciones['unidad_1'] += tmp_unidad_1/5*0.1
            calificaciones['unidad_2'] += tmp_unidad_2/3*0.1
            calificaciones['unidad_3'] += tmp_unidad_3/2*0.1

            conn_insercion = get_connection(config)
            with conn_insercion.cursor() as cursor_insercion:
                for unit in ['unidad_1', 'unidad_2', 'unidad_3']:
                    cursor_insercion.execute('SELECT 1 FROM durango_calificaciones_equivocaciones WHERE username=%s AND unidad=%s', (username,unit))
                    if cursor_insercion.rowcount == 0:
                        cursor_insercion.execute((
                            'INSERT INTO durango_calificaciones_equivocaciones SET '
                            'username=%s,'
                            'unidad=%s,'
                            'calificacion=%s,'
                            'equivocaciones=%s,'
                            'horas_requeridas=%s'),
                            (
                                username,
                                unit,
                                calificaciones[unit],
                                equivocaciones[unit],
                                tiempos[unit]/1000/3600
                            )
                        )
                    else:
                        cursor_insercion.execute((
                            'UPDATE durango_calificaciones_equivocaciones SET '
                            'calificacion=%s,'
                            'equivocaciones=%s,'
                            'horas_requeridas=%s '
                            'WHERE username=%s AND unidad=%s'),
                            (
                                calificaciones[unit],
                                equivocaciones[unit],
                                tiempos[unit]/1000/3600,
                                username,
                                unit
                            )
                        )
            conn_insercion.commit()
            conn_insercion.close()
        
        