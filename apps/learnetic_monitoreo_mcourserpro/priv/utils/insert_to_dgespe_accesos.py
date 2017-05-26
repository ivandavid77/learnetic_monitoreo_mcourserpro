# -*- coding: utf-8 -*-
import sys
import csv
import datetime
import random
import pymysql.cursors
import yaml

HEADER = ['random_event_id','created_date','event_type','session_type','user_id',
                             'username','firstname','lastname','user_role','user_school_id','user_school_name',
                             'lesson_id','lesson_title','lesson_type','course_id','course_title','course_lessons_count',
                             'course_ebooks_count','chapter_id','chapter_title','assignment_id','group_assignment_id',
                             'assignment_grade','assignment_state','assignment_due_date','score','errors_count',
                             'checks_count','mistake_count','session_duration','request_country_code','request_region',
                             'request_city','request_citylatlon','user_agent','mlibro_system_version','mlibro_version',
                             'mlibro_type','mlibro_guid','mlibro_language','user_email','user_first_name_adult',
                             'user_last_name_adult','user_email_adult','user_age_type','user_regulation_agreement',
                             'user_regulation_marketing','user_regulation_information','user_school_national_id',
                             'user_school_type','user_school_city','user_school_zip_code','user_school_province',
                             'user_school_country','user_school_email',
                         'tipo_dispositivo', 'sistema_operativo', 'modo', 'plataforma']

def get_config():
    with open('../config/database.yaml') as f:
        return yaml.load(f.read())

def get_connection(config):
    db = config['db']
    return pymysql.connect(db=db['name'],
                           user=db['user'],
                           password=db['password'],
                           host=db['host'],
                           charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)

def get_tipo_dispositivo(tipo_dispositivo):
    tipo_dispositivo = tipo_dispositivo.strip().lower()
    if tipo_dispositivo[:1] in ('p', ''):
        return 'laptop_pc'
    elif tipo_dispositivo[:1] == 't':
        return 'tableta'
    elif tipo_dispositivo[:1] == 'c':
        return 'celular'
    else:
        return 'laptop_pc'
    
def get_sistema_operativo(sistema_operativo):
    sistema_operativo = sistema_operativo.strip().lower()
    if sistema_operativo[:1] in ('w',''):
        return 'windows'
    elif sistema_operativo[:1] in ('i','m'):
        return 'ios'
    elif sistema_operativo[:1] == 'a':
        return 'android'
    else:
        return 'windows'
    
def get_modo(modo):
    modo = modo.strip().lower()
    if modo[:2] in ('of',''):
        return 'offline'
    elif modo[:2] == 'on':
        return 'online'
    else:
        return 'offline'

def get_plataforma(plataforma):
    plataforma = plataforma.strip().lower()
    if plataforma[:2] in ('ml', ''):
        return 'mlibro'
    elif plataforma[:2] == 'mc':
        return 'mcourserpro'
    else:
        return 'mlibro'

db = {}
with open('datos_jose.csv', 'rb') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)
    for row in reader:
        if row[0].strip() == '':
            continue
        if len(row) < len(HEADER):
            row = row + ['']*(len(HEADER)-len(row))
        record = dict(zip(HEADER,row))
        if record['event_type'] in ('score_sync','session_end'):
            unidad = record['lesson_title'][:8].lower()
            if unidad in ('unidad 1','unidad 2','unidad 3'):
                session_duration = record['session_duration']
                if session_duration == '':
                    continue
                session_duration = int(session_duration)
                if session_duration == 0:
                    continue
                username = record['username']
                tipo_dispositivo = get_tipo_dispositivo(record['tipo_dispositivo'])
                sistema_operativo = get_sistema_operativo(record['sistema_operativo'])
                modo = get_modo(record['modo'])
                plataforma = get_plataforma(record['plataforma'])
                key = username + '-' + unidad + '-' + tipo_dispositivo + '-' + sistema_operativo + '-' + modo + '-' + plataforma
                if key in db:
                    db[key] += session_duration
                else:
                    db[key] = session_duration
config = get_config()
conn = get_connection(config)
with conn.cursor() as cursor:
    cursor.execute('DELETE FROM dgespe_accesos')
    conn.commit()
for key in db:
    with conn.cursor() as cursor:
        username, unidad, tipo_dispositivo, sistema_operativo, modo, plataforma = key.split('-')
        horas_invertidas = db[key]/1000.0/3600.0            
        cursor.execute((
            'INSERT INTO dgespe_accesos SET '
            'username=%s,'
            'unidad=%s,'
            'tipo_dispositivo=%s,'
            'sistema_operativo=%s,'
            'modo=%s,'
            'plataforma=%s,'
            'horas_invertidas=%s'), (
                username,
                unidad,
                tipo_dispositivo,
                sistema_operativo,
                modo,
                plataforma,
                horas_invertidas))
        conn.commit()            
conn.close()
       