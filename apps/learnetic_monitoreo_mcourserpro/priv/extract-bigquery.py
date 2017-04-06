#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import MySQLdb
from google.cloud import bigquery

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
    'page_score__absolute_score': 0,
    'page_score__max_score': 0,
    'page_score__score': 0,
    'page_score__total_time': 0}

def initialize_units(username, db, index):
  # UNIDAD 1: Educación multigrado
  create_unit('UNIDAD 1', ['Ejercicio 2', 'Ejercicio 3', 'Ejercicio 5a', 'Ejercicio 5b', 'Ejercicio 5c'], username, db, index)
  # UNIDAD 2: Educación multigrado
  create_unit('UNIDAD 2', ['Ejercicio 1', 'Ejercicio 2', 'Ejercicio 3', 'Ejercicio 4', 'Ejercicio 5'], username, db, index)
  # UNIDAD 3: Educación multigrado
  create_unit('UNIDAD 3', ['Ejercicio 1', 'Ejercicio 3'], username, db, index)

# Inicializacion
username = sys.argv[1] # demoalumno2
db = []
index = {}
initialize_units(username, db, index)

# Obtener los datos almacenados en la base de datos y modificar las bases de datos actuales en memoria
conn = MySQLdb.connect(db="learnetic", user="tableau", passwd="123456", host="richit.ai")
cursor = conn.cursor()
cursor.execute("""
SELECT
unit,
exercise,
modified_date,
score,
total_time,
page_score__absolute_score,
page_score__max_score,
page_score__score,
page_score__total_time
FROM durango_datos_bigquery
WHERE username = "{}"
""".format(username))
results = cursor.fetchall()
for row in results:  
  record = index[row[0]+row[1]]
  record['modified_date'] = row[2]
  record['score'] = row[3]
  record['total_time'] = row[4]
  record['page_score__absolute_score'] = row[5]
  record['page_score__max_score'] = row[6]
  record['page_score__score'] = row[7]
  record['page_score__total_time'] = row[8]
conn.close()

# Obtener los ejercicios que se califican en automático y actualizar cuando sea necesario las db en memoria
client = bigquery.Client()
query = """
SELECT
modified_date,
score,
total_time,
lesson_title,
page_score
FROM `mcourser-mexico-he.scores.scores_2017*`
WHERE username = "{}"
AND ARRAY_LENGTH(page_score) > 0
AND lesson_title IN (
"UNIDAD 1: Educación multigrado", 
"UNIDAD 2: Educación multigrado",
"UNIDAD 3: Educación multigrado")
""".format(username)
query_results = client.run_sync_query(query)
query_results.use_legacy_sql = False
query_results.run()
if query_results.complete:
  for row in query_results.rows:    
    modified_date = row[0].strftime('%Y-%m-%d %H:%M:%S UTC')
    score = row[1]
    total_time = row[2]
    lesson_title = row[3]
    page_score = row[4]
    unit_name = lesson_title.split(':')[0]
    for ps in page_score:
      exercise = (ps['page_name']).strip()
      if modified_date > record['modified_date']:
        record = index[unit_name+exercise]
        record['modified_date'] = modified_date
        record['score'] = score
        record['total_time'] = total_time
        record['page_score__absolute_score'] = ps['absolute_score']
        record['page_score__max_score'] = ps['max_score']
        record['page_score__score'] = ps['score']
        record['page_score__total_time'] = ps['total_time']
elif query_results.errors:
  print(str(query_results.errors))
  sys.exit(1)

# Actualizar la informacion en la base de datos
conn = MySQLdb.connect(db="learnetic", user="tableau", passwd="123456", host="richit.ai")
cursor = conn.cursor()
for rec in db:
  cursor.execute("""
SELECT 1
FROM durango_datos_bigquery
WHERE username="{}" AND unit="{}" AND exercise="{}"
""".format(rec['username'],rec['unit'],rec['exercise']))
  operation = ''
  where = ''
  if cursor.rowcount == 0:
    # Actualización
    operation = 'INSERT INTO durango_datos_bigquery SET username="{}",unit="{}",exercise="{}",'.format(rec['username'],rec['unit'],rec['exercise'])
  else:
    # Inserción
    operation = 'UPDATE durango_datos_bigquery SET '
    where = ' WHERE username="{}" AND unit="{}" AND exercise="{}"'.format(rec['username'],rec['unit'],rec['exercise'])
  
  sql = (operation+"""
modified_date="{}",
score="{}",
total_time="{}",
page_score__absolute_score="{}",
page_score__max_score="{}",
page_score__score="{}",
page_score__total_time="{}"
"""+where).format(rec['modified_date'],
                  rec['score'],
                  rec['total_time'],
                  rec['page_score__absolute_score'],
                  rec['page_score__max_score'],
                  rec['page_score__score'],
                  rec['page_score__total_time'])
  cursor.execute(sql)
conn.commit()
conn.close()

