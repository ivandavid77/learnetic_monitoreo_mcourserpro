# -*- coding: utf-8 -*-
import httplib2
import os
import io
import yaml
import pymysql.cursors
from googleapiclient.http import MediaIoBaseDownload
from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

#https://developers.google.com/resources/api-libraries/documentation/sheets/v4/python/latest/sheets_v4.spreadsheets.html
#https://developers.google.com/resources/api-libraries/documentation/drive/v3/python/latest/drive_v3.files.html#list

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/drive_and_sheets.json'
SCOPES = ['https://www.googleapis.com/auth/drive.metadata',
          'https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/drive.file',
          'https://www.googleapis.com/auth/spreadsheets']
CLIENT_SECRET_FILE = './config/client_secret.json'
APPLICATION_NAME = 'DurangoDrive'

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

def get_credentials():
    """Gets valid user credentials from storage.
    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.
    Returns:
        Credentials, the obtained credential.
    """
    credential_dir = './config'
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir, 'drive_and_sheets.json')
    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials

def get_row(elem, row):
    if len(row) > (elem + 1):
        try:
            if type(row[elem]) != str:
                return float(row[elem])
            else:
                return float(row[elem].strip())
        except:
            return 0.0
    else:
        return 0.0

def main():
    db = []
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    sheets_service = discovery.build('sheets', 'v4', http=http, discoveryServiceUrl='https://sheets.googleapis.com/$discovery/rest?version=v4')
    drive_service = discovery.build('drive', 'v3', http=http)
    results = drive_service.files().list(q='trashed=false', fields="files(id, name, mimeType, size)").execute()
    for item in results.get('files', []):
        name = item['name'].encode('utf-8')
        if name.startswith('DUR') or name.startswith('JAL'):
            if item['mimeType'] == 'application/vnd.google-apps.spreadsheet':
                result = sheets_service.spreadsheets().values().get(spreadsheetId=item['id'], range='C:BN').execute()
                values = result.get('values', [])
                for row in values:
                    if row:
                        if len(row[0]) > 8 and (row[0].startswith('DUR') or row[0].startswith('JAL')):
                            db.append({
                                'username': row[0].strip(),
                                'unidad_1_reflexion_inicial': get_row(1, row),
                                'unidad_1_ejercicio_1': get_row(2, row),
                                'unidad_1_ejercicio_6': get_row(9, row),
                                'unidad_1_ejercicio_7': get_row(10, row),
                                'unidad_1_evaluacion_final': get_row(11, row),
                                'unidad_1_foro_de_discusion': get_row(12, row),
                                'unidad_2_ejercicio_6': get_row(26, row),
                                'unidad_2_ejercicio_7': get_row(27, row),
                                'unidad_2_ejercicio_9': get_row(29, row),
                                'unidad_2_evaluacion_final': get_row(30, row),
                                'unidad_2_foro_de_discusion': get_row(31, row),
                                'unidad_3_ejercicio_2': get_row(41, row),
                                'unidad_3_ejercicio_4': get_row(43, row),
                                'unidad_3_ejercicio_5': get_row(44, row),
                                'unidad_3_evaluacion_final': get_row(45, row),
                                'unidad_3_foro_de_discusion': get_row(46, row),
                            })
    config = get_config()
    conn = get_connection(config)
    with conn.cursor() as cursor:
        for rec in db:
            cursor.execute(('SELECT 1 FROM durango_datos_drive '
                            'WHERE username=%s'),
                            (rec['username'],))
            operation = ''
            where = ''
            operation_params = []
            params = [
                rec['unidad_1_reflexion_inicial'],
                rec['unidad_1_ejercicio_1'],
                rec['unidad_1_ejercicio_6'],
                rec['unidad_1_ejercicio_7'],
                rec['unidad_1_evaluacion_final'],
                rec['unidad_1_foro_de_discusion'],
                rec['unidad_2_ejercicio_6'],
                rec['unidad_2_ejercicio_7'],
                rec['unidad_2_ejercicio_9'],
                rec['unidad_2_evaluacion_final'],
                rec['unidad_2_foro_de_discusion'],
                rec['unidad_3_ejercicio_2'],
                rec['unidad_3_ejercicio_4'],
                rec['unidad_3_ejercicio_5'],
                rec['unidad_3_evaluacion_final'],
                rec['unidad_3_foro_de_discusion']
            ]
            if cursor.rowcount == 0:
                # Actualización
                operation = 'INSERT INTO durango_datos_drive SET username=%s,'
                params = [rec['username']] + params
            else:
                # Inserción
                operation = 'UPDATE durango_datos_drive SET '
                where = ' WHERE username=%s'
                params = params + [rec['username']]
            sql = operation+(
                'unidad_1_reflexion_inicial=%s,'
                'unidad_1_ejercicio_1=%s,'
                'unidad_1_ejercicio_6=%s,'
                'unidad_1_ejercicio_7=%s,'
                'unidad_1_evaluacion_final=%s,'
                'unidad_1_foro_de_discusion=%s,'
                'unidad_2_ejercicio_6=%s,'
                'unidad_2_ejercicio_7=%s,'
                'unidad_2_ejercicio_9=%s,'
                'unidad_2_evaluacion_final=%s,'
                'unidad_2_foro_de_discusion=%s,'
                'unidad_3_ejercicio_2=%s,'
                'unidad_3_ejercicio_4=%s,'
                'unidad_3_ejercicio_5=%s,'
                'unidad_3_evaluacion_final=%s,'
                'unidad_3_foro_de_discusion=%s')+where
            cursor.execute(sql, tuple(params))
    conn.commit()
    conn.close()

if __name__ == '__main__':
    main()