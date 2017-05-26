# -*- coding: utf-8 -*-
import sys
import csv
import datetime
from google.cloud import bigquery

if __name__ == '__main__':
    client = bigquery.Client()
    query = (
        'SELECT * '
        'FROM `mcourser-mexico-he.events.events2017*` '
        'WHERE SUBSTR(username,0,3) IN ("DUR","JAL")'
        'AND LENGTH(username) > 9')
    query_results = client.run_sync_query(query)
    query_results.use_legacy_sql = False
    query_results.run()
    if query_results.complete:
        with open('datos_bigquery.csv', 'wb') as csvfile:
            writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
            writer.writerow(['random_event_id','created_date','event_type','session_type','user_id',
                             'username','firstname','lastname','user_role','user_school_id','user_school_name',
                             'lesson_id','lesson_title','lesson_type','course_id','course_title','course_lessons_count',
                             'course_ebooks_count','chapter_id','chapter_title','assignment_id','group_assignment_id',
                             'assignment_grade','assignment_state','assignment_due_date','score','errors_count',
                             'checks_count','mistake_count','session_duration','request_country_code','request_region',
                             'request_city','request_citylatlon','user_agent','mlibro_system_version','mlibro_version',
                             'mlibro_type','mlibro_GUID','mlibro_language','user_email','user_first_name_adult',
                             'user_last_name_adult','user_email_adult','user_age_type','user_regulation_agreement',
                             'user_regulation_marketing','user_regulation_information','user_school_national_id',
                             'user_school_type','user_school_city','user_school_zip_code','user_school_province',
                             'user_school_country','user_school_email'])
            for row in query_results.rows:
                result = []
                for elem in row:
                    if type(elem) == unicode:
                        result.append(elem.encode('utf-8'))
                    elif type(elem) == datetime.datetime:
                        result.append(elem.strftime('%Y-%m-%d %H:%M:%S UTC'))
                    elif type(elem)  == int:
                        result.append(elem)
                    elif elem == None:
                        result.append('')
                    else:
                        result.append(elem)
                writer.writerow(result)
            
    elif query_results.errors:
        print(str(query_results.errors))
        sys.exit(1)
