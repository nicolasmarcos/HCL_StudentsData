# Install necessary libraries
#pip install "snowflake-connector-python[pandas]"
#pip install snowflake-connector-python

# Importa bibliotecas necessárias
from airflow.decorators import task
import pandas as pd
import json
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector
from pandas import json_normalize
import os
import datetime

# Function that loads the data
@task(task_id="t_carga_finalmerged", trigger_rule='all_success')
def carga_finalmerged(filepath_input_students, filepath_input_misseddays):
    # Gets both json files and prepare their dataframes
    f_students = open(filepath_input_students)
    data_students = json.load(f_students)
    df_students = json_normalize(data_students['students'])
    df_students.rename(columns={"grades.math" : "grade_math",	"grades.science": "grade_science", 	"grades.history" : "grade_history",	"grades.english" : "grade_english"}, inplace=True)
    f_misseddays = open(filepath_input_misseddays)
    data_misseddays = json.load(f_misseddays)
    df_misseddays = json_normalize(data_misseddays['missed_classes'])

    # Join files in final dataframe
    df_final = pd.merge(df_students, df_misseddays, how='left', on='student_id')
    df_final.columns = df_final.columns.str.upper()

    # Gets current datetime to store in log columns in snowflake
    ct = datetime.datetime.now()
    df_final['DT_INS'] = ct
    df_final['DT_INS'] = df_final['DT_INS'].dt.tz_localize('UTC')
    df_final['DT_UPDT'] = pd.to_datetime("")
    df_final['DT_UPDT'] = df_final['DT_UPDT'].dt.tz_localize('UTC',nonexistent='NaT')
    df_final['DT_INACT'] = pd.to_datetime("")
    df_final['DT_INACT'] = df_final['DT_INACT'].dt.tz_localize('UTC',nonexistent='NaT')
    
    # Instatiate the connection to snowflake. You should put here your credentials:
    conn = snowflake.connector.connect(
    user='',
    password='',
    url='',
    account='',
    warehouse='STUDENTS_DB',
    database='students_merged_db',
    schema='students_staging',
    role='ACCOUNTADMIN'
    )

    write_pandas(conn,df_final,table_name='FINAL_MERGED',overwrite=True,auto_create_table=False)
