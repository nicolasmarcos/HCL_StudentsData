# Import necessary libraries
import sys
import os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
sys.path.append("HCL_StudentsData")
from airflow.models import DAG
from datetime import timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from src.tasks.carga_finalmerged import carga_finalmerged

# It defines dag and arguments
default_args = {
    'owner': 'airflow'
    ,'start_date': days_ago(1)
    ,'depends_on_past': False
    ,'retries': 1
    ,'retry_delay': timedelta(minutes=5)
}

# Instantiate the dag
with DAG(
    default_args=default_args
    ,dag_id="cargaFinalMerged"
    ,description='ETL de arquivos json students e missed_days e carga no Snowflake'
    ,catchup=False
) as dag:

    # Defines the base filesystem of the project
    # This variable could be easily changed to get the file from another path in the network, or get its value from an environment variable in airflow. Here, it assumes that the files are in a relative folder of the dag.
    base_path = '/'
    # Define paths 
    relative_path_input_students = os.path.join(base_path,'datalake', 'raw','students.json')
    relative_path_input_misseddays = os.path.join(base_path,'datalake', 'raw','missed_days.json')

    # Defines dag tasks
    t_start = DummyOperator(task_id='t_start')
    t_carga_finalmerged = carga_finalmerged(filepath_input_students=relative_path_input_students, filepath_input_misseddays=relative_path_input_misseddays)
    
    t_start >> t_carga_finalmerged

