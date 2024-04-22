# import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#defining DAG arguments

default_args = {
    'owner': 'Longdvnl',
    'start_date': days_ago(0),
    'email': ['longdvnl@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# define the DAG
dag = DAG(
    dag_id='process_web_log',
    default_args=default_args,
    description='ETL & Data Pipelines using Apache Airflow',
    schedule_interval=timedelta(days=1),
)

# define the tasks

# define the task named extract_data
extract_data = BashOperator(
    task_id='extract_data',
    bash_command='cut -d" " -f1 /home/project/airflow/dags/capstone/accesslog.txt \
        > /home/project/airflow/dags/capstone/extracted_data.txt',
    dag=dag,
)

# define the task named transform_data
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='sed "/198.46.149.143/d" /home/project/airflow/dags/capstone/extracted_data.txt \
        > /home/project/airflow/dags/capstone/transformed_data.txt', 
    dag=dag,
)

# define the task named load_data
load_data = BashOperator(
    task_id='load_data',
    bash_command='tar cvf /home/project/airflow/dags/capstoneweblog.tar /home/project/airflow/dags/capstone/transformed_data.txt',
    dag=dag,
)


# task pipeline
extract >> transform_data >> load_data