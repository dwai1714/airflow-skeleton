# fpg_main_dag.py
"""
Prerequisites:
# This example is to show how different things are done in airflow

"""
import os
from datetime import datetime

from airflow import settings
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator

from custom_functions import spark_submit
from dynaconf import settings as dyn_settings

base_path = os.path.dirname(os.path.abspath(__file__))
session = settings.Session()
email_to = dyn_settings.EMAIL

DAG_NAME = 'fpg_main_dag'
args = {
    'start_date': datetime(2017, 3, 20),
    'provide_context': True
}

dag = DAG(
    dag_id=DAG_NAME,
    default_args=args,
    schedule_interval=None,
    concurrency=8,
)
first_dummy_task = DummyOperator(task_id='first_dummy_task', retries=1, dag=dag)
second_dummy_task = DummyOperator(task_id='second_dummy_task', retries=1, dag=dag)

def print_hello(): # This job will fail as there is no **kwargs
    return 'Hello world!'

hello_task = PythonOperator(task_id='hello_task', python_callable=print_hello, email_on_failure=True, email=email_to, dag=dag)

def get_airflow_ts_nodash_id(**kwargs):
    print(str(kwargs['ts_nodash']))
    return str(kwargs['ts_nodash'])

get_airflow_ts_nodash_id_task = PythonOperator(
    task_id='get_airflow_ts_nodash_id_task',
    python_callable=get_airflow_ts_nodash_id,
    dag=dag)

ts_no_dash_id = '{{ ti.xcom_pull(task_ids="get_airflow_ts_nodash_id_task" , dag_id = "fpg_main_dag")}}' # showing how to get xcom

bash_job_task = BashOperator(
        task_id="bash_job_task",
        bash_command= "echo one",
        dag=dag)

email_task = EmailOperator(
        task_id='send_email',
        to=email_to,
        subject='Airflow Alert - ' + ts_no_dash_id,
        html_content=""" <h3>Email Test </h3> """,
        dag=dag
)

spark_job_task = spark_submit(python_file=base_path + '/spark_job.py', job_name="spark", dag=dag, conn_id=None, env_var_size='SMALL')

first_dummy_task.set_downstream(get_airflow_ts_nodash_id_task)

get_airflow_ts_nodash_id_task.set_downstream([spark_job_task, bash_job_task])
email_task.set_upstream([spark_job_task, bash_job_task])
second_dummy_task.set_upstream(email_task)
hello_task.set_upstream(email_task)