from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email': ['amysw13@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('1234cdee67_dag',
         default_args=default_args,
         description='Trigger Databricks notebook on a schedule',
         schedule_interval='@daily',
         catchup=False,
         tags=['Databricks']
         ) as dag:

    run_databricks_task = DatabricksSubmitRunOperator(
        task_id='run_databricks_notebook',
        databricks_conn_id='databricks_default',
        new_cluster={
            'spark_version': '7.3.x-scala2.12',
            'node_type_id': 'Standard_D3_v2',
            'num_workers': 1,
        },
        notebook_task={
            'notebook_path': '/Users/amysw13@gmail.com/Reading, cleaning and querying Pinterest Data from mounted S3 bucket using Sparks', # Update the path to your Databricks notebook
            }
    )