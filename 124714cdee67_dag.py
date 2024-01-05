from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta


#Define params for Submit Run Operator
notebook_task = {
    'notebook_path': '/Users/amysw13@gmail.com/Reading, cleaning and querying Pinterest Data from mounted S3 bucket using Sparks',
}


#Define params for Run Now Operator
notebook_params = {
    "Variable":5
}


default_args = {
    'owner': 'Amy',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}


with DAG('124714cdee67_dag',
    # should be a datetime format
    start_date= datetime(2024, 1, 5),
    # check out possible intervals, should be a string #@hourly
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    description='Trigger Databricks notebook on a schedule',
    tags=['Databricks']
    ) as dag:


    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        # the connection we set-up previously
        databricks_conn_id='databricks_default',
        existing_cluster_id='1108-162752-8okw8dgg',
        notebook_task=notebook_task
    )
    opr_submit_run