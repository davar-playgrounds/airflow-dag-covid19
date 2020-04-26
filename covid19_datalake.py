from datetime import timedelta

from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from dags_packages.covid19_master_vault.extract_data import data_extraction
from dags_packages.covid19_master_vault.transform_data import data_transformation
from dags_packages.covid19_master_vault.load_data_s3 import data_load_s3


default_args = {
    'owner': 'tuliocg',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['tuliocgon@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'covid19-datalake',
    default_args=default_args,
    description='DAG to populate S3 with COVID data sources',
    schedule_interval=timedelta(hours=2)
    ) as dag:

    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=data_extraction
    )

    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=data_transformation
    )

    load_data_s3 = PythonOperator(
        task_id='load_data_s3',
        python_callable=data_load_s3
    )

    extract_data >> transform_data >> load_data_s3
