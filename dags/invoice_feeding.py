import airflow
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta

from jobs.upload_invoice_to_gcs import upload_invoice_to_gcs
from jobs.upload_invoice_gcs_to_bq import upload_invoice_gcs_to_bq


default_args = {
    'owner': 'Mek',
    'retries': 1,
    'retry_delay': timedelta(seconds=3),
    'catchup': False
}

with DAG(

    default_args = default_args,
    dag_id = 'invoice_feeding',
    start_date = airflow.utils.dates.days_ago(1),
    schedule_interval = '*/5 * * * *',   # Run every 5 minutes
    catchup = False

)as dag:
    
    start_task = EmptyOperator(
        task_id = 'start'
    )

    invoice_task = SparkSubmitOperator(
        task_id = 'invoice_feeding',
        conn_id = 'spark-conn',
        application = 'dags/jobs/table_invoice.py'
    )

    upload_invoice_to_gcs_task = PythonOperator(
        task_id = 'upload_invoice_to_gcs',
        python_callable = upload_invoice_to_gcs
    )

    upload_invoice_gcs_to_bq_task = PythonOperator(
        task_id = 'upload_invoice_gcs_tp_bq',
        python_callable = upload_invoice_gcs_to_bq
    )

    # Task Dependencies
    start_task >> invoice_task >> upload_invoice_to_gcs_task >> upload_invoice_gcs_to_bq_task

