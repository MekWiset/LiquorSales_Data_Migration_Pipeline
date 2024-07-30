import airflow
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import timedelta

from jobs.upload_to_gcs import upload_to_gcs
from jobs.upload_gcs_to_bq import upload_gcs_to_bq


default_args = {
    'owner': 'Mek',
    'retries': 1,
    'retry_delay': timedelta(seconds=3)
}

with DAG(

    default_args = default_args,
    dag_id = 'liquor_sales_dag',
    start_date = airflow.utils.dates.days_ago(1),
    schedule_interval = None,
    catchup = False

)as dag:
    
    start_task = EmptyOperator(
        task_id = 'start'
    )

    item_number_bridge_task = SparkSubmitOperator(
        task_id = 'item_number_bridge',
        conn_id = 'spark-conn',
        application = 'dags/jobs/table_item_number_bridge.py',
    )

    vendor_task = SparkSubmitOperator(
        task_id = 'vendor',
        conn_id = 'spark-conn',
        application = 'dags/jobs/table_vendor.py',
    )

    item_price_history_task = SparkSubmitOperator(
        task_id = 'item_price_history',
        conn_id = 'spark-conn',
        application = 'dags/jobs/table_item_price_history.py',
    )

    category_task = SparkSubmitOperator(
        task_id = 'category',
        conn_id = 'spark-conn',
        application = 'dags/jobs/table_category.py',
    )

    item_task = SparkSubmitOperator(
        task_id = 'item',
        conn_id = 'spark-conn',
        application = 'dags/jobs/table_item.py',
    )

    store_address_bridge_task = SparkSubmitOperator(
        task_id = 'store_address_bridge',
        conn_id = 'spark-conn',
        application = 'dags/jobs/table_store_address_bridge.py',
    )

    store_number_bridge_task = SparkSubmitOperator(
        task_id = 'store_number_bridge',
        conn_id = 'spark-conn',
        application = 'dags/jobs/table_store_number_bridge.py',
    )

    store_address_history_task = SparkSubmitOperator(
        task_id = 'store_address_history',
        conn_id = 'spark-conn',
        application = 'dags/jobs/table_store_address_history.py',
    )

    store_address_task = SparkSubmitOperator(
        task_id = 'store_address',
        conn_id = 'spark-conn',
        application = 'dags/jobs/table_store_address.py',
    )

    store_county_task = SparkSubmitOperator(
        task_id = 'county',
        conn_id = 'spark-conn',
        application = 'dags/jobs/table_store_county.py',
    )

    upload_to_gcs_task = PythonOperator(
        task_id = 'upload_to_gcs',
        python_callable = upload_to_gcs,
        op_kwargs = {
            'local_dir':'outputs',
            'bucket':'liquor_sales'
        }
    )

    upload_gcs_to_bq_task = PythonOperator(
        task_id = 'upload_gcs_to_bq',
        python_callable = upload_gcs_to_bq,
        op_kwargs = {
            'bucket_name':'liquor_sales',
            'tables':[
                'item_number_bridge',
                'category',
                'vendor',
                'item_price_history',
                'item',
                'store_number_bridge',
                'store_address_bridge',
                'store_county',
                'store_address_history',
                'store_address'
            ]
        }
    )

    trigger_invoice_feeding_dag_task = TriggerDagRunOperator(
        task_id = 'trigger_invoice_feeding_dag',
        trigger_dag_id='invoice_feeding'
    )


    # Task Dependencies
    start_task >> item_number_bridge_task >> vendor_task >> item_price_history_task >> category_task >> item_task >> store_address_bridge_task >> store_number_bridge_task
    store_number_bridge_task >> store_address_history_task >> store_address_task >> store_county_task >> upload_to_gcs_task >> upload_gcs_to_bq_task >> trigger_invoice_feeding_dag_task

    