from airflow import DAG
from datetime import datetime
from custom_operators.gcs_operators import ExampleDataToGCSOperator

with DAG(
    'create_and_write_example_data_to_gcs',
    start_date=datetime(2021, 1, 1),
    schedule_interval='@daily'
) as dag:

    create_and_write_example_data = ExampleDataToGCSOperator(
        task_id='create_example_data',
        run_date='{{ ds }}',
        gcp_conn_id='airflow_gke_gcs_conn_id',
        gcs_bucket='example-data-bucket'
    )

    create_and_write_example_data