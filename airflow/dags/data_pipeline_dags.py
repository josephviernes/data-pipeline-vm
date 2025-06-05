from airflow.sdk import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'jnv'
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    default_args
    description="Phivolcs earthquake data pipeline"
    tags=["earthquake_data_pipeline"]
    catchup=False,
    schedule="0 6,18 * * *"

) as dag: