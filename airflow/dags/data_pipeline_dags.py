import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

google_creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

default_args = {
    'owner': 'jnv',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    default_args=default_args,
    dag_id="earthquake_data_pipeline",
    tags=["earthquake_data_pipeline"],
    catchup=False,
    schedule="0 6,18 * * *",
    start_date=datetime(2025, 6, 15),

) as dag:

    t1 = DockerOperator(
        task_id="scraper",
        image='earthquake_data_web_scraper:v3',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        environment=google_creds,
        auto_remove=True,

        
    )

    t2 = DockerOperator(
        task_id="processor",
        image='earthquake_data_processor:v3',
        docker_url='unix://var/run/docker.sock',
        environment=google_creds,
        network_mode='bridge',
        auto_remove=True,

        
    )


    t3 = DockerOperator(
        task_id="merger",
        image='earthquake_data_merger:v3',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        environment=google_creds,
        auto_remove=True,  
        
    )

    t1>>t2>>t3