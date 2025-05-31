from airflow.decorators import task, dag
from airflow.providers.docker.operators.docker import DockerOperator

from datetime import datetime

@dag(start_date=datetime(2025, 6, 1), schedule_interval='daily', catchup=False)

def docker_dag():

    @task()
    t1 = DockerOperator(
        task_id='t1'
        image=''
    )