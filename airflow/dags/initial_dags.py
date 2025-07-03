from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from docker.types import Mount

creds_container_path = "/gsa/finalproject-456408-a18af71e91f6.json"
creds_host_folder_path = "/home/joseph/Documents/dez_final_project/gsa"

default_args = {
    'owner': 'jnv',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    default_args=default_args,
    dag_id="initial_dags",
    tags=["earthquake_data_pipeline"],
    catchup=False,
    schedule=None,
    start_date=datetime(2025, 6, 10),
) as dag:

    t1 = DockerOperator(
        task_id="bulk_scraper",
        image='earthquake_data_bulk_web_scraper:v1.0',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        mounts=[
            Mount(source="/var/run", target="/var/run", type="bind"),
            Mount(source=creds_host_folder_path, target="/gsa", type="bind", read_only=True)
        ],
        environment={"GOOGLE_APPLICATION_CREDENTIALS": creds_container_path, "bucket": "phivolcs_earthquake_data", "folder": "dailies"},
        command='python3 scraper.py',
        auto_remove=True,
    )

    t2 = DockerOperator(
        task_id="processor",
        image='earthquake_data_processor:v2.0',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        mounts=[
            Mount(source="/var/run", target="/var/run", type="bind"),
            Mount(source=creds_host_folder_path, target="/gsa", type="bind", read_only=True)
        ],
        environment={"GOOGLE_APPLICATION_CREDENTIALS": creds_container_path, "bucket": "phivolcs_earthquake_data", "folder": "dailies"},
        command='python3 processor.py',
        auto_remove=True,
    )

    t3 = DockerOperator(
        task_id="merger",
        image='earthquake_data_merger:v2.0',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        mounts=[
            Mount(source="/var/run", target="/var/run", type="bind"),
            Mount(source=creds_host_folder_path, target="/gsa", type="bind", read_only=True)
        ],
        environment={"GOOGLE_APPLICATION_CREDENTIALS": creds_container_path},
        command='python3 merger.py',
        auto_remove=True,
    )

    t1 >> t2 >> t3
