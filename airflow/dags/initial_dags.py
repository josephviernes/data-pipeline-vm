import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from docker.types import Mount


#set-up your google credentials
creds_container_path = "/gsa/my_creds.json"
creds_host_folder_path = "/home/jnv/Documents/project/earthquake-data-pipeline/keys"

#set-up your google bucket and if neeeded, the folder inside the bucket
bucket = "earthquake-etl-bucket"
folder = "dailies"
project = "earthquake-etl"
dataset = "earthquake_etl_dataset"

# set the date range of your initial data. Months with no data will be catched by the exception and will be skipped.
years = ["2020", "2021","2022","2023","2024","2025"]
months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"] 


default_args = {
    'owner': 'jnv',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    default_args=default_args,
    dag_id="initial_dags",
    tags=["earthquake"],
    catchup=False,
    schedule=None,
    start_date=datetime(2025, 6, 10),
) as dag:

    t1 = DockerOperator(
        task_id="bulk_scraper",
        image='asia-southeast1-docker.pkg.dev/earthquake-etl/etl-docker-repo/earthquake_bulk_data_web_scraper:v2.0',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        mounts=[
            Mount(source="/var/run", target="/var/run", type="bind"),
            Mount(source=creds_host_folder_path, target="/gsa", type="bind", read_only=True)
        ],
        environment={
            "GOOGLE_APPLICATION_CREDENTIALS": creds_container_path,
            "bucket": bucket,
            "folder": folder,
            "project": project,
            "dataset": dataset,
            "years": ",".join(years),     # Pass them as clean comma-separated strings
            "months": ",".join(months),   # Pass them as clean comma-separated strings
        },
        command='python3 bulk_scraper.py',
        auto_remove=True,
    )

    t2 = DockerOperator(
        task_id="processor",
        image='asia-southeast1-docker.pkg.dev/earthquake-etl/etl-docker-repo/earthquake_data_processor:v2.0',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        mounts=[
            Mount(source="/var/run", target="/var/run", type="bind"),
            Mount(source=creds_host_folder_path, target="/gsa", type="bind", read_only=True)
        ],
        environment={"GOOGLE_APPLICATION_CREDENTIALS": creds_container_path, "bucket": bucket, "folder": folder, "project": project, "dataset": dataset, "years": years, "months": months},
        command='python3 processor.py',
        auto_remove=True,
    )

    t3 = DockerOperator(
        task_id="merger",
        image='asia-southeast1-docker.pkg.dev/earthquake-etl/etl-docker-repo/earthquake_data_merger:v2.0',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        mounts=[
            Mount(source="/var/run", target="/var/run", type="bind"),
            Mount(source=creds_host_folder_path, target="/gsa", type="bind", read_only=True)
        ],
        environment={"GOOGLE_APPLICATION_CREDENTIALS": creds_container_path, "bucket": bucket, "folder": folder, "project": project, "dataset": dataset, "years": years, "months": months},
        command='python3 merger.py',
        auto_remove=True,
    )

    t1 >> t2 >> t3
