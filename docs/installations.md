Required Libraries for ETL scripts:

    pip install beautifulsoup4 requests urllib3 google-cloud-storage google-cloud-bigquery pyspark

    ENVIRONMENT VARIABLES:

    # pyspark environment variables for spark

    export SPARK_HOME=/opt/spark
    export PATH=$SPARK_HOME/bin:$PATH
    export PYSPARK_PYTHON=python3

    export GOOGLE_APPLICATION_CREDENTIALS="path/to/json"

    # .env file for airflow

    AIRFLOW__WEBSERVER__SECRET_KEY=
    AIRFLOW__CORE__FERNET_KEY=

    AIRFLOW_UID=1000

1. clone the repository
2. set-up you google service account - admin, bq admin, gcs/bucket admin
3. save the google credentials path to ~/.bashrc or (recommended) assign service account to a compute resource
4. docker build each of the ETL script images
5. AIRFLOW SETUP:
    a. generate your AIRFLOW__WEBSERVER__SECRET_KEY and AIRFLOW__CORE__FERNET_KEY then save it to the airflow folder as .env text file, also add AIRFLOW_UID=1000
    a.1 on your host machine, run "getent group docker" to determine docker group id then add it to your .env file as "AIRFLOW_GID=984" or whatever your Docker group ID is
    b. make this directories on the airflow folder: mkdir -p ./dags ./logs ./plugins ./config
    c. run this: docker compose up airflow-init
    d. Now you can trigger the airflow through "docker compose up"
