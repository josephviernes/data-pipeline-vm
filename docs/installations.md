Required Libraries for ETL scripts:

    pip install beautifulsoup4 requests urllib3 google-cloud-storage google-cloud-bigquery pyspark

    ENVIRONMENT VARIABLES:

    # pyspark environment variables

    export SPARK_HOME=/opt/spark
    export PATH=$SPARK_HOME/bin:$PATH
    export PYSPARK_PYTHON=python3

    export GOOGLE_APPLICATION_CREDENTIALS="path/to/json"

1. clone the repository
2. set-up you google service account - admin, bq admin, gcs/bucket admin
3. save the google credentials path to ~/.bashrc or (recommended) assign service account to a compute resource
4. docker build each of the ETL script images
5. generate your AIRFLOW__WEBSERVER__SECRET_KEY and AIRFLOW__CORE__FERNET_KEY then save it to the root folder as .env text file