# Use an official Python runtime as the base image
FROM python:3.12.3

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file to the working directory
COPY requirements.txt .

# Install dependencies needed for Spark (Java, curl, etc.)
RUN apt-get update && apt-get install -y openjdk-11-jdk curl && rm -rf /var/lib/apt/lists/*

# Set environment variables for Java
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Download Spark dependencies
RUN curl -L "https://downloads.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz" | \
    tar -xz -C /opt && \
    mv /opt/spark-3.5.1-bin-hadoop3 /opt/spark

# Install the Python libraries including pyspark
RUN pip install -r requirements.txt

# Pyspark environment variables

ENV SPARK_HOME=/opt/spark
ENV PYSPARK_PYTHON=python3
ENV PATH=${SPARK_HOME}/bin:$PATH

# Copy the rest of the application code to the container
COPY . .

# Expose port 5000 to the host machine
EXPOSE 5000

# Define the command to run the app
# CMD ["python", "transformer.py"]
