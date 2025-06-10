# Use an official Python runtime as the base image
FROM python:3.12.3-slim

# Set the working directory inside the container
WORKDIR /app

# Install Java 11 (Temurin) and curl
RUN apt-get update && apt-get install -y wget gnupg curl && \
    mkdir -p /etc/apt/keyrings && \
    wget -O- https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor > /etc/apt/keyrings/adoptium.gpg && \
    echo "deb [signed-by=/etc/apt/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb bookworm main" > /etc/apt/sources.list.d/adoptium.list && \
    apt-get update && apt-get install -y temurin-11-jdk && \
    rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/temurin-11-jdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Copy the requirements file to the working directory and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --default-timeout=100 --retries=5 -r requirements.txt

# Set PySpark to use the right Python
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Copy the rest of the application code to the container
COPY . .

# Expose port 5000 to the host machine
EXPOSE 5000

# Define the command to run the app
CMD ["python3", "processor.py"]
