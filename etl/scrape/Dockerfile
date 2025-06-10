# Use an official Python runtime as the base image
FROM python:3.12.3

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file to the working directory
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code to the container
COPY . .

# Expose port 5000 to the host machine
EXPOSE 5000

# Define the command to run the app
CMD ["python3", "scraper.py"]