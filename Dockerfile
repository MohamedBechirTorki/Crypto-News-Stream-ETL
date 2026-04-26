FROM apache/airflow:3.2.0-python3.10

USER root

# Optional system deps
RUN apt-get update && apt-get install -y \
    build-essential \
    && apt-get clean

USER airflow

# Copy requirements
COPY requirements.txt .

# Install Python deps
RUN pip install --no-cache-dir -r requirements.txt