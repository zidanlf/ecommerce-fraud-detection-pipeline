FROM apache/airflow:2.6.3-python3.9

# Switch to root to install system package
USER root

# Install git (required by DBT)
RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Install Python requirements
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt