FROM apache/airflow:2.8.1

# Install project dependencies as the airflow user (not root)
COPY data_generator/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
