# Airflow image with the pipeline's dependencies installed (same base as the course project).
# The project code itself is NOT copied in: docker-compose mounts it, so editing a .py file
# on your PC is picked up without rebuilding the image.
FROM apache/airflow:3.3.2

COPY requirements.txt /tmp/requirements.txt

# Installing "apache-airflow==<current version>" alongside our requirements stops pip from
# upgrading or downgrading Airflow itself while resolving our dependencies.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt
