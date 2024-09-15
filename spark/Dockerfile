FROM bitnami/spark:latest

COPY .. /opt/spark-apps/

RUN pip install --upgrade pip

RUN pip install -r /opt/spark-apps/requirements.txt
