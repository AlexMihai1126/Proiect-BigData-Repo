FROM apache/spark:4.1.1-python3

USER root

WORKDIR /app

RUN pip install --no-cache-dir numpy

COPY ml.py .

CMD ["/opt/spark/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1", "/app/ml.py"]