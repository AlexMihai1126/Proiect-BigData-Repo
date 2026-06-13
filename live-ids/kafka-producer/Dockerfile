FROM python:3.12-slim

WORKDIR /app

COPY pip-dependencies.txt .
RUN pip install --no-cache-dir -r pip-dependencies.txt

COPY kafka.py .

CMD ["python", "kafka.py"]