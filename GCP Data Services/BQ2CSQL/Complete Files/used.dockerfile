FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY bigquery_to_cloudsql_loader.py .

CMD ["python", "bigquery_to_cloudsql_loader.py"]
