FROM python:3.11-slim
WORKDIR /app
COPY ingestion /app/ingestion
COPY streaming /app/streaming
RUN pip install --no-cache-dir requests kafka-python
ENV PYTHONPATH=/app
CMD ["python", "/app/ingestion/api_collector.py"]
