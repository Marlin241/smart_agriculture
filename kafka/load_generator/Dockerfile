FROM python:3.11-slim

RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends librdkafka-dev gcc python3-dev && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --quiet confluent-kafka lz4

COPY kafka/load_generator/generator.py /app/generator.py

WORKDIR /app

CMD ["python", "-u", "generator.py"]
