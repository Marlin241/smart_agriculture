FROM python:3.11-slim

RUN pip install --quiet kafka-python-ng

COPY kafka/load_generator/generator.py /app/generator.py

WORKDIR /app

CMD ["python", "-u", "generator.py"]
