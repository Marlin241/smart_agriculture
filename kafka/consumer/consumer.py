import io
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaConsumer
from minio import Minio
from minio.error import S3Error

KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC             = os.environ.get('KAFKA_TOPIC', 'sensor_data')
KAFKA_GROUP_ID          = os.environ.get('KAFKA_GROUP_ID', 'smart-farm-consumers')
MINIO_ENDPOINT          = os.environ.get('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY        = os.environ.get('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY        = os.environ.get('MINIO_SECRET_KEY', 'minioadmin')
MINIO_BUCKET_RAW        = os.environ.get('MINIO_BUCKET_RAW', 'smart-farm-raw')
FLUSH_INTERVAL_SECONDS  = int(os.environ.get('FLUSH_INTERVAL_SECONDS', '300'))

BUCKETS = [
    MINIO_BUCKET_RAW,
    'smart-farm-clean',
    'smart-farm-quarantine',
    'smart-farm-reports',
    'smart-farm-archive',
]


def build_partition_path(now: datetime) -> str:
    return (
        f"year={now.year}/month={now.month:02d}/"
        f"day={now.day:02d}/hour={now.hour:02d}/"
        f"batch_{now.strftime('%H%M%S')}.parquet"
    )


def connect_minio() -> Minio:
    for attempt in range(1, 4):
        try:
            client = Minio(
                MINIO_ENDPOINT,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=False,
            )
            client.list_buckets()
            print(f"[Consumer] MinIO connecté sur {MINIO_ENDPOINT}")
            return client
        except Exception as e:
            print(f"[Consumer] MinIO tentative {attempt}/3 échouée: {e}")
            if attempt < 3:
                time.sleep(5)
    print("[Consumer] MinIO inaccessible après 3 tentatives — arrêt.")
    sys.exit(1)


def ensure_buckets(client: Minio) -> None:
    for bucket in BUCKETS:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            print(f"[Consumer] Bucket créé: {bucket}")
        else:
            print(f"[Consumer] Bucket existant: {bucket}")


def flush_batch(messages: list, minio_client: Minio) -> None:
    if not messages:
        return
    now = datetime.now(timezone.utc)
    object_name = build_partition_path(now)
    df = pd.DataFrame(messages)
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine='pyarrow', index=False)
    buffer.seek(0)
    size = buffer.getbuffer().nbytes
    try:
        minio_client.put_object(
            MINIO_BUCKET_RAW, object_name, buffer, size,
            content_type='application/octet-stream',
        )
        print(f"[Consumer] {len(messages)} messages → {MINIO_BUCKET_RAW}/{object_name}")
    except S3Error as e:
        print(f"[Consumer] Erreur upload MinIO: {e} — batch conservé")
        raise


def main():
    minio_client = connect_minio()
    ensure_buckets(minio_client)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
    )
    print(f"[Consumer] Écoute sur topic '{KAFKA_TOPIC}'")

    batch = []
    last_flush = time.time()

    def handle_shutdown(sig, frame):
        print("[Consumer] Arrêt — flush final en cours...")
        try:
            flush_batch(batch, minio_client)
        except Exception:
            pass
        consumer.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    while True:
        records = consumer.poll(timeout_ms=1000)
        for _, msgs in records.items():
            for msg in msgs:
                batch.append(msg.value)

        if time.time() - last_flush >= FLUSH_INTERVAL_SECONDS:
            try:
                flush_batch(batch, minio_client)
                batch = []
            except Exception:
                pass
            last_flush = time.time()


if __name__ == '__main__':
    main()
