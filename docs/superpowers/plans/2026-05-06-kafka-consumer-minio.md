# Kafka Consumer → MinIO Parquet — Plan d'implémentation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Créer un service Docker qui consomme le topic Kafka `sensor_data` et écrit les messages bruts en Parquet partitionné dans MinIO toutes les 5 minutes.

**Architecture:** Un script Python (`consumer.py`) tourne dans un container Docker, écoute `sensor_data` via `kafka-python-ng`, accumule les messages en mémoire, puis toutes les 5 minutes convertit le batch en Parquet (pandas + pyarrow) et l'upload dans MinIO (`smart-farm-raw`) avec un chemin partitionné Hive (`year=/month=/day=/hour=/batch_HHMMSS.parquet`). MinIO et le consumer sont ajoutés au `docker-compose.yml` existant. Trois buckets sont initialisés au démarrage : `smart-farm-raw` (utilisé ici), `smart-farm-clean` et `smart-farm-quarantine` (utilisés en sous-système C).

**Tech Stack:** `kafka-python-ng==2.2.3`, `pandas`, `pyarrow`, `minio` (Python SDK), Docker Compose, MinIO (`minio/minio`), Python 3.12-slim, `pytest` + `unittest.mock`.

---

## Structure des fichiers

| Fichier | Action | Responsabilité |
|---|---|---|
| `requirements.txt` | Créer | Toutes les dépendances Python du projet |
| `requirements_webots.txt` | Supprimer | Remplacé par `requirements.txt` |
| `.env` | Créer | Credentials MinIO (non commité) |
| `.gitignore` | Créer | Exclure `.env` et artefacts Python |
| `kafka/consumer/Dockerfile` | Créer | Image Docker du consumer |
| `kafka/consumer/consumer.py` | Créer | Script principal : poll Kafka → Parquet → MinIO |
| `kafka/consumer/test_consumer.py` | Créer | Tests unitaires des fonctions pures |
| `docker-compose.yml` | Modifier | Ajout services `minio` + `kafka-consumer` + volume |

---

### Task 1 : Fichiers de configuration (requirements.txt, .env, .gitignore)

**Files:**
- Create: `requirements.txt`
- Delete: `requirements_webots.txt`
- Create: `.env`
- Create: `.gitignore`

- [ ] **Step 1 : Créer `requirements.txt` à la racine du projet**

```
kafka-python-ng==2.2.3
pandas
pyarrow
minio
```

- [ ] **Step 2 : Supprimer `requirements_webots.txt`**

Supprimer le fichier `requirements_webots.txt` — remplacé par `requirements.txt`.

- [ ] **Step 3 : Créer `.env` à la racine**

```
MINIO_USER=minioadmin
MINIO_PASSWORD=minioadmin
```

- [ ] **Step 4 : Créer `.gitignore` à la racine**

```
.env
.venv/
__pycache__/
*.pyc
```

- [ ] **Step 5 : Installer les dépendances sur l'hôte**

```bash
pip install -r requirements.txt
```

Résultat attendu : installation sans erreur (kafka-python-ng, pandas, pyarrow, minio installés).

---

### Task 2 : Dockerfile du consumer

**Files:**
- Create: `kafka/consumer/Dockerfile`

- [ ] **Step 1 : Créer le répertoire `kafka/consumer/`**

Créer le dossier `kafka/consumer/` s'il n'existe pas.

- [ ] **Step 2 : Créer `kafka/consumer/Dockerfile`**

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY kafka/consumer/consumer.py .
CMD ["python", "-u", "consumer.py"]
```

> Le contexte de build dans `docker-compose.yml` sera `.` (racine du projet).
> `COPY kafka/consumer/consumer.py .` part donc bien de la racine.

---

### Task 3 : Tests unitaires du consumer

**Files:**
- Create: `kafka/consumer/test_consumer.py`

- [ ] **Step 1 : Créer `kafka/consumer/test_consumer.py`**

```python
import io
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest

from consumer import BUCKETS, build_partition_path, ensure_buckets, flush_batch


def test_build_partition_path_format():
    now = datetime(2026, 5, 6, 14, 35, 12, tzinfo=timezone.utc)
    path = build_partition_path(now)
    assert path == 'year=2026/month=05/day=06/hour=14/batch_143512.parquet'


def test_build_partition_path_zero_padding():
    now = datetime(2026, 1, 3, 9, 5, 7, tzinfo=timezone.utc)
    path = build_partition_path(now)
    assert path == 'year=2026/month=01/day=03/hour=09/batch_090507.parquet'


def test_flush_batch_empty_does_nothing():
    minio_client = MagicMock()
    flush_batch([], minio_client)
    minio_client.put_object.assert_not_called()


def test_flush_batch_uploads_parquet():
    messages = [
        {
            'sensor_id': 'field_1_ble', 'timestamp': '2026-05-06T14:32:00Z',
            'field_id': 1, 'culture': 'Ble', 'temperature_c': 21.4,
            'humidity_pct': 55.2, 'solar_radiation_wm2': 420.0, 'rainfall_mm': 0.0,
            'soil_nitrogen_mg_kg': 62.0, 'soil_phosphorus_mg_kg': 35.0,
            'soil_potassium_mg_kg': 180.0, 'soil_ph': 6.4,
            'plant_growth_pct': 34.5, 'robot_action': 'vide',
            'phase': 'en_croissance', 'water_used_L': 10.0,
            'fertilizer_used_kg': 0.05, 'harvest_count': 0,
        }
    ]
    minio_client = MagicMock()
    flush_batch(messages, minio_client)

    assert minio_client.put_object.call_count == 1
    args = minio_client.put_object.call_args[0]
    assert args[0] == 'smart-farm-raw'
    # Vérifier que le buffer contient un Parquet valide
    buffer = args[2]
    buffer.seek(0)
    df = pd.read_parquet(buffer)
    assert len(df) == 1
    assert df.iloc[0]['culture'] == 'Ble'
    assert df.iloc[0]['field_id'] == 1


def test_ensure_buckets_creates_missing():
    client = MagicMock()
    client.bucket_exists.return_value = False
    ensure_buckets(client)
    assert client.make_bucket.call_count == len(BUCKETS)


def test_ensure_buckets_skips_existing():
    client = MagicMock()
    client.bucket_exists.return_value = True
    ensure_buckets(client)
    client.make_bucket.assert_not_called()
```

- [ ] **Step 2 : Vérifier que les tests échouent (consumer.py n'existe pas encore)**

Depuis le répertoire `kafka/consumer/` :

```bash
cd kafka/consumer
pytest test_consumer.py -v
```

Résultat attendu : `ModuleNotFoundError: No module named 'consumer'`

---

### Task 4 : Implémenter `consumer.py`

**Files:**
- Create: `kafka/consumer/consumer.py`

- [ ] **Step 1 : Créer `kafka/consumer/consumer.py`**

```python
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

BUCKETS = [MINIO_BUCKET_RAW, 'smart-farm-clean', 'smart-farm-quarantine']


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
```

- [ ] **Step 2 : Vérifier que les tests passent**

```bash
cd kafka/consumer
pytest test_consumer.py -v
```

Résultat attendu :
```
test_consumer.py::test_build_partition_path_format PASSED
test_consumer.py::test_build_partition_path_zero_padding PASSED
test_consumer.py::test_flush_batch_empty_does_nothing PASSED
test_consumer.py::test_flush_batch_uploads_parquet PASSED
test_consumer.py::test_ensure_buckets_creates_missing PASSED
test_consumer.py::test_ensure_buckets_skips_existing PASSED

6 passed in X.XXs
```

---

### Task 5 : Modifier `docker-compose.yml`

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1 : Remplacer le contenu de `docker-compose.yml`**

```yaml
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENERS: PLAINTEXT_HOST://0.0.0.0:9092,PLAINTEXT://0.0.0.0:29092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT_HOST://localhost:9092,PLAINTEXT://kafka:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT_HOST:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    depends_on:
      - kafka
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092

  minio:
    image: minio/minio
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: ${MINIO_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_PASSWORD}
    command: server /data --console-address ":9001"
    volumes:
      - minio_data:/data

  kafka-consumer:
    build:
      context: .
      dockerfile: kafka/consumer/Dockerfile
    depends_on:
      - kafka
      - minio
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      KAFKA_TOPIC: sensor_data
      KAFKA_GROUP_ID: smart-farm-consumers
      MINIO_ENDPOINT: minio:9000
      MINIO_ACCESS_KEY: ${MINIO_USER}
      MINIO_SECRET_KEY: ${MINIO_PASSWORD}
      MINIO_BUCKET_RAW: smart-farm-raw
      FLUSH_INTERVAL_SECONDS: 300
    restart: on-failure

volumes:
  minio_data:
```

- [ ] **Step 2 : Vérifier la syntaxe**

```bash
docker compose config
```

Résultat attendu : configuration YAML affichée sans erreur.

---

### Task 6 : Test d'intégration

- [ ] **Step 1 : Builder le consumer**

```bash
docker compose build kafka-consumer
```

Résultat attendu : `Successfully built` (ou `Successfully tagged`) sans erreur.

- [ ] **Step 2 : Démarrer la stack complète**

```bash
docker compose up -d
```

Attendre 20-30 secondes le temps que Kafka et MinIO démarrent.

- [ ] **Step 3 : Vérifier que les 5 containers tournent**

```bash
docker ps
```

Résultat attendu : 5 containers actifs — `zookeeper`, `kafka`, `kafka-ui`, `minio`, `kafka-consumer`.

- [ ] **Step 4 : Vérifier les logs du consumer**

```bash
docker logs smart_agriculture-kafka-consumer-1 -f
```

Résultat attendu dans les premières secondes :
```
[Consumer] MinIO connecté sur minio:9000
[Consumer] Bucket créé: smart-farm-raw
[Consumer] Bucket créé: smart-farm-clean
[Consumer] Bucket créé: smart-farm-quarantine
[Consumer] Écoute sur topic 'sensor_data'
```

- [ ] **Step 5 : Lancer Webots**

Ouvrir Webots → charger `worlds/agrAI.wbt`. Les 4 robots affichent `Kafka connecté sur localhost:9092` dans la console Webots.

- [ ] **Step 6 : Attendre le premier flush (5 minutes) et vérifier**

Après 5 minutes, le log du consumer doit afficher :
```
[Consumer] 40 messages → smart-farm-raw/year=2026/month=05/day=06/hour=XX/batch_XXXXXX.parquet
```

- [ ] **Step 7 : Vérifier dans la console MinIO**

Ouvrir `http://localhost:9001`, se connecter avec `minioadmin` / `minioadmin`.
Naviguer vers `smart-farm-raw` → vérifier la présence d'un fichier `.parquet` dans la structure `year=.../month=.../day=.../hour=.../`.
