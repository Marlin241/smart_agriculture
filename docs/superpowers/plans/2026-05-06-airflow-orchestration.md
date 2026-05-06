# Airflow Orchestration — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Déployer Apache Airflow 2.9.2 dans Docker Compose et implémenter 4 DAGs orchestrant le pipeline Big Data agricole (cleaning Spark, rapport journalier, santé capteurs, archivage).

**Architecture:** LocalExecutor + PostgreSQL 15 pour les métadonnées Airflow. Le DAG 1 déclenche le job Spark via DockerOperator (image baked-in, pas de bind mount). Les DAGs 2, 3 et 4 utilisent PythonOperator avec le SDK MinIO et pandas. Réseau Docker explicite `smart-farm-net` avec `name:` fixe pour portabilité.

**Tech Stack:** apache/airflow:2.9.2, apache-airflow-providers-docker, minio SDK, pandas, pyarrow, postgres:15, pytest avec mocks airflow.

---

## Structure des fichiers

```
airflow/
  Dockerfile                              ← image Airflow + providers + minio + pandas
  dags/
    spark_cleaning_job.py                 ← DAG 1 : DockerOperator @hourly
    daily_agronomic_report.py             ← DAG 2 : PythonOperator 01h00 quotidien
    sensor_health_check.py               ← DAG 3 : PythonOperator */2h
    data_archiving.py                    ← DAG 4 : PythonOperator lundi 02h00
  tests/
    conftest.py                          ← mock airflow + sys.path pour tous les tests
    test_daily_agronomic_report.py       ← tests de compute_daily_report()
    test_sensor_health_check.py          ← tests de find_recent_objects()
    test_data_archiving.py               ← tests de find_objects_older_than()

kafka/consumer/consumer.py               ← modifié : +2 buckets ligne 23
spark/Dockerfile                         ← modifié : COPY spark /opt/spark-jobs
docker-compose.yml                       ← modifié : réseau + 4 services + 2 volumes
.env                                     ← modifié : +AIRFLOW_ADMIN_PASSWORD
```

---

## Task 1 : Ajouter les 2 nouveaux buckets dans consumer.py

**Files:**
- Modify: `kafka/consumer/consumer.py:23`

> Note : `test_consumer.py::test_ensure_buckets_creates_missing` utilise `len(BUCKETS)` — il s'adapte automatiquement sans modification.

- [ ] **Step 1 : Vérifier que le test actuel passe**

```powershell
cd kafka/consumer
pytest test_consumer.py -v
```

Attendu : 6 tests PASS.

- [ ] **Step 2 : Modifier la liste BUCKETS dans consumer.py**

Remplacer la ligne 23 de `kafka/consumer/consumer.py` :

```python
# avant
BUCKETS = [MINIO_BUCKET_RAW, 'smart-farm-clean', 'smart-farm-quarantine']

# après
BUCKETS = [
    MINIO_BUCKET_RAW,
    'smart-farm-clean',
    'smart-farm-quarantine',
    'smart-farm-reports',
    'smart-farm-archive',
]
```

- [ ] **Step 3 : Vérifier que tous les tests passent encore**

```powershell
pytest test_consumer.py -v
```

Attendu : 6 tests PASS (test_ensure_buckets_creates_missing attend maintenant 5 appels via `len(BUCKETS)`).

- [ ] **Step 4 : Commit**

```powershell
cd ../..
git add kafka/consumer/consumer.py
git commit -m "feat(consumer): ajouter buckets smart-farm-reports et smart-farm-archive"
```

---

## Task 2 : Bake les scripts Spark dans l'image (portabilité DockerOperator)

**Files:**
- Modify: `spark/Dockerfile`
- Modify: `docker-compose.yml:73-74` (suppression bind mount spark)

- [ ] **Step 1 : Ajouter COPY dans spark/Dockerfile**

Remplacer le contenu de `spark/Dockerfile` :

```dockerfile
FROM apache/spark:3.5.1
USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL \
       https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
       -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    && curl -fsSL \
       https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
       -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar
COPY spark /opt/spark-jobs
```

- [ ] **Step 2 : Build l'image pour vérifier**

```powershell
docker compose build spark
```

Attendu : build réussi, message `smart_agriculture-spark  Built`.

- [ ] **Step 3 : Vérifier que cleaning_job.py est bien dans l'image**

```powershell
docker run --rm smart_agriculture-spark:latest bash -c "ls /opt/spark-jobs/"
```

Attendu : `cleaning_job.py  test_cleaning_job.py`

- [ ] **Step 4 : Commit**

```powershell
git add spark/Dockerfile
git commit -m "feat(spark): bake cleaning_job.py dans l image pour portabilite DockerOperator"
```

---

## Task 3 : Mettre à jour docker-compose.yml et .env

**Files:**
- Modify: `docker-compose.yml`
- Modify: `.env`

- [ ] **Step 1 : Ajouter AIRFLOW_ADMIN_PASSWORD dans .env**

Remplacer le contenu de `.env` :

```
MINIO_USER=minioadmin
MINIO_PASSWORD=minioadmin
AIRFLOW_ADMIN_PASSWORD=admin
```

- [ ] **Step 2 : Écrire le nouveau docker-compose.yml**

Remplacer le contenu complet de `docker-compose.yml` :

```yaml
networks:
  smart-farm-net:
    driver: bridge
    name: smart-farm-net

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    networks: [smart-farm-net]

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
    networks: [smart-farm-net]

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    depends_on:
      - kafka
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
    networks: [smart-farm-net]

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
    networks: [smart-farm-net]

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
    networks: [smart-farm-net]

  spark:
    build:
      context: .
      dockerfile: spark/Dockerfile
    command: bash -c "/opt/spark/bin/spark-submit /opt/spark-jobs/cleaning_job.py"
    environment:
      MINIO_ENDPOINT: http://minio:9000
      MINIO_ACCESS_KEY: ${MINIO_USER}
      MINIO_SECRET_KEY: ${MINIO_PASSWORD}
    depends_on:
      - minio
    networks: [smart-farm-net]

  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres_data:/var/lib/postgresql/data
    networks: [smart-farm-net]
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 10s
      retries: 5
      start_period: 5s

  airflow-init:
    build:
      context: .
      dockerfile: airflow/Dockerfile
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    entrypoint: /bin/bash
    command:
      - -c
      - |
        airflow db migrate
        airflow users create \
          --username admin \
          --password ${AIRFLOW_ADMIN_PASSWORD} \
          --firstname Admin \
          --lastname User \
          --role Admin \
          --email admin@farm.local || true
        echo "Airflow initialisé."
    restart: "no"
    networks: [smart-farm-net]

  airflow-webserver:
    build:
      context: .
      dockerfile: airflow/Dockerfile
    depends_on:
      postgres:
        condition: service_healthy
      airflow-init:
        condition: service_completed_successfully
    ports:
      - "8082:8080"
    environment:
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
      MINIO_ENDPOINT: http://minio:9000
      MINIO_ACCESS_KEY: ${MINIO_USER}
      MINIO_SECRET_KEY: ${MINIO_PASSWORD}
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - airflow_logs:/opt/airflow/logs
      - /var/run/docker.sock:/var/run/docker.sock
    command: webserver
    restart: on-failure
    networks: [smart-farm-net]

  airflow-scheduler:
    build:
      context: .
      dockerfile: airflow/Dockerfile
    depends_on:
      postgres:
        condition: service_healthy
      airflow-init:
        condition: service_completed_successfully
    environment:
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
      MINIO_ENDPOINT: http://minio:9000
      MINIO_ACCESS_KEY: ${MINIO_USER}
      MINIO_SECRET_KEY: ${MINIO_PASSWORD}
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - airflow_logs:/opt/airflow/logs
      - /var/run/docker.sock:/var/run/docker.sock
    command: scheduler
    restart: on-failure
    networks: [smart-farm-net]

volumes:
  minio_data:
  postgres_data:
  airflow_logs:
```

- [ ] **Step 3 : Vérifier la configuration**

```powershell
docker compose config
```

Attendu : YAML résolu sans erreur, 10 services listés (zookeeper, kafka, kafka-ui, minio, kafka-consumer, spark, postgres, airflow-init, airflow-webserver, airflow-scheduler), réseau `smart-farm-net` présent, 3 volumes (minio_data, postgres_data, airflow_logs).

- [ ] **Step 4 : Commit**

```powershell
git add docker-compose.yml .env
git commit -m "feat(infra): reseau explicite smart-farm-net et services Airflow dans docker-compose"
```

---

## Task 4 : Créer airflow/Dockerfile

**Files:**
- Create: `airflow/Dockerfile`

- [ ] **Step 1 : Créer le répertoire et le Dockerfile**

```powershell
New-Item -ItemType Directory -Force airflow/dags
New-Item -ItemType Directory -Force airflow/tests
```

Créer `airflow/Dockerfile` :

```dockerfile
FROM apache/airflow:2.9.2
RUN pip install --no-cache-dir \
    apache-airflow-providers-docker \
    minio \
    pandas \
    pyarrow
```

- [ ] **Step 2 : Builder l'image**

```powershell
docker compose build airflow-webserver
```

Attendu : build réussi. Le build installe les 4 packages pip.

- [ ] **Step 3 : Vérifier que les packages sont disponibles dans l'image**

```powershell
docker run --rm smart_agriculture-airflow-webserver:latest python -c "import minio, pandas, airflow.providers.docker; print('OK')"
```

Attendu : `OK`

- [ ] **Step 4 : Commit**

```powershell
git add airflow/Dockerfile
git commit -m "feat(airflow): Dockerfile Airflow 2.9.2 avec providers-docker, minio, pandas"
```

---

## Task 5 : DAG 1 — spark_cleaning_job.py

**Files:**
- Create: `airflow/dags/spark_cleaning_job.py`

- [ ] **Step 1 : Créer airflow/dags/spark_cleaning_job.py**

```python
import os
from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

with DAG(
    dag_id='spark_cleaning_job',
    schedule_interval='@hourly',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['spark', 'cleaning'],
) as dag:
    DockerOperator(
        task_id='run_cleaning_job',
        image='smart_agriculture-spark:latest',
        command='bash -c "/opt/spark/bin/spark-submit /opt/spark-jobs/cleaning_job.py"',
        environment={
            'MINIO_ENDPOINT': os.environ.get('MINIO_ENDPOINT', 'http://minio:9000'),
            'MINIO_ACCESS_KEY': os.environ.get('MINIO_ACCESS_KEY', 'minioadmin'),
            'MINIO_SECRET_KEY': os.environ.get('MINIO_SECRET_KEY', 'minioadmin'),
        },
        network_mode='smart-farm-net',
        auto_remove=True,
        docker_url='unix:///var/run/docker.sock',
    )
```

- [ ] **Step 2 : Vérifier la syntaxe Python**

```powershell
python -c "import ast; ast.parse(open('airflow/dags/spark_cleaning_job.py').read()); print('Syntaxe OK')"
```

Attendu : `Syntaxe OK`

- [ ] **Step 3 : Commit**

```powershell
git add airflow/dags/spark_cleaning_job.py
git commit -m "feat(airflow): DAG spark_cleaning_job via DockerOperator toutes les heures"
```

---

## Task 6 : DAG 2 — daily_agronomic_report.py + tests

**Files:**
- Create: `airflow/tests/conftest.py`
- Create: `airflow/tests/test_daily_agronomic_report.py`
- Create: `airflow/dags/daily_agronomic_report.py`

- [ ] **Step 1 : Créer airflow/tests/conftest.py**

```python
import os
import sys
from unittest.mock import MagicMock

for mod_name in [
    'airflow',
    'airflow.models',
    'airflow.operators',
    'airflow.operators.python',
    'airflow.providers',
    'airflow.providers.docker',
    'airflow.providers.docker.operators',
    'airflow.providers.docker.operators.docker',
    'airflow.exceptions',
    'docker',
    'docker.types',
]:
    sys.modules[mod_name] = MagicMock()

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dags')))
```

- [ ] **Step 2 : Écrire les tests (ils doivent FAIL)**

Créer `airflow/tests/test_daily_agronomic_report.py` :

```python
import pytest
import pandas as pd
from daily_agronomic_report import compute_daily_report


def _make_row(sensor_id, temp, hum, n, p, k, alerts, stage):
    return {
        'sensor_id': sensor_id,
        'temperature_c': temp,
        'humidity_pct': hum,
        'soil_nitrogen_mg_kg': n,
        'soil_phosphorus_mg_kg': p,
        'soil_potassium_mg_kg': k,
        'alerts': alerts,
        'plant_stage': stage,
    }


def test_date_is_preserved():
    df = pd.DataFrame([_make_row('s1', 28.0, 60.0, 45.0, 22.0, 90.0, [], 'croissance')])
    report = compute_daily_report(df, '2026-05-06')
    assert report['date'] == '2026-05-06'


def test_temperature_aggregates():
    df = pd.DataFrame([
        _make_row('s1', 20.0, 60.0, 45.0, 22.0, 90.0, [], 'croissance'),
        _make_row('s1', 30.0, 60.0, 45.0, 22.0, 90.0, [], 'croissance'),
    ])
    report = compute_daily_report(df, '2026-05-06')
    assert report['par_champ']['s1']['temperature_moy'] == pytest.approx(25.0)
    assert report['par_champ']['s1']['temperature_min'] == pytest.approx(20.0)
    assert report['par_champ']['s1']['temperature_max'] == pytest.approx(30.0)


def test_alerte_count_and_detail():
    df = pd.DataFrame([
        _make_row('s1', 28.0, 60.0, 45.0, 22.0, 90.0, ['sécheresse', 'déficit_azote'], 'croissance'),
        _make_row('s1', 28.0, 60.0, 45.0, 22.0, 90.0, ['sécheresse'], 'croissance'),
    ])
    report = compute_daily_report(df, '2026-05-06')
    assert report['par_champ']['s1']['nb_alertes'] == 3
    assert report['par_champ']['s1']['alertes_detail']['sécheresse'] == 2
    assert report['par_champ']['s1']['alertes_detail']['déficit_azote'] == 1


def test_cultures_matures():
    df = pd.DataFrame([
        _make_row('s1', 28.0, 60.0, 45.0, 22.0, 90.0, [], 'maturité'),
        _make_row('s2', 28.0, 60.0, 45.0, 22.0, 90.0, [], 'croissance'),
    ])
    report = compute_daily_report(df, '2026-05-06')
    assert 's1' in report['cultures_matures']
    assert 's2' not in report['cultures_matures']


def test_cultures_recolte_imminente():
    df = pd.DataFrame([
        _make_row('s1', 28.0, 60.0, 45.0, 22.0, 90.0, [], 'récolte_imminente'),
    ])
    report = compute_daily_report(df, '2026-05-06')
    assert 's1' in report['cultures_recolte_imminente']
    assert 's1' not in report['cultures_matures']


def test_total_alertes_across_fields():
    df = pd.DataFrame([
        _make_row('s1', 28.0, 60.0, 45.0, 22.0, 90.0, ['sécheresse'], 'croissance'),
        _make_row('s2', 28.0, 60.0, 45.0, 22.0, 90.0, ['sécheresse', 'déficit_azote'], 'croissance'),
    ])
    report = compute_daily_report(df, '2026-05-06')
    assert report['total_alertes']['sécheresse'] == 2
    assert report['total_alertes']['déficit_azote'] == 1


def test_empty_alerts_list():
    df = pd.DataFrame([_make_row('s1', 28.0, 60.0, 45.0, 22.0, 90.0, [], 'croissance')])
    report = compute_daily_report(df, '2026-05-06')
    assert report['par_champ']['s1']['nb_alertes'] == 0
    assert report['par_champ']['s1']['alertes_detail'] == {}
```

- [ ] **Step 3 : Vérifier que les tests FAIL**

```powershell
pytest airflow/tests/test_daily_agronomic_report.py -v
```

Attendu : FAIL avec `ImportError: cannot import name 'compute_daily_report' from 'daily_agronomic_report'`

- [ ] **Step 4 : Créer airflow/dags/daily_agronomic_report.py**

```python
import io
import json
import os
from datetime import datetime

import pandas as pd
from minio import Minio

from airflow import DAG
from airflow.operators.python import PythonOperator


def compute_daily_report(df: pd.DataFrame, date_str: str) -> dict:
    report = {
        'date': date_str,
        'par_champ': {},
        'cultures_matures': [],
        'cultures_recolte_imminente': [],
        'total_alertes': {},
    }

    for sensor_id, group in df.groupby('sensor_id'):
        alertes_flat = [a for row in group['alerts'] for a in (row or [])]
        alertes_detail = {}
        for a in alertes_flat:
            alertes_detail[a] = alertes_detail.get(a, 0) + 1

        report['par_champ'][sensor_id] = {
            'temperature_moy': round(float(group['temperature_c'].mean()), 2),
            'temperature_min': round(float(group['temperature_c'].min()), 2),
            'temperature_max': round(float(group['temperature_c'].max()), 2),
            'humidity_moy': round(float(group['humidity_pct'].mean()), 2),
            'nitrogen_moy': round(float(group['soil_nitrogen_mg_kg'].mean()), 2),
            'phosphorus_moy': round(float(group['soil_phosphorus_mg_kg'].mean()), 2),
            'potassium_moy': round(float(group['soil_potassium_mg_kg'].mean()), 2),
            'nb_alertes': len(alertes_flat),
            'alertes_detail': alertes_detail,
        }

        stages = group['plant_stage'].tolist()
        if 'maturité' in stages:
            report['cultures_matures'].append(sensor_id)
        if 'récolte_imminente' in stages:
            report['cultures_recolte_imminente'].append(sensor_id)

    total = {}
    for champ_data in report['par_champ'].values():
        for alerte, count in champ_data['alertes_detail'].items():
            total[alerte] = total.get(alerte, 0) + count
    report['total_alertes'] = total

    return report


def generate_report(**context):
    yesterday = context['data_interval_start'].date()
    date_str = str(yesterday)

    client = Minio(
        os.environ['MINIO_ENDPOINT'].replace('http://', ''),
        access_key=os.environ['MINIO_ACCESS_KEY'],
        secret_key=os.environ['MINIO_SECRET_KEY'],
        secure=False,
    )

    prefix = f"year={yesterday.year}/month={yesterday.month}/day={yesterday.day}/"
    objects = list(client.list_objects('smart-farm-clean', prefix=prefix, recursive=True))

    if not objects:
        print(f"[Report] Aucune donnée pour {date_str}")
        return

    dfs = []
    for obj in objects:
        data = client.get_object('smart-farm-clean', obj.object_name)
        dfs.append(pd.read_parquet(io.BytesIO(data.read())))
    df = pd.concat(dfs, ignore_index=True)

    report = compute_daily_report(df, date_str)
    report_bytes = json.dumps(report, ensure_ascii=False, indent=2).encode('utf-8')
    report_name = f"report_{date_str}.json"
    client.put_object(
        'smart-farm-reports', report_name,
        io.BytesIO(report_bytes), len(report_bytes),
        content_type='application/json',
    )
    print(f"[Report] {report_name} écrit dans smart-farm-reports")


with DAG(
    dag_id='daily_agronomic_report',
    schedule_interval='0 1 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['report', 'agronomic'],
) as dag:
    PythonOperator(
        task_id='generate_daily_report',
        python_callable=generate_report,
    )
```

- [ ] **Step 5 : Vérifier que les tests PASS**

```powershell
pytest airflow/tests/test_daily_agronomic_report.py -v
```

Attendu : 7 tests PASS.

- [ ] **Step 6 : Commit**

```powershell
git add airflow/dags/daily_agronomic_report.py airflow/tests/conftest.py airflow/tests/test_daily_agronomic_report.py
git commit -m "feat(airflow): DAG daily_agronomic_report et tests compute_daily_report"
```

---

## Task 7 : DAG 3 — sensor_health_check.py + tests

**Files:**
- Create: `airflow/tests/test_sensor_health_check.py`
- Create: `airflow/dags/sensor_health_check.py`

- [ ] **Step 1 : Écrire les tests (ils doivent FAIL)**

Créer `airflow/tests/test_sensor_health_check.py` :

```python
from datetime import datetime, timedelta, timezone
from sensor_health_check import find_recent_objects


class MockObject:
    def __init__(self, last_modified, size=1024):
        self.last_modified = last_modified
        self.size = size


def test_returns_recent_objects():
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=2)
    recent = MockObject(now - timedelta(hours=1))
    old = MockObject(now - timedelta(hours=3))
    result = find_recent_objects([recent, old], cutoff)
    assert result == [recent]


def test_returns_empty_when_all_old():
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=2)
    result = find_recent_objects(
        [MockObject(now - timedelta(hours=5)), MockObject(now - timedelta(hours=10))],
        cutoff,
    )
    assert result == []


def test_returns_empty_for_empty_list():
    cutoff = datetime.now(timezone.utc) - timedelta(hours=2)
    assert find_recent_objects([], cutoff) == []


def test_returns_all_when_all_recent():
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=2)
    objects = [MockObject(now - timedelta(minutes=30)), MockObject(now - timedelta(minutes=90))]
    result = find_recent_objects(objects, cutoff)
    assert len(result) == 2
```

- [ ] **Step 2 : Vérifier que les tests FAIL**

```powershell
pytest airflow/tests/test_sensor_health_check.py -v
```

Attendu : FAIL avec `ImportError: cannot import name 'find_recent_objects'`

- [ ] **Step 3 : Créer airflow/dags/sensor_health_check.py**

```python
import os
from datetime import datetime, timedelta, timezone

from minio import Minio

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator


def find_recent_objects(objects: list, cutoff: datetime) -> list:
    return [obj for obj in objects if obj.last_modified >= cutoff]


def check_sensors(**context):
    cutoff = datetime.now(timezone.utc) - timedelta(hours=2)

    client = Minio(
        os.environ['MINIO_ENDPOINT'].replace('http://', ''),
        access_key=os.environ['MINIO_ACCESS_KEY'],
        secret_key=os.environ['MINIO_SECRET_KEY'],
        secure=False,
    )

    all_objects = list(client.list_objects('smart-farm-raw', recursive=True))
    recent = find_recent_objects(all_objects, cutoff)

    if not recent:
        raise AirflowException(
            "Aucune donnée reçue dans smart-farm-raw depuis 2h — vérifier Webots et kafka-consumer"
        )

    total_size = sum(obj.size for obj in recent)
    print(f"[HealthCheck] {len(recent)} fichiers reçus ({total_size // 1024} KiB)")


with DAG(
    dag_id='sensor_health_check',
    schedule_interval='0 */2 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['monitoring', 'health'],
) as dag:
    PythonOperator(
        task_id='check_sensor_data',
        python_callable=check_sensors,
    )
```

- [ ] **Step 4 : Vérifier que les tests PASS**

```powershell
pytest airflow/tests/test_sensor_health_check.py -v
```

Attendu : 4 tests PASS.

- [ ] **Step 5 : Commit**

```powershell
git add airflow/dags/sensor_health_check.py airflow/tests/test_sensor_health_check.py
git commit -m "feat(airflow): DAG sensor_health_check et tests find_recent_objects"
```

---

## Task 8 : DAG 4 — data_archiving.py + tests

**Files:**
- Create: `airflow/tests/test_data_archiving.py`
- Create: `airflow/dags/data_archiving.py`

- [ ] **Step 1 : Écrire les tests (ils doivent FAIL)**

Créer `airflow/tests/test_data_archiving.py` :

```python
from datetime import datetime, timedelta, timezone
from data_archiving import find_objects_older_than


class MockObject:
    def __init__(self, last_modified, object_name='test.parquet', size=1024):
        self.last_modified = last_modified
        self.object_name = object_name
        self.size = size


def test_returns_objects_older_than_threshold():
    ref = datetime.now(timezone.utc)
    old = MockObject(ref - timedelta(days=8))
    recent = MockObject(ref - timedelta(days=3))
    result = find_objects_older_than([old, recent], 7, ref)
    assert result == [old]


def test_returns_empty_when_all_recent():
    ref = datetime.now(timezone.utc)
    result = find_objects_older_than(
        [MockObject(ref - timedelta(days=2))], 7, ref
    )
    assert result == []


def test_boundary_exactly_7_days_is_not_archived():
    ref = datetime.now(timezone.utc)
    exactly_7 = MockObject(ref - timedelta(days=7))
    result = find_objects_older_than([exactly_7], 7, ref)
    assert result == []


def test_returns_empty_for_empty_list():
    ref = datetime.now(timezone.utc)
    assert find_objects_older_than([], 7, ref) == []


def test_30_day_threshold_for_archive_deletion():
    ref = datetime.now(timezone.utc)
    old = MockObject(ref - timedelta(days=31))
    recent = MockObject(ref - timedelta(days=20))
    result = find_objects_older_than([old, recent], 30, ref)
    assert result == [old]
```

- [ ] **Step 2 : Vérifier que les tests FAIL**

```powershell
pytest airflow/tests/test_data_archiving.py -v
```

Attendu : FAIL avec `ImportError: cannot import name 'find_objects_older_than'`

- [ ] **Step 3 : Créer airflow/dags/data_archiving.py**

```python
import os
from datetime import datetime, timedelta, timezone

from minio import Minio
from minio.commonconfig import CopySource

from airflow import DAG
from airflow.operators.python import PythonOperator


def find_objects_older_than(objects: list, days: int, reference: datetime) -> list:
    cutoff = reference - timedelta(days=days)
    return [obj for obj in objects if obj.last_modified < cutoff]


def archive_and_cleanup(**context):
    reference = datetime.now(timezone.utc)

    client = Minio(
        os.environ['MINIO_ENDPOINT'].replace('http://', ''),
        access_key=os.environ['MINIO_ACCESS_KEY'],
        secret_key=os.environ['MINIO_SECRET_KEY'],
        secure=False,
    )

    raw_objects = list(client.list_objects('smart-farm-raw', recursive=True))
    to_archive = find_objects_older_than(raw_objects, 7, reference)

    archived_size = 0
    for obj in to_archive:
        client.copy_object(
            'smart-farm-archive', obj.object_name,
            CopySource('smart-farm-raw', obj.object_name),
        )
        client.remove_object('smart-farm-raw', obj.object_name)
        archived_size += obj.size
    print(f"[Archive] {len(to_archive)} objets archivés ({archived_size // 1024} KiB)")

    archive_objects = list(client.list_objects('smart-farm-archive', recursive=True))
    to_delete = find_objects_older_than(archive_objects, 30, reference)

    deleted_size = 0
    for obj in to_delete:
        client.remove_object('smart-farm-archive', obj.object_name)
        deleted_size += obj.size
    print(f"[Archive] {len(to_delete)} objets supprimés de smart-farm-archive ({deleted_size // 1024} KiB)")


with DAG(
    dag_id='data_archiving',
    schedule_interval='0 2 * * 1',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['archiving', 'maintenance'],
) as dag:
    PythonOperator(
        task_id='archive_and_cleanup',
        python_callable=archive_and_cleanup,
    )
```

- [ ] **Step 4 : Vérifier que les tests PASS**

```powershell
pytest airflow/tests/test_data_archiving.py -v
```

Attendu : 5 tests PASS.

- [ ] **Step 5 : Lancer tous les tests du projet**

```powershell
pytest airflow/tests/ spark/test_cleaning_job.py kafka/consumer/test_consumer.py -v
```

Attendu : tous les tests PASS (16 tests Airflow + 20 tests Spark + 6 tests consumer = 42 tests).

- [ ] **Step 6 : Commit**

```powershell
git add airflow/dags/data_archiving.py airflow/tests/test_data_archiving.py
git commit -m "feat(airflow): DAG data_archiving et tests find_objects_older_than"
```

---

## Task 9 : Test d'intégration Airflow

- [ ] **Step 1 : Reconstruire toutes les images**

```powershell
docker compose build
```

Attendu : build réussi pour kafka-consumer, spark, airflow-webserver, airflow-scheduler, airflow-init.

- [ ] **Step 2 : Démarrer l'infrastructure (sans Airflow en premier)**

```powershell
docker compose up -d zookeeper kafka minio kafka-consumer kafka-ui
```

Attendu : 5 containers Up.

- [ ] **Step 3 : Lancer l'initialisation Airflow**

```powershell
docker compose up airflow-init
```

Attendu : logs montrant `airflow db migrate` puis `airflow users create` puis `Airflow initialisé.` puis exit code 0.

- [ ] **Step 4 : Démarrer Airflow**

```powershell
docker compose up -d airflow-webserver airflow-scheduler postgres
```

Attendu : 3 nouveaux containers Up.

- [ ] **Step 5 : Vérifier l'UI Airflow**

Ouvrir http://localhost:8082 dans un navigateur.
Login : `admin` / `admin`
Attendu : 4 DAGs listés — `spark_cleaning_job`, `daily_agronomic_report`, `sensor_health_check`, `data_archiving`. Tous en état `paused` (normal au démarrage).

- [ ] **Step 6 : Déclencher manuellement le DAG spark_cleaning_job**

Dans l'UI Airflow : cliquer sur `spark_cleaning_job` → bouton "Trigger DAG" (triangle ▶).
Attendu : le run passe en état `running` puis `success`. Vérifier dans l'onglet Logs de la tâche que les lignes `[Spark] 2348 lignes lues` et `[Spark] ... lignes écrites dans smart-farm-clean` apparaissent.

- [ ] **Step 7 : Vérifier smart-farm-reports depuis MinIO**

```powershell
docker compose exec minio mc ls local/smart-farm-reports
```

> Note : `smart-farm-reports` ne contient rien encore — le DAG 2 s'exécutera à 01h00 demain. C'est attendu.

- [ ] **Step 8 : Commit final**

```powershell
git add .
git commit -m "feat(airflow): integration complete - 4 DAGs operationnels dans Docker Compose"
```
