# Spark Cleaning Job — Plan d'implémentation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Créer un job PySpark qui lit les données brutes de MinIO `smart-farm-raw`, valide, nettoie, enrichit, et écrit dans `smart-farm-clean` et `smart-farm-quarantine`.

**Architecture:** Script unique `spark/cleaning_job.py` avec fonctions pures testables (compute_stress_score, compute_plant_stage, compute_soil_fertility_score, compute_alerts) exposées comme UDFs Spark. Le container `bitnami/spark:3.5` tourne en mode `local[*]` — un seul container, pas de cluster. Les JARs hadoop-aws sont pré-téléchargés dans l'image via un Dockerfile pour éviter tout téléchargement Maven au runtime.

**Tech Stack:** `pyspark==3.5.0`, `bitnami/spark:3.5`, hadoop-aws 3.3.4, aws-java-sdk-bundle 1.12.262, MinIO S3A, `pytest`.

---

## Structure des fichiers

| Fichier | Action | Responsabilité |
|---|---|---|
| `spark/cleaning_job.py` | Créer | Fonctions pures + fonctions Spark + main() |
| `spark/test_cleaning_job.py` | Créer | Tests unitaires des 4 fonctions pures |
| `spark/Dockerfile` | Créer | Étend bitnami/spark:3.5, pré-installe les JARs S3A |
| `docker-compose.yml` | Modifier | Ajout service `spark` avec build context racine |
| `requirements.txt` | Modifier | Ajout `pyspark==3.5.0` |

---

### Task 1 : Préparer requirements.txt + spark/Dockerfile

**Files:**
- Modify: `requirements.txt`
- Create: `spark/Dockerfile`

- [ ] **Step 1 : Ajouter `pyspark==3.5.0` à `requirements.txt`**

```
kafka-python-ng==2.2.3
pandas
pyarrow
minio
pyspark==3.5.0
```

- [ ] **Step 2 : Installer pyspark sur l'hôte**

```bash
pip install pyspark==3.5.0
```

Résultat attendu : installation sans erreur.

> Note : `pyspark` nécessite Java sur l'hôte uniquement pour *exécuter* un SparkSession. Les tests unitaires importent uniquement les fonctions pures — ils ne démarrent pas de SparkSession — donc Java n'est pas requis pour les tests.

- [ ] **Step 3 : Créer le répertoire `spark/`**

Créer le dossier `spark/` à la racine du projet.

- [ ] **Step 4 : Créer `spark/Dockerfile`**

```dockerfile
FROM bitnami/spark:3.5
USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL \
       https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
       -o /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar \
    && curl -fsSL \
       https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
       -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar
USER 1001
```

> Justification : les JARs `hadoop-aws` et `aws-java-sdk-bundle` permettent à Spark d'accéder à MinIO via le protocole S3A. Ils sont pré-téléchargés dans l'image au moment du `docker compose build` — pas de téléchargement Maven au runtime.

---

### Task 2 : Tests unitaires des fonctions pures

**Files:**
- Create: `spark/test_cleaning_job.py`

- [ ] **Step 1 : Créer `spark/test_cleaning_job.py`**

```python
import pytest

from cleaning_job import (
    compute_alerts,
    compute_plant_stage,
    compute_soil_fertility_score,
    compute_stress_score,
)


# ── compute_stress_score ──────────────────────────────────────────────────────

def test_stress_score_no_stress():
    # humidity=55 (>40), nitrogen=62 (>30) → stress_hum=0, stress_nit=0
    assert compute_stress_score(55.0, 62.0) == pytest.approx(0.0)


def test_stress_score_humidity_only():
    # humidity=20: stress_hum=(40-20)*1.2=24, nitrogen=62: stress_nit=0
    # result = min(100, (24+0)/2) = 12.0
    assert compute_stress_score(20.0, 62.0) == pytest.approx(12.0)


def test_stress_score_both():
    # humidity=10: stress_hum=(40-10)*1.2=36, nitrogen=10: stress_nit=(30-10)*0.8=16
    # result = min(100, (36+16)/2) = 26.0
    assert compute_stress_score(10.0, 10.0) == pytest.approx(26.0)


def test_stress_score_none_input():
    assert compute_stress_score(None, 62.0) is None
    assert compute_stress_score(55.0, None) is None


# ── compute_plant_stage ───────────────────────────────────────────────────────

def test_plant_stage_mature_phase():
    assert compute_plant_stage(50.0, 'mature') == 'récolte_imminente'


def test_plant_stage_maturite():
    assert compute_plant_stage(95.0, 'en_croissance') == 'maturité'


def test_plant_stage_developpement():
    assert compute_plant_stage(70.0, 'en_croissance') == 'développement'


def test_plant_stage_croissance():
    assert compute_plant_stage(40.0, 'en_croissance') == 'croissance'


def test_plant_stage_germination():
    assert compute_plant_stage(10.0, 'en_croissance') == 'germination'


def test_plant_stage_none_growth():
    assert compute_plant_stage(None, 'en_croissance') == 'inconnu'


# ── compute_soil_fertility_score ──────────────────────────────────────────────

def test_soil_fertility_score_max():
    # N=100, P=45, K=200 → (1.0+1.0+1.0)*100/3 = 100.0
    assert compute_soil_fertility_score(100.0, 45.0, 200.0) == pytest.approx(100.0)


def test_soil_fertility_score_half():
    # N=50, P=22.5, K=100 → (0.5+0.5+0.5)*100/3 = 50.0
    assert compute_soil_fertility_score(50.0, 22.5, 100.0) == pytest.approx(50.0)


def test_soil_fertility_score_none():
    assert compute_soil_fertility_score(None, 45.0, 200.0) is None
    assert compute_soil_fertility_score(100.0, None, 200.0) is None
    assert compute_soil_fertility_score(100.0, 45.0, None) is None


# ── compute_alerts ────────────────────────────────────────────────────────────

def test_alerts_empty():
    assert compute_alerts(55.0, 62.0, 5.0, 50.0) == []


def test_alerts_secheresse():
    assert 'sécheresse' in compute_alerts(20.0, 62.0, 5.0, 50.0)


def test_alerts_exces_humidite():
    assert 'excès_humidité' in compute_alerts(90.0, 62.0, 5.0, 50.0)


def test_alerts_deficit_azote():
    assert 'déficit_azote' in compute_alerts(55.0, 15.0, 5.0, 50.0)


def test_alerts_stress_eleve():
    assert 'stress_élevé' in compute_alerts(55.0, 62.0, 70.0, 50.0)


def test_alerts_pret_a_recolter():
    assert 'prêt_à_récolter' in compute_alerts(55.0, 62.0, 5.0, 95.0)


def test_alerts_multiple():
    alerts = compute_alerts(20.0, 15.0, 5.0, 50.0)
    assert 'sécheresse' in alerts
    assert 'déficit_azote' in alerts
```

- [ ] **Step 2 : Vérifier que les tests échouent (cleaning_job.py inexistant)**

```bash
cd spark
python -m pytest test_cleaning_job.py -v
```

Résultat attendu : `ModuleNotFoundError: No module named 'cleaning_job'`

---

### Task 3 : Implémenter les fonctions pures dans `cleaning_job.py`

**Files:**
- Create: `spark/cleaning_job.py` (fonctions pures uniquement)

- [ ] **Step 1 : Créer `spark/cleaning_job.py` avec les fonctions pures**

```python
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, dayofmonth, hour, lag, lead, month, to_timestamp, udf, when, year,
)
from pyspark.sql.types import ArrayType, DoubleType, StringType
from pyspark.sql.window import Window

MINIO_ENDPOINT    = os.environ.get('MINIO_ENDPOINT',    'http://minio:9000')
MINIO_ACCESS_KEY  = os.environ.get('MINIO_ACCESS_KEY',  'minioadmin')
MINIO_SECRET_KEY  = os.environ.get('MINIO_SECRET_KEY',  'minioadmin')
BUCKET_RAW        = 'smart-farm-raw'
BUCKET_CLEAN      = 'smart-farm-clean'
BUCKET_QUARANTINE = 'smart-farm-quarantine'

TIMESTAMP_PATTERN = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$'


# ── Fonctions pures (testables sans SparkSession) ─────────────────────────────

def compute_stress_score(humidity_pct, soil_nitrogen_mg_kg):
    if humidity_pct is None or soil_nitrogen_mg_kg is None:
        return None
    stress_hum = max(0.0, 40.0 - humidity_pct) * 1.2
    stress_nit = max(0.0, 30.0 - soil_nitrogen_mg_kg) * 0.8
    return min(100.0, (stress_hum + stress_nit) / 2.0)


def compute_plant_stage(plant_growth_pct, phase):
    if phase == 'mature':
        return 'récolte_imminente'
    if plant_growth_pct is None:
        return 'inconnu'
    if plant_growth_pct >= 90:
        return 'maturité'
    if plant_growth_pct >= 60:
        return 'développement'
    if plant_growth_pct >= 25:
        return 'croissance'
    return 'germination'


def compute_soil_fertility_score(n, p, k):
    if n is None or p is None or k is None:
        return None
    return min(100.0, (n / 100.0 + p / 45.0 + k / 200.0) * 100.0 / 3.0)


def compute_alerts(humidity_pct, soil_nitrogen_mg_kg, stress_score, plant_growth_pct):
    alerts = []
    if humidity_pct is not None and humidity_pct < 30:
        alerts.append('sécheresse')
    if humidity_pct is not None and humidity_pct > 85:
        alerts.append('excès_humidité')
    if soil_nitrogen_mg_kg is not None and soil_nitrogen_mg_kg < 20:
        alerts.append('déficit_azote')
    if stress_score is not None and stress_score > 60:
        alerts.append('stress_élevé')
    if plant_growth_pct is not None and plant_growth_pct > 90:
        alerts.append('prêt_à_récolter')
    return alerts


# ── Fonctions Spark ───────────────────────────────────────────────────────────

def build_spark_session():
    return (SparkSession.builder
        .appName('SmartFarmCleaning')
        .master('local[*]')
        .config('spark.hadoop.fs.s3a.endpoint',          MINIO_ENDPOINT)
        .config('spark.hadoop.fs.s3a.access.key',         MINIO_ACCESS_KEY)
        .config('spark.hadoop.fs.s3a.secret.key',         MINIO_SECRET_KEY)
        .config('spark.hadoop.fs.s3a.path.style.access',  'true')
        .config('spark.hadoop.fs.s3a.impl',
                'org.apache.hadoop.fs.s3a.S3AFileSystem')
        .getOrCreate())


def validate(df):
    """Sépare les lignes valides (→ clean) des lignes rejetées (→ quarantine)."""
    invalid = df.filter(
        col('timestamp').isNull() |
        col('sensor_id').isNull() |
        ~col('timestamp').rlike(TIMESTAMP_PATTERN)
    )
    valid = df.filter(
        col('timestamp').isNotNull() &
        col('sensor_id').isNotNull() &
        col('timestamp').rlike(TIMESTAMP_PATTERN)
    )
    return valid, invalid


def clean(df):
    """Corrige les anomalies capteurs et déduplique."""
    w = Window.partitionBy('sensor_id').orderBy('timestamp')
    return (df
        .withColumn('_temp_lag',  lag('temperature_c',  1).over(w))
        .withColumn('_temp_lead', lead('temperature_c', 1).over(w))
        .withColumn('temperature_c', when(
            (col('temperature_c') < -5) | (col('temperature_c') > 60),
            (col('_temp_lag') + col('_temp_lead')) / 2
        ).otherwise(col('temperature_c')))
        .drop('_temp_lag', '_temp_lead')
        .withColumn('humidity_pct',
            when((col('humidity_pct') < 0) | (col('humidity_pct') > 100), None)
            .otherwise(col('humidity_pct')))
        .withColumn('soil_nitrogen_mg_kg',
            when(col('soil_nitrogen_mg_kg') < 0, None)
            .otherwise(col('soil_nitrogen_mg_kg')))
        .withColumn('soil_phosphorus_mg_kg',
            when(col('soil_phosphorus_mg_kg') < 0, None)
            .otherwise(col('soil_phosphorus_mg_kg')))
        .withColumn('soil_potassium_mg_kg',
            when(col('soil_potassium_mg_kg') < 0, None)
            .otherwise(col('soil_potassium_mg_kg')))
        .withColumn('plant_growth_pct',
            when((col('plant_growth_pct') < 0) | (col('plant_growth_pct') > 100), None)
            .otherwise(col('plant_growth_pct')))
        .dropDuplicates(['sensor_id', 'timestamp']))


def enrich(df):
    """Ajoute stress_score, plant_stage, soil_fertility_score, alerts."""
    stress_udf    = udf(compute_stress_score,        DoubleType())
    stage_udf     = udf(compute_plant_stage,          StringType())
    fertility_udf = udf(compute_soil_fertility_score, DoubleType())
    alerts_udf    = udf(compute_alerts,               ArrayType(StringType()))

    return (df
        .withColumn('stress_score',
            stress_udf(col('humidity_pct'), col('soil_nitrogen_mg_kg')))
        .withColumn('plant_stage',
            stage_udf(col('plant_growth_pct'), col('phase')))
        .withColumn('soil_fertility_score',
            fertility_udf(col('soil_nitrogen_mg_kg'),
                          col('soil_phosphorus_mg_kg'),
                          col('soil_potassium_mg_kg')))
        .withColumn('alerts',
            alerts_udf(col('humidity_pct'), col('soil_nitrogen_mg_kg'),
                       col('stress_score'), col('plant_growth_pct'))))


def add_partition_columns(df):
    """Extrait year/month/day/hour depuis timestamp pour le partitionnement Hive."""
    ts = to_timestamp(col('timestamp'), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    return (df
        .withColumn('year',  year(ts))
        .withColumn('month', month(ts))
        .withColumn('day',   dayofmonth(ts))
        .withColumn('hour',  hour(ts)))


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel('WARN')

    print('[Spark] Lecture de smart-farm-raw...')
    df = spark.read.parquet(f's3a://{BUCKET_RAW}/')
    print(f'[Spark] {df.count()} lignes lues')

    valid_df, quarantine_df = validate(df)
    q_count = quarantine_df.count()
    if q_count > 0:
        print(f'[Spark] {q_count} lignes → smart-farm-quarantine')
        quarantine_df.write.mode('append').parquet(f's3a://{BUCKET_QUARANTINE}/')

    enriched_df = enrich(clean(valid_df))
    partitioned_df = add_partition_columns(enriched_df)

    print('[Spark] Écriture dans smart-farm-clean...')
    (partitioned_df.write
        .mode('append')
        .partitionBy('year', 'month', 'day', 'hour')
        .parquet(f's3a://{BUCKET_CLEAN}/'))

    print(f'[Spark] {partitioned_df.count()} lignes écrites dans smart-farm-clean')
    spark.stop()


if __name__ == '__main__':
    main()
```

- [ ] **Step 2 : Vérifier que les 20 tests passent**

```bash
cd spark
python -m pytest test_cleaning_job.py -v
```

Résultat attendu :
```
test_cleaning_job.py::test_stress_score_no_stress PASSED
test_cleaning_job.py::test_stress_score_humidity_only PASSED
test_cleaning_job.py::test_stress_score_both PASSED
test_cleaning_job.py::test_stress_score_none_input PASSED
test_cleaning_job.py::test_plant_stage_mature_phase PASSED
test_cleaning_job.py::test_plant_stage_maturite PASSED
test_cleaning_job.py::test_plant_stage_developpement PASSED
test_cleaning_job.py::test_plant_stage_croissance PASSED
test_cleaning_job.py::test_plant_stage_germination PASSED
test_cleaning_job.py::test_plant_stage_none_growth PASSED
test_cleaning_job.py::test_soil_fertility_score_max PASSED
test_cleaning_job.py::test_soil_fertility_score_half PASSED
test_cleaning_job.py::test_soil_fertility_score_none PASSED
test_cleaning_job.py::test_alerts_empty PASSED
test_cleaning_job.py::test_alerts_secheresse PASSED
test_cleaning_job.py::test_alerts_exces_humidite PASSED
test_cleaning_job.py::test_alerts_deficit_azote PASSED
test_cleaning_job.py::test_alerts_stress_eleve PASSED
test_cleaning_job.py::test_alerts_pret_a_recolter PASSED
test_cleaning_job.py::test_alerts_multiple PASSED

20 passed in X.XXs
```

---

### Task 4 : Modifier `docker-compose.yml`

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1 : Ajouter le service `spark` au `docker-compose.yml`**

Ajouter ce bloc après le service `kafka-consumer` et avant la section `volumes:` :

```yaml
  spark:
    build:
      context: .
      dockerfile: spark/Dockerfile
    environment:
      MINIO_ENDPOINT: http://minio:9000
      MINIO_ACCESS_KEY: ${MINIO_USER}
      MINIO_SECRET_KEY: ${MINIO_PASSWORD}
    volumes:
      - ./spark:/opt/spark-jobs
    depends_on:
      - minio
```

- [ ] **Step 2 : Vérifier la syntaxe**

```bash
docker compose config
```

Résultat attendu : configuration YAML complète affichée sans erreur (6 services : zookeeper, kafka, kafka-ui, minio, kafka-consumer, spark).

---

### Task 5 : Test d'intégration

- [ ] **Step 1 : Builder l'image Spark**

```bash
docker compose build spark
```

Résultat attendu : `Successfully built` sans erreur.

> Note : le build télécharge deux JARs depuis Maven Central (~200 Mo). Il faut une connexion internet. Le résultat est mis en cache dans l'image Docker — les runs suivants ne téléchargent plus rien.

- [ ] **Step 2 : S'assurer qu'il y a des données dans `smart-farm-raw`**

Démarrer Webots (`worlds/agrAI.wbt`) et attendre au moins 5 minutes que le consumer ait flushé un premier batch Parquet dans `smart-farm-raw`.

Vérifier dans la console MinIO (`http://localhost:9001`) que le bucket `smart-farm-raw` contient au moins un fichier `.parquet`.

- [ ] **Step 3 : Lancer le job Spark**

```bash
docker compose run --rm spark python /opt/spark-jobs/cleaning_job.py
```

Résultat attendu :
```
[Spark] Lecture de smart-farm-raw...
[Spark] 40 lignes lues
[Spark] 40 lignes écrites dans smart-farm-clean
```

- [ ] **Step 4 : Vérifier dans la console MinIO**

Ouvrir `http://localhost:9001`, naviguer vers `smart-farm-clean`.
Vérifier la présence de fichiers `part-*.parquet` dans la structure `year=.../month=.../day=.../hour=.../`.
