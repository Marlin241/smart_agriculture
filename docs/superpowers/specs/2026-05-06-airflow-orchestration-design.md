# Spec — Sous-système D : Orchestration Airflow

**Date :** 2026-05-06
**Périmètre :** Déploiement d'Apache Airflow dans Docker Compose et implémentation de 4 DAGs orchestrant le pipeline Big Data agricole.

---

## 1. Objectif

Automatiser l'exécution périodique des traitements batch du pipeline. Airflow orchestre le job Spark de nettoyage, génère un rapport agronomique journalier, surveille la santé des capteurs, et archive les anciennes données brutes.

---

## 2. Justification des choix techniques

| Choix | Justification |
|---|---|
| **LocalExecutor + PostgreSQL** | SQLite (alternative simple) présente des risques de corruption sous charge et ne supporte pas les accès concurrents (UI + scheduler simultanés). PostgreSQL est la configuration minimale correcte recommandée par Airflow. CeleryExecutor serait du sur-engineering pour 4 DAGs académiques. |
| **DockerOperator pour Spark** | Le job Spark tourne déjà via `docker compose run`. DockerOperator utilise le même mécanisme (SDK Python Docker via socket) avec en plus : logs rapatriés dans l'UI Airflow, retour d'erreur structuré, pas besoin de `docker` CLI dans le container Airflow. Même niveau d'accès au daemon Docker que BashOperator, meilleure intégration. |
| **PythonOperator pour les 3 autres DAGs** | Les tâches de rapport, santé et archivage manipulent des objets MinIO et des DataFrames pandas — pas besoin de la JVM Spark pour ces volumes. PythonOperator exécute directement dans le process Airflow, zéro container supplémentaire. |
| **`COPY spark /opt/spark-jobs` dans le Dockerfile Spark** | Le DockerOperator appelle le daemon Docker directement (pas via docker-compose), il a besoin de chemins absolus hôte pour les bind mounts. Bake les fichiers dans l'image élimine ce besoin, rend le projet 100% portable sans `PROJECT_PATH` hardcodé dans `.env`. Trade-off : `docker compose build spark` requis si `cleaning_job.py` change — acceptable car le script est stabilisé. |
| **Réseau explicite `smart-farm-net`** | Le nom de réseau par défaut de Docker Compose est `{répertoire}_{default}` — il change si le projet est cloné dans un autre dossier. Un réseau nommé explicitement avec `name: smart-farm-net` garantit un nom fixe sur n'importe quel PC, ce que le DockerOperator utilise via `network_mode='smart-farm-net'`. |
| **DAG 1 : fréquence 1 heure** | Le consumer Kafka flush toutes les 5 min dans `smart-farm-raw`. Volume académique faible (4 champs), dashboard consulté périodiquement et non en surveillance temps-réel — une latence max de 1h est suffisante. Démarrages JVM toutes les 15 min seraient inutilement coûteux. |
| **DAG 2 : exécution à 01h00** | Couvre l'intégralité des données de la veille (J-1 00h00 → 23h59) avec une marge. Rapport disponible tôt le matin avant la journée de travail. |
| **DAG 4 : archive puis supprime après 30 jours** | Suppression directe est risquée (données irrecouvrables). `smart-farm-archive` sert de zone de rétention : les données restent 30 jours supplémentaires avant suppression définitive. `smart-farm-quarantine` est réservé aux données invalides (mauvais capteurs) — y mélanger des données vieilles mais valides créerait une confusion sémantique. |

---

## 3. Architecture

```
Docker Compose (réseau : smart-farm-net)
────────────────────────────────────────────────────────────────
  postgres:15              ← métadonnées Airflow (runs, états, logs)
  airflow-init             ← one-shot : db migrate + user admin
  airflow-webserver :8082  ← UI Airflow  (8080=kafka-ui)
  airflow-scheduler        ← surveille les DAGs, déclenche les tâches

  (existants)
  minio, kafka, kafka-consumer, spark, zookeeper, kafka-ui
────────────────────────────────────────────────────────────────

Flux de données orchestré par Airflow :

  smart-farm-raw  ──[DAG 1 : @hourly]──►  smart-farm-clean
                  ──[DAG 2 : 01h00]────►  smart-farm-reports
                  ──[DAG 3 : */2h]─────►  (alerte si absent)
                  ──[DAG 4 : lundi 02h]►  smart-farm-archive ──[30j]──► supprimé

Buckets MinIO :
  smart-farm-raw          (existant — données brutes Parquet)
  smart-farm-clean        (existant — données nettoyées/enrichies)
  smart-farm-quarantine   (existant — données invalides capteurs)
  smart-farm-reports      (nouveau — rapports JSON journaliers)
  smart-farm-archive      (nouveau — données raw archivées)
```

---

## 4. Image Airflow personnalisée

**Fichier : `airflow/Dockerfile`**

```dockerfile
FROM apache/airflow:2.9.2
RUN pip install --no-cache-dir \
    apache-airflow-providers-docker \
    minio \
    pandas \
    pyarrow
```

Packages ajoutés :
- `apache-airflow-providers-docker` : fournit `DockerOperator`
- `minio` : SDK Python pour lire/écrire les buckets depuis PythonOperator
- `pandas` + `pyarrow` : agrégats du rapport journalier (DAG 2)

---

## 5. Les 4 DAGs

### 5.1 `spark_cleaning_job` — toutes les heures (`@hourly`)

**Fichier :** `airflow/dags/spark_cleaning_job.py`

Un seul `DockerOperator` :
- Image : `smart_agriculture-spark:latest` (contient `cleaning_job.py` baked-in)
- Commande : `bash -c "/opt/spark/bin/spark-submit /opt/spark-jobs/cleaning_job.py"`
- Variables d'environnement : `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`
- `network_mode='smart-farm-net'` — pour atteindre MinIO
- `auto_remove=True` — container supprimé après exécution
- `catchup=False` — pas de rattrapage des heures passées au démarrage

**Sortie :** `smart-farm-clean/year=.../month=.../day=.../hour=.../`

---

### 5.2 `daily_agronomic_report` — quotidien à 01h00 (`0 1 * * *`)

**Fichier :** `airflow/dags/daily_agronomic_report.py`

Un `PythonOperator` qui :
1. Calcule la date de la veille (J-1)
2. Liste les objets de `smart-farm-clean` dont le chemin contient `year={Y}/month={M}/day={D}/`
3. Télécharge et concatène les Parquet avec pandas
4. Calcule par `sensor_id` : température moy/min/max, humidité moyenne, NPK moyen, nombre d'alertes par type, stades `maturité` et `récolte_imminente`
5. Produit `report_YYYY-MM-DD.json` et l'envoie dans `smart-farm-reports`

**Format de sortie :**
```json
{
  "date": "2026-05-06",
  "par_champ": {
    "sensor_1": {
      "temperature_moy": 28.4,
      "temperature_min": 22.1,
      "temperature_max": 34.7,
      "humidity_moy": 62.1,
      "nitrogen_moy": 45.2,
      "phosphorus_moy": 21.8,
      "potassium_moy": 95.4,
      "nb_alertes": 3,
      "alertes_detail": {"sécheresse": 1, "déficit_azote": 2}
    }
  },
  "cultures_matures": ["sensor_3"],
  "cultures_recolte_imminente": ["sensor_1"],
  "total_alertes": {"sécheresse": 2, "déficit_azote": 3, "excès_humidité": 0}
}
```

**Sortie :** `smart-farm-reports/report_YYYY-MM-DD.json`

---

### 5.3 `sensor_health_check` — toutes les 2 heures (`0 */2 * * *`)

**Fichier :** `airflow/dags/sensor_health_check.py`

Un `PythonOperator` qui :
1. Liste les objets de `smart-farm-raw` via MinIO SDK
2. Filtre ceux modifiés dans les **2 dernières heures**
3. Si aucun objet récent → lève `AirflowException("Aucune donnée reçue depuis 2h")` — tâche en état `failed` dans l'UI, visible immédiatement
4. Si données présentes → log du nombre de fichiers et taille totale (ex. `"12 fichiers reçus, 168 KiB"`)

**Pas d'email** : alerte visible dans l'UI Airflow suffit pour un projet académique (YAGNI).

---

### 5.4 `data_archiving` — lundi 02h00 (`0 2 * * 1`)

**Fichier :** `airflow/dags/data_archiving.py`

Un `PythonOperator` en **deux étapes** dans la même fonction :

**Étape 1 — Archivage** (objets `smart-farm-raw` > 7 jours) :
- Copie chaque objet vers `smart-farm-archive` avec le même chemin
- Supprime l'objet source dans `smart-farm-raw`
- Log : `"X objets archivés (Y MiB)"`

**Étape 2 — Suppression définitive** (objets `smart-farm-archive` > 30 jours) :
- Supprime les objets de `smart-farm-archive` dont `last_modified` > 30 jours
- Log : `"X objets supprimés de smart-farm-archive (Y MiB)"`

---

## 6. Modifications `docker-compose.yml`

### Réseau explicite (nouveau)
```yaml
networks:
  smart-farm-net:
    driver: bridge
    name: smart-farm-net
```
Tous les services existants et nouveaux déclarent `networks: [smart-farm-net]`.

### Nouveaux services
```yaml
postgres:
  image: postgres:15
  environment:
    POSTGRES_USER: airflow
    POSTGRES_PASSWORD: airflow
    POSTGRES_DB: airflow
  volumes:
    - postgres_data:/var/lib/postgresql/data
  networks: [smart-farm-net]

airflow-init:
  build:
    context: .
    dockerfile: airflow/Dockerfile
  depends_on: [postgres]
  environment:
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    _AIRFLOW_DB_MIGRATE: 'true'
    _AIRFLOW_WWW_USER_CREATE: 'true'
    _AIRFLOW_WWW_USER_USERNAME: admin
    _AIRFLOW_WWW_USER_PASSWORD: ${AIRFLOW_ADMIN_PASSWORD}
  entrypoint: /bin/bash
  command: >
    -c "airflow db migrate &&
        airflow users create
          --username admin
          --password ${AIRFLOW_ADMIN_PASSWORD}
          --firstname Admin --lastname User
          --role Admin --email admin@farm.local || true"
  networks: [smart-farm-net]

airflow-webserver:
  build:
    context: .
    dockerfile: airflow/Dockerfile
  depends_on: [postgres, airflow-init]
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
  networks: [smart-farm-net]

airflow-scheduler:
  build:
    context: .
    dockerfile: airflow/Dockerfile
  depends_on: [postgres, airflow-init]
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
  networks: [smart-farm-net]
```

### Nouveaux volumes
```yaml
volumes:
  minio_data:       # existant
  postgres_data:    # nouveau
  airflow_logs:     # nouveau
```

---

## 7. Modifications `.env`

```
# Existant
MINIO_USER=minioadmin
MINIO_PASSWORD=minioadmin

# Nouveau
AIRFLOW_ADMIN_PASSWORD=admin
```

Aucun chemin absolu — projet 100% portable.

---

## 8. Modification `spark/Dockerfile`

Ajout de `COPY spark /opt/spark-jobs` pour bake les scripts dans l'image :

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

La suppression du volume bind mount `./spark:/opt/spark-jobs` dans `docker-compose.yml` est incluse dans les modifications du service `spark`.

---

## 9. Modification `kafka/consumer/consumer.py`

Ajout de `smart-farm-reports` et `smart-farm-archive` dans la liste `BUCKETS` existante :

```python
BUCKETS = [
    MINIO_BUCKET_RAW,
    'smart-farm-clean',
    'smart-farm-quarantine',
    'smart-farm-reports',   # nouveau
    'smart-farm-archive',   # nouveau
]
```

---

## 10. Fichiers créés / modifiés

| Fichier | Action |
|---|---|
| `airflow/Dockerfile` | Créé |
| `airflow/dags/spark_cleaning_job.py` | Créé |
| `airflow/dags/daily_agronomic_report.py` | Créé |
| `airflow/dags/sensor_health_check.py` | Créé |
| `airflow/dags/data_archiving.py` | Créé |
| `docker-compose.yml` | Modifié (réseau explicite, 4 services, 2 volumes, `spark` sans bind mount) |
| `spark/Dockerfile` | Modifié (ajout `COPY spark /opt/spark-jobs`) |
| `kafka/consumer/consumer.py` | Modifié (2 buckets supplémentaires) |
| `.env` | Modifié (ajout `AIRFLOW_ADMIN_PASSWORD`) |
