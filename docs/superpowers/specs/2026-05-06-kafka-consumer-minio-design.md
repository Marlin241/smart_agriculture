# Spec — Sous-système B : Kafka Consumer → MinIO/raw (Parquet)

**Date :** 2026-05-06
**Périmètre :** Consumer Kafka qui lit le topic `sensor_data` et stocke les données brutes en Parquet partitionné dans MinIO, ajout de MinIO dans Docker Compose, unification du `requirements.txt`, fichier `.env` pour les credentials.

---

## 1. Objectif

Lire en continu les messages du topic Kafka `sensor_data` (produits par les 4 robots Webots) et les écrire toutes les 5 minutes dans un bucket MinIO sous forme de fichiers Parquet partitionnés par date/heure. Ces fichiers constituent le **stockage brut** du pipeline — Spark (sous-système C) les lira pour nettoyer et enrichir les données.

---

## 2. Architecture

```
Docker Compose Network
─────────────────────────────────────────────────────────────
  Kafka (kafka:29092)            ← réseau Docker interne
        ↓ poll() toutes les 1s
  kafka-consumer (nouveau service)
    └─ consumer.py
         ├─ kafka-python-ng  → consomme topic sensor_data
         ├─ pandas + pyarrow → convertit batch en Parquet
         └─ minio SDK        → upload vers MinIO

  MinIO (minio:9000)             ← nouveau service
    ├─ bucket: smart-farm-raw        (données brutes Webots)
    ├─ bucket: smart-farm-clean      (données traitées Spark — créé maintenant, utilisé en C)
    └─ bucket: smart-farm-quarantine (données rejetées — créé maintenant, utilisé en C)
```

Le consumer se connecte à Kafka via **le réseau Docker interne** (`kafka:29092`), contrairement aux robots Webots qui utilisent `localhost:9092` depuis l'hôte.

---

## 3. Organisation des fichiers dans MinIO

### Partitionnement Hive (compatible Spark)

```
smart-farm-raw/
  year=2026/
    month=05/
      day=06/
        hour=14/
          batch_143000.parquet
          batch_143500.parquet
        hour=15/
          batch_150000.parquet
```

- **Le dossier** indique quand le consumer a flushé le batch (heure d'ingestion).
- **Le champ `timestamp`** à l'intérieur du Parquet indique l'heure exacte de chaque lecture capteur.
- Spark peut lire `smart-farm-raw/year=2026/month=05/day=06/hour=14/` et auto-détecter `year`, `month`, `day`, `hour` comme colonnes de partition — pas besoin de scanner tout le bucket.

### Nom du fichier

`batch_HHMMSS.parquet` — basé sur l'heure UTC du flush.

---

## 4. Schéma Parquet

Chaque fichier Parquet contient N lignes (les messages accumulés pendant 5 minutes).
Le schéma correspond exactement au payload produit par `field_robot.py` :

| Colonne                  | Type      |
|--------------------------|-----------|
| `sensor_id`              | string    |
| `timestamp`              | string    |
| `field_id`               | int64     |
| `culture`                | string    |
| `temperature_c`          | float64   |
| `humidity_pct`           | float64   |
| `solar_radiation_wm2`    | float64   |
| `rainfall_mm`            | float64   |
| `soil_nitrogen_mg_kg`    | float64   |
| `soil_phosphorus_mg_kg`  | float64   |
| `soil_potassium_mg_kg`   | float64   |
| `soil_ph`                | float64   |
| `plant_growth_pct`       | float64   |
| `robot_action`           | string    |
| `phase`                  | string    |
| `water_used_L`           | float64   |
| `fertilizer_used_kg`     | float64   |
| `harvest_count`          | int64     |

Aucune transformation appliquée — les valeurs sont stockées telles quelles depuis Kafka.

---

## 5. Logique du consumer (`consumer.py`)

```
Démarrage
  └─ Connexion MinIO (retry x3, 5s entre chaque)
  └─ Création des 3 buckets si inexistants
  └─ Connexion Kafka (groupe: smart-farm-consumers)

Boucle principale
  ├─ poll(timeout_ms=1000) → messages Kafka
  ├─ Ajout à la liste en mémoire
  └─ Si (now - last_flush) >= 300s :
       ├─ Liste vide → skip
       └─ Liste non vide :
            ├─ pd.DataFrame(messages)
            ├─ buffer = io.BytesIO()
            ├─ df.to_parquet(buffer, engine='pyarrow', index=False)
            ├─ Upload vers smart-farm-raw / chemin partitionné
            └─ Reset liste + timer

Arrêt (SIGTERM)
  └─ Flush du batch en cours avant de quitter (pas de perte)
```

### Variables d'environnement lues par le script

| Variable                   | Valeur par défaut    |
|----------------------------|----------------------|
| `KAFKA_BOOTSTRAP_SERVERS`  | `kafka:29092`        |
| `KAFKA_TOPIC`              | `sensor_data`        |
| `KAFKA_GROUP_ID`           | `smart-farm-consumers` |
| `MINIO_ENDPOINT`           | `minio:9000`         |
| `MINIO_ACCESS_KEY`         | lu depuis `.env`     |
| `MINIO_SECRET_KEY`         | lu depuis `.env`     |
| `MINIO_BUCKET_RAW`         | `smart-farm-raw`     |
| `FLUSH_INTERVAL_SECONDS`   | `300`                |

---

## 6. Gestion des erreurs

| Situation | Comportement |
|---|---|
| MinIO indisponible au démarrage | Retry x3 (5s), puis arrêt propre — `restart: on-failure` relance le container |
| Kafka indisponible | `poll()` retourne zéro message, le consumer continue de tourner |
| Batch vide au flush | Skip — aucun fichier Parquet vide créé |
| Erreur d'upload MinIO | Log d'erreur, batch conservé en mémoire et fusionné au prochain flush |
| SIGTERM reçu | Flush du batch en cours avant arrêt |

---

## 7. Modifications `docker-compose.yml`

Ajout de deux services au fichier existant :

```yaml
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

Console MinIO : `http://localhost:9001` — login avec les credentials du `.env`.

---

## 8. Fichier `.env` (racine du projet)

```
MINIO_USER=minioadmin
MINIO_PASSWORD=minioadmin
```

Docker Compose charge `.env` automatiquement depuis la racine. Le `.env` est ajouté au `.gitignore` — les credentials ne sont jamais commités.

---

## 9. Unification `requirements.txt`

Le fichier `requirements_webots.txt` est **supprimé**. Un unique `requirements.txt` à la racine remplace tous les fichiers de dépendances :

```
kafka-python-ng==2.2.3
pandas
pyarrow
minio
```

Installation sur la machine hôte (pour Webots) : `pip install -r requirements.txt`
Le container Docker installe le même fichier via le `Dockerfile`.

---

## 10. Fichiers créés / modifiés

| Fichier | Action |
|---|---|
| `kafka/consumer/consumer.py` | Créé |
| `kafka/consumer/Dockerfile` | Créé |
| `docker-compose.yml` | Modifié (ajout minio + kafka-consumer + volume) |
| `requirements.txt` | Créé (remplace requirements_webots.txt) |
| `requirements_webots.txt` | Supprimé |
| `.env` | Créé |
| `.gitignore` | Créé ou modifié (ajout .env) |
