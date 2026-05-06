# Spec — Sous-système C : Spark Cleaning Job

**Date :** 2026-05-06
**Périmètre :** Job PySpark de nettoyage et enrichissement des données brutes. Lit depuis MinIO `smart-farm-raw`, valide, nettoie, enrichit, et écrit dans `smart-farm-clean` et `smart-farm-quarantine`.

---

## 1. Objectif

Transformer les données brutes produites par les robots Webots (stockées en Parquet dans `smart-farm-raw`) en données propres et enrichies prêtes pour le dashboard. Le job applique des règles de validation agronomique, corrige les anomalies capteurs, et calcule 4 métriques dérivées que Webots ne produit pas (`stress_score`, `plant_stage`, `soil_fertility_score`, `alerts`).

---

## 2. Justification des choix techniques

| Choix | Justification |
|---|---|
| **Spark mode `local[*]`** | Pas de cluster nécessaire pour 4 champs × quelques milliers de messages. Le mode local utilise tous les threads CPU du container sans master/worker séparé — évite 3 containers inutiles pour un volume académique. |
| **Script unique** | ~150 lignes de code total. Découper en modules (`validators.py`, `enrichers.py`) serait de la sur-architecture pour du code qui change toujours ensemble — YAGNI (You Ain't Gonna Need It). |
| **Déclenchement manuel (sous-système C)** | Airflow (sous-système D) orchestrera le job automatiquement toutes les heures. Séparer les deux responsabilités évite de coupler l'orchestration au traitement. |
| **Traitement de tout le bucket** | Volume académique faible — pas besoin de watermark ou de suivi d'offset. Airflow ajoutera une fenêtre temporelle si nécessaire. |
| **`plant_stage` remplace `maturity_label`** | Le `PROJECT_CONTEXT.md` initial prévoyait un `maturity_label` basé sur `color_rgb` (capteur couleur pour mangues). La simulation réelle utilise `plant_growth_pct` sur Blé/Maïs/Tournesol/Soja — `plant_stage` est adapté aux vrais champs disponibles. |
| **`stress_score` recalculé par Spark** | Le stress était calculé dans `field_robot.py` uniquement pour piloter la machine à états — il n'est pas exporté dans le payload. Le déporter dans Spark respecte la séparation "capteurs bruts / analyse dérivée". |

---

## 3. Architecture

```
Docker Compose Network
─────────────────────────────────────────────────────────────
  MinIO (minio:9000)
    └─ smart-farm-raw/year=.../month=.../day=.../hour=.../
         └─ batch_HHMMSS.parquet   ← input

  spark (nouveau service — bitnami/spark:3.5, mode local[*])
    └─ /opt/spark-jobs/cleaning_job.py
         ├─ SparkSession locale
         ├─ Lit tous les Parquet de smart-farm-raw
         ├─ Valide (quarantine si sensor_id/timestamp invalide)
         ├─ Nettoie (null + interpolation température)
         ├─ Déduplique
         ├─ Enrichit (4 colonnes calculées)
         ├─→ smart-farm-quarantine/  (lignes rejetées)
         └─→ smart-farm-clean/year=.../month=.../day=.../hour=.../

Lancement manuel    : docker compose run spark python /opt/spark-jobs/cleaning_job.py
Lancement Airflow   : DAG spark_cleaning_job (sous-système D)
```

---

## 4. Règles de validation

### 4.1 Lignes rejetées → `smart-farm-quarantine`

Une ligne est rejetée entièrement quand son **identité** ou son **ancre temporelle** est compromise — sans ces deux informations, la ligne est inutilisable pour toute analyse downstream (agrégation, interpolation, déduplication, dashboard).

| Condition | Justification |
|---|---|
| `timestamp` manquant ou format invalide (non ISO 8601) | Sans timestamp, impossible de situer la mesure dans le temps, d'interpoler, ou d'agréger par heure/jour |
| `sensor_id` absent | Sans sensor_id, impossible d'identifier le champ, de dédupliquer, ou de partitionner par `Window` Spark |

### 4.2 Anomalies corrigées en place → `smart-farm-clean`

Si l'identité et le timestamp sont valides, la ligne est conservée même avec des valeurs aberrantes. Seules les valeurs aberrantes sont nullées ou interpolées.

| Champ | Condition | Action |
|---|---|---|
| `temperature_c` | < −5 ou > 60 | Interpolation linéaire via `Window(partitionBy=sensor_id, orderBy=timestamp)` — remplacée par la moyenne des valeurs valides précédente et suivante |
| `humidity_pct` | < 0 ou > 100 | → `null` |
| `soil_nitrogen_mg_kg` | < 0 | → `null` |
| `soil_phosphorus_mg_kg` | < 0 | → `null` |
| `soil_potassium_mg_kg` | < 0 | → `null` |
| `plant_growth_pct` | < 0 ou > 100 | → `null` |
| Doublons | même `sensor_id` + `timestamp` | `dropDuplicates(['sensor_id', 'timestamp'])` — 1 occurrence conservée |

---

## 5. Enrichissement

4 colonnes calculées ajoutées à chaque ligne valide dans `smart-farm-clean` :

### 5.1 `stress_score` (DoubleType, 0–100)

Réintroduit depuis la logique interne de `field_robot.py`, maintenant calculé côté Spark pour respecter la séparation capteurs bruts / analyse :

```
stress_score = min(100,
    (max(0, 40 − humidity_pct) × 1.2
   + max(0, 30 − soil_nitrogen_mg_kg) × 0.8) / 2)
```

### 5.2 `plant_stage` (StringType)

Étiquette du stade de croissance basée sur `plant_growth_pct` et `phase` :

| Condition | Valeur |
|---|---|
| `phase == 'mature'` | `récolte_imminente` |
| `plant_growth_pct >= 90` | `maturité` |
| `plant_growth_pct >= 60` | `développement` |
| `plant_growth_pct >= 25` | `croissance` |
| `plant_growth_pct < 25` | `germination` |
| `plant_growth_pct IS NULL` | `inconnu` |

### 5.3 `soil_fertility_score` (DoubleType, 0–100)

Score composite NPK normalisé par rapport aux maxima pratiques issus de `field_robot.py` — N est borné à 100 par `min(100, ...)` lors de la fertilisation ; P et K ne sont jamais réapprovisionnés, leurs maxima initiaux sont P=45 (Tournesol) et K=200 (Tournesol) :

```
soil_fertility_score = min(100,
    (soil_nitrogen_mg_kg / 100
   + soil_phosphorus_mg_kg / 45
   + soil_potassium_mg_kg / 200) × 100 / 3)
```

`null` si l'un des 3 nutriments est `null`.

### 5.4 `alerts` (ArrayType(StringType))

Liste d'alertes agronomiques. Liste vide `[]` si aucune condition n'est remplie.

| Alerte | Condition |
|---|---|
| `sécheresse` | `humidity_pct < 30` |
| `excès_humidité` | `humidity_pct > 85` |
| `déficit_azote` | `soil_nitrogen_mg_kg < 20` |
| `stress_élevé` | `stress_score > 60` |
| `prêt_à_récolter` | `plant_growth_pct > 90` |

---

## 6. Schéma de sortie (`smart-farm-clean`)

Schéma d'entrée (19 colonnes du payload Webots) + 4 colonnes enrichies :

| Colonne ajoutée | Type | Description |
|---|---|---|
| `stress_score` | double | Score de stress hydrique + azoté (0–100) |
| `plant_stage` | string | Stade de croissance (`germination` → `récolte_imminente`) |
| `soil_fertility_score` | double | Score de fertilité NPK normalisé (0–100) |
| `alerts` | array\<string\> | Alertes agronomiques actives |

**Partitionnement de la sortie :**
```
smart-farm-clean/
  year=2026/month=05/day=06/hour=14/
    part-00000-xxxx.parquet
```
Colonnes de partition extraites du champ `timestamp` — même convention que `smart-farm-raw`, compatible Spark/Airflow/Dashboard.

---

## 7. Configuration Docker

### Ajout au `docker-compose.yml`

```yaml
spark:
  image: bitnami/spark:3.5
  environment:
    SPARK_MODE: master
    MINIO_ENDPOINT: http://minio:9000
    MINIO_ACCESS_KEY: ${MINIO_USER}
    MINIO_SECRET_KEY: ${MINIO_PASSWORD}
  volumes:
    - ./spark:/opt/spark-jobs
  depends_on:
    - minio
```

### Ajout au `requirements.txt`

```
pyspark==3.5.0
```

`pyspark` est installé sur l'hôte pour permettre les tests unitaires sans Docker.

---

## 8. Fichiers créés / modifiés

| Fichier | Action |
|---|---|
| `spark/cleaning_job.py` | Créé |
| `spark/test_cleaning_job.py` | Créé |
| `docker-compose.yml` | Modifié (ajout service `spark`) |
| `requirements.txt` | Modifié (ajout `pyspark==3.5.0`) |
