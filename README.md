# Ferme Intelligente — Pipeline Big Data Agricole

Projet académique simulant une exploitation agricole de 4 champs (Blé, Maïs, Tournesol, Soja). Des robots virtuels collectent des données de capteurs en temps réel, qui transitent par un pipeline complet jusqu'à un dashboard de visualisation destiné à l'agriculteur.

---

## Vue d'ensemble du pipeline

```
┌─────────────────────────────────────────────────────────────────────┐
│  Webots (simulation)                                                │
│  4 robots → capteurs température, humidité, NPK, stress…           │
└────────────────────────┬────────────────────────────────────────────┘
                         │ JSON toutes les N secondes
                         ▼
              ┌──────────────────┐
              │  Apache Kafka    │  topic : sensor_data
              └────────┬─────────┘
                       │
                       ▼
              ┌──────────────────┐
              │  Consumer Kafka  │  → écrit en Parquet
              └────────┬─────────┘
                       │
                       ▼
         ┌─────────────────────────┐
         │  MinIO  bucket: raw     │  stockage objet S3-compatible
         └─────────────┬───────────┘
                       │  déclenché par Airflow (toutes les heures)
                       ▼
         ┌─────────────────────────┐
         │  Apache Spark           │  nettoyage + enrichissement
         │  (cleaning_job.py)      │  (alertes, score de stress…)
         └─────────────┬───────────┘
                       │
                       ▼
         ┌─────────────────────────┐
         │  MinIO  bucket: clean   │  données prêtes pour le dashboard
         └─────────────┬───────────┘
                       │
                       ▼
         ┌─────────────────────────┐
         │  Dashboard Dash/Plotly  │  interface agriculteur — port 8050
         │  rafraîchissement 5 min │
         └─────────────────────────┘
                       ▲
         ┌─────────────┴───────────┐
         │  Apache Airflow         │  orchestration des jobs (4 DAGs)
         │  interface — port 8082  │
         └─────────────────────────┘
```

---

## Prérequis

| Outil | Version minimale | Usage |
|---|---|---|
| [Docker Desktop](https://www.docker.com/products/docker-desktop/) | 24+ | Faire tourner tous les services |
| [Webots](https://cyberbotics.com/) | R2025a | Simulation des robots |
| Git | — | Cloner le dépôt |

> Webots est le seul outil qui s'installe sur la machine hôte. Tout le reste (Kafka, MinIO, Spark, Airflow, Dashboard) tourne dans Docker.

---

## Installation

### 1. Cloner le dépôt

```bash
git clone <url-du-repo>
cd smart_agriculture
```

### 2. Créer le fichier d'environnement

Copier l'exemple et adapter si nécessaire (les valeurs par défaut fonctionnent en local) :

```bash
# Linux / macOS
cp .env.example .env

# Windows (PowerShell)
Copy-Item .env.example .env
```

Contenu du `.env` :

```env
MINIO_USER=minioadmin
MINIO_PASSWORD=minioadmin
AIRFLOW_ADMIN_PASSWORD=admin
```

> Ce fichier n'est pas versionné (`.gitignore`). Ne jamais mettre de vraies clés en production.

### 3. Démarrer les services Docker

```bash
docker compose up -d
```

Premier lancement : Docker télécharge les images et compile les Dockerfiles (~5 minutes).

#### Services démarrés et leurs ports

| Service | URL / port | Identifiants |
|---|---|---|
| MinIO (console) | http://localhost:9001 | `minioadmin` / `minioadmin` |
| MinIO (API S3) | http://localhost:9000 | — |
| Kafka | `localhost:9092` | — |
| Kafka UI | http://localhost:8080 | — |
| Airflow | http://localhost:8082 | `admin` / `admin` |
| Dashboard | http://localhost:8050 | — |

### 4. Lancer la simulation Webots

1. Ouvrir Webots
2. **File → Open World** → sélectionner `worlds/agrAI.wbt`
3. La simulation démarre automatiquement — les 4 robots commencent à collecter des données

> Les robots écrivent leur état dans `/tmp/farm_field_{1..4}.json`. Le superviseur Webots lit ces fichiers pour mettre à jour l'affichage 3D (couleur du sol, LEDs, hauteur des plantes).

---

## Architecture détaillée

### Subsystem A — Simulation Webots (`controllers/`)

Deux contrôleurs Python exécutés par Webots :

- **`field_robot.py`** : machine à états pour chaque robot (6 phases : `sol_vide → plantation → arrosage_initial → en_croissance → mature → en_recolte`). Gère les capteurs, la consommation d'eau et d'engrais, les économiques par cycle.
- **`field_supervisor.py`** : superviseur global. Lit les états des robots depuis `/tmp/farm_field_{fid}.json` et met à jour la scène 3D (couleurs, LEDs, hauteur des plantes).

Les 4 champs :

| Champ | Culture | Emoji |
|---|---|---|
| field_1 | Blé | 🌾 |
| field_2 | Maïs | 🌽 |
| field_3 | Tournesol | 🌻 |
| field_4 | Soja | 🌱 |

### Subsystem B — Streaming Kafka (`kafka/consumer/`)

Le consumer Kafka écoute le topic `sensor_data` et écrit les messages en fichiers **Parquet** dans le bucket MinIO `smart-farm-raw`. Un flush est effectué toutes les 5 minutes (configurable via `FLUSH_INTERVAL_SECONDS`).

### Subsystem C — Traitement Spark (`spark/`)

Le job `cleaning_job.py` lit depuis `smart-farm-raw`, applique les règles suivantes, puis écrit dans `smart-farm-clean` :

- Suppression des doublons (`sensor_id` + `timestamp`)
- Détection et filtrage des valeurs aberrantes (température, humidité, NPK)
- Calcul du **score de stress** (0–100)
- Enrichissement : **phase de croissance**, **alertes agronomiques** (sécheresse, excès d'humidité, déficit en azote…)

### Subsystem D — Orchestration Airflow (`airflow/dags/`)

4 DAGs automatisés :

| DAG | Fréquence | Action |
|---|---|---|
| `spark_cleaning_job` | Toutes les heures | Lance le job Spark de nettoyage |
| `daily_agronomic_report` | 01h00 chaque jour | Génère le rapport JSON J vs J-1 dans `smart-farm-reports` |
| `sensor_health_check` | Toutes les 2 heures | Vérifie que des données arrivent bien dans MinIO |
| `data_archiving` | Chaque dimanche | Archive les anciennes données brutes |

Accès Airflow : http://localhost:8082 (admin / `AIRFLOW_ADMIN_PASSWORD`)

### Subsystem E — Dashboard (`dashboard/`)

Interface web Python/Dash visible à http://localhost:8050. Données lues depuis MinIO, rafraîchies toutes les **5 minutes** sans rechargement de page.

**Onglet "Tableau de bord"** — vue temps réel :
- 4 cartes cliquables (une par champ) avec statut coloré (🟢 Sain / 🟠 Attention / 🔴 Alerte)
- Panneau de détail : courbes température + humidité sur 24h, jauges NPK, score de stress, liste des alertes

**Onglet "Rapport journalier"** — comparaison J vs J-1 :
- Tableau avec flèches de tendance (↑ rouge = dégradation, ↑ vert = amélioration, ~ = stable)
- Résumé des alertes du jour
- Note automatique si le rapport de la veille n'est pas encore disponible

---

## Lancer les tests

### Tests du consumer Kafka

```bash
pip install -r requirements.txt
pytest kafka/consumer/test_consumer.py -v
```

### Tests du job Spark

```bash
pytest spark/test_cleaning_job.py -v
```

### Tests du dashboard (sans connexion MinIO)

```bash
pip install -r dashboard/requirements.txt
pytest dashboard/tests/ -v
```

---

## Structure du projet

```
smart_agriculture/
├── controllers/
│   ├── field_robot/          # Contrôleur de chaque robot (machine à états)
│   └── field_supervisor/     # Superviseur Webots (rendu 3D)
├── kafka/
│   └── consumer/             # Consumer Kafka → Parquet → MinIO raw
├── spark/
│   ├── cleaning_job.py       # Job de nettoyage et enrichissement
│   └── test_cleaning_job.py
├── airflow/
│   ├── Dockerfile
│   └── dags/                 # 4 DAGs Airflow
├── dashboard/
│   ├── app.py                # Point d'entrée Dash
│   ├── data.py               # Lecture MinIO + transformations
│   ├── pages/
│   │   ├── overview.py       # Onglet "Tableau de bord"
│   │   └── report.py         # Onglet "Rapport journalier"
│   ├── assets/style.css      # Styles de l'interface
│   └── tests/
├── worlds/
│   └── agrAI.wbt             # Monde Webots (charger dans Webots)
├── docker-compose.yml        # Tous les services (Kafka, MinIO, Spark, Airflow, Dashboard)
├── requirements.txt          # Dépendances Kafka consumer + tests Spark
└── .env                      # Variables d'environnement (non versionné)
```

---

## Dépannage rapide

**Le dashboard affiche "Aucune donnée récente"**
→ Vérifier que MinIO contient des fichiers dans `smart-farm-clean` : http://localhost:9001
→ Vérifier que le job Spark a bien tourné dans Airflow : http://localhost:8082

**Airflow n'exécute pas les DAGs**
→ S'assurer que les DAGs ne sont pas en pause dans l'interface Airflow (toggle bleu = actif)

**Le container dashboard ne démarre pas**
→ Reconstruire l'image : `docker compose build dashboard && docker compose up -d dashboard`

**Webots : les robots ne bougent pas**
→ Vérifier que la simulation est bien lancée (bouton Play dans Webots, pas en pause)
