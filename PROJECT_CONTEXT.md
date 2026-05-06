# Contexte Projet — Pipeline Big Data Agricole (Mangues)

> Ce document sert à initialiser Claude Code sur le contexte complet du projet.
> Si des points ne sont pas clairs, pose des questions avant de commencer à coder.

---

## 1. Vue d'ensemble

Projet académique Big Data visant à collecter, traiter et visualiser des données de capteurs agricoles simulés dans **Webots**, afin d'aider un agriculteur à prendre de meilleures décisions sur son champ de manguiers.

### Stack technologique cible

| Composant | Technologie | Rôle |
|---|---|---|
| Simulation | Webots | Génère les données capteurs |
| Streaming | Apache Kafka | Consomme et transporte les données |
| Stockage brut | MinIO (bucket `raw`) | Stockage des données non traitées |
| Traitement | Apache Spark | Nettoyage et enrichissement |
| Stockage propre | MinIO (bucket `clean`) | Données prêtes pour le dashboard |
| Orchestration | Apache Airflow | Planification des jobs Spark |
| Visualisation | Dashboard (à définir) | Interface décision agriculteur |

### Architecture globale

```
Webots (simulation capteurs)
        ↓
    Kafka (topic: sensor_data)
        ↓
  MinIO bucket: raw
        ↓
  Spark (nettoyage + enrichissement)
        ↓
  MinIO bucket: clean
        ↓
  Dashboard agriculteur
        ↑
  Airflow (orchestration des jobs Spark)
```

---

## 2. Capteurs simulés dans Webots

Chaque message Kafka correspond à une lecture de capteurs sur un manguier. Voici le schéma JSON attendu :

```json
{
  "sensor_id": "manguier_zone_A_01",
  "timestamp": "2025-05-06T14:32:00Z",
  "temperature": 32.4,
  "humidity": 67.2,
  "nutrients": {
    "nitrogen": 45.0,
    "phosphorus": 22.1,
    "potassium": 38.7
  },
  "plant_height_cm": 87.5,
  "color_rgb": {
    "r": 210,
    "g": 180,
    "b": 40
  },
  "leds": {
    "blue_watering": false,
    "green_data_collection": true,
    "orange_fertilization": false,
    "yellow_harvest": false
  }
}
```

### Détail des capteurs

#### Température
- Unité : °C
- Plage valide : -5°C à 60°C (contexte Afrique de l'Ouest)
- Utilité : conditions de croissance, stress thermique

#### Humidité
- Unité : % (humidité relative de l'air)
- Plage valide : 0 à 100
- Utilité : risque maladies fongiques, stress hydrique

#### Nutriments (NPK)
- `nitrogen` : Azote (mg/kg)
- `phosphorus` : Phosphore (mg/kg)
- `potassium` : Potassium (mg/kg)
- Valeurs négatives = erreur capteur

#### Hauteur plante
- Champ : `plant_height_cm`
- Unité : centimètres
- Mesure la hauteur actuelle de la plante dans la simulation

#### Couleur RGB (maturité mangue)
- Webots envoie les valeurs RGB brutes du capteur de couleur
- **Spark calcule la catégorie de maturité** selon ces seuils :

| Catégorie | Condition RGB | Signification |
|---|---|---|
| `vert` | G dominant | Mangue immature |
| `jaune_vert` | R>150, G>150, B<100 | Début de maturation |
| `jaune` | R>200, G>160, B<80 | Presque mûr |
| `orange` | R>200, G<140, B<80 | Récolte urgente |
| `indefini` | Autre | Valeur non classifiable |

> ⚠️ Ces seuils RGB sont provisoires — à calibrer avec de vraies photos de mangues.

#### LEDs du robot (état du robot Webots)
Les LEDs indiquent l'action en cours du robot dans la simulation. Elles s'allument **automatiquement** selon des conditions définies dans Webots.

| LED | Couleur | Action du robot |
|---|---|---|
| `blue_watering` | 🔵 Bleu | Robot en train d'arroser |
| `green_data_collection` | 🟢 Vert | Robot collecte uniquement (aucune autre action) |
| `orange_fertilization` | 🟠 Orange | Robot en train de fertiliser le sol |
| `yellow_harvest` | 🟡 Jaune | Robot en train de récolter (plantes disparaissent une à une) |

> Une seule LED est active à la fois. Elles permettent de contextualiser les données capteurs selon l'action en cours.

#### Timestamp
- Format attendu : ISO 8601 (`2025-05-06T14:32:00Z`)
- Toute ligne sans timestamp valide est **rejetée** dans un bucket `quarantine`

---

## 3. Traitement Spark — règles de nettoyage

Le job Spark lit depuis `MinIO/raw` et écrit dans `MinIO/clean`.

### Règles de validation

| Champ | Condition d'anomalie | Action |
|---|---|---|
| `temperature` | < -5 ou > 60 | Marqué aberrant, interpolation linéaire |
| `humidity` | < 0 ou > 100 | Erreur capteur → `null` |
| `nutrients.*` | Valeur négative | Valeur impossible → `null` |
| `plant_height_cm` | Décroissance brusque | Aberration → lissage |
| `timestamp` | Manquant ou invalide | Ligne rejetée → bucket `quarantine` |
| Doublons | Même `sensor_id` + `timestamp` | Dédupliqué (garder 1 occurrence) |

### Enrichissement ajouté par Spark

```json
{
  "maturity_label": "jaune",
  "maturity_score": 0.75,
  "alerts": ["humidite_critique", "recolte_proche"]
}
```

- `maturity_label` : calculé depuis `color_rgb` selon les seuils définis ci-dessus
- `maturity_score` : valeur numérique 0.0 → 1.0 (0=vert, 1=orange)
- `alerts` : liste d'alertes générées selon des seuils agronomiques

### Alertes agronomiques (à implémenter)

- `humidite_critique` : humidité > 85% depuis plus de 48h → risque moisissures
- `secheresse` : humidité < 30% → stress hydrique
- `deficit_azote` : nitrogen < seuil optimal mangue
- `recolte_proche` : maturity_label == "jaune" ou "orange"
- `temperature_excessive` : température > 45°C

---

## 4. Airflow — DAGs prévus

Airflow orchestre les **traitements batch périodiques**. Il ne gère pas le streaming (c'est Kafka).

### DAG 1 — `spark_cleaning_job`
- **Fréquence** : toutes les heures (ou à définir)
- **Action** : déclenche le job Spark sur les nouveaux fichiers dans `MinIO/raw`
- **Sortie** : données propres dans `MinIO/clean`

### DAG 2 — `daily_agronomic_report`
- **Fréquence** : 1 fois par jour
- **Action** : calcule les agrégats journaliers (moyennes, tendances, alertes)
- **Sortie** : rapport JSON/CSV pour le dashboard

### DAG 3 — `sensor_health_check`
- **Fréquence** : toutes les 2 heures
- **Action** : vérifie que `MinIO/raw` a bien reçu des données récemment
- **Alerte** : si aucune donnée depuis 2h → notification (email ou log)

### DAG 4 — `data_archiving`
- **Fréquence** : 1 fois par semaine
- **Action** : archive ou supprime les anciennes données raw après X jours

---

## 5. Dashboard — informations à afficher

L'objectif est d'aider l'agriculteur à prendre des décisions. Les métriques importantes :

### Temps réel
- Température et humidité actuelles (courbes)
- État des nutriments NPK vs seuils optimaux manguier
- Indicateur de maturité par zone du champ
- Action en cours du robot (basé sur l'état des LEDs)

### Aide à la décision
- **Alerte récolte** : *"Zone A — mangues à 80% de maturité, récolte recommandée"*
- **Alerte irrigation** : *"Humidité critique — arrosage nécessaire"*
- **Alerte fertilisation** : *"Déficit en Azote détecté en zone B"*
- **Alerte maladie** : *"Humidité > 85% depuis 48h — risque de moisissures"*

---

## 6. Source de données — état actuel

### Simulation Webots (en cours)
- Un collègue développe la simulation dans Webots
- Les capteurs envoient les données via un **producteur Kafka** à implémenter
- La fréquence d'envoi exacte n'est pas encore définie (à clarifier)

### Dataset réel (en discussion)
- L'équipe discute d'utiliser un vrai dataset public en complément ou remplacement
- Candidats possibles : NASA POWER (météo agricole), datasets Kaggle (crop recommendation avec NPK, température, humidité, pluie, pH)
- **Décision non prise** — Webots est la priorité pour l'instant

---

## 7. Points encore ouverts / à clarifier

Si tu as besoin de précisions avant de commencer, voici les questions ouvertes :

1. **Fréquence d'envoi Kafka** : toutes les secondes ? toutes les minutes ?
2. **Format des nutrients dans Webots** : NPK séparé confirmé, mais quelles unités exactement ?
3. **Seuils RGB maturité** : les valeurs actuelles sont provisoires, à calibrer
4. **Dashboard** : quel outil ? (Grafana, Metabase, app custom React/Vue ?)
5. **Dataset réel** : décision finale prise ou on reste sur Webots pour tout le projet ?
6. **Infrastructure** : tout en local (Docker Compose) ou déploiement VPS prévu ?

---

## 8. Structure de dossiers suggérée

```
big_data_agri/
├── kafka/
│   ├── producer/          # Producteur Kafka (connecté à Webots ou dataset)
│   └── consumer/          # Consommateur Kafka → écrit dans MinIO raw
├── spark/
│   └── cleaning_job.py    # Job Spark de nettoyage et enrichissement
├── airflow/
│   └── dags/
│       ├── spark_cleaning_job.py
│       ├── daily_agronomic_report.py
│       ├── sensor_health_check.py
│       └── data_archiving.py
├── minio/
│   └── setup.py           # Init buckets (raw, clean, quarantine)
├── dashboard/             # À définir selon l'outil choisi
├── docker-compose.yml     # Kafka, MinIO, Spark, Airflow, etc.
└── PROJECT_CONTEXT.md     # Ce fichier
```

---

*Document généré lors de la phase de conception — à mettre à jour au fil du développement.*
