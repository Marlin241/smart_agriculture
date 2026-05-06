# Dashboard Dash — Design Spec (Subsystem E)

## Objectif

Interface web destinée à un agriculteur non-technique pour visualiser en temps quasi-réel l'état de ses 4 champs (Blé, Maïs, Tournesol, Soja). Données sources : fichiers Parquet nettoyés dans MinIO (`smart-farm-clean`) et rapports JSON quotidiens (`smart-farm-reports`).

## Architecture

### Fichiers

```
dashboard/
├── Dockerfile
├── app.py          # Point d'entrée Dash, layout global, routing 2 onglets
├── data.py         # Lecture MinIO, transformations, fonctions pures testables
├── pages/
│   ├── overview.py # Onglet "Tableau de bord" : 4 cartes + panneau détail
│   └── report.py   # Onglet "Rapport journalier" : tableau comparatif J vs J-1
└── tests/
    └── test_data.py
```

### Service Docker

Ajout dans `docker-compose.yml` :

```yaml
dashboard:
  build: ./dashboard
  ports:
    - "8050:8050"
  environment:
    - MINIO_ENDPOINT=${MINIO_ENDPOINT}
    - MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY}
    - MINIO_SECRET_KEY=${MINIO_SECRET_KEY}
  networks:
    - smart-farm-net
  depends_on:
    - minio
```

### Flux de données

```
MinIO smart-farm-clean (Parquet 24h)  ──┐
                                        ├─► data.py ──► dcc.Store ──► callbacks
MinIO smart-farm-reports (JSON J + J-1) ─┘

dcc.Interval(300 000 ms) ──► re-lecture MinIO ──► mise à jour dcc.Store
```

Toutes les interactions utilisateur (clic carte, navigation onglet) lisent depuis le `dcc.Store` — aucune requête MinIO déclenchée par l'utilisateur.

## Onglet 1 — Tableau de bord

### Barre de navigation

- Titre : `🌾 Ferme intelligente`
- Deux onglets : `Tableau de bord` (actif par défaut) · `Rapport journalier`
- Compteur de rafraîchissement à droite : `🔄 Mise à jour dans 4:32`

### Bandeau d'alertes globales

Visible uniquement s'il existe au moins une alerte active parmi les 4 champs. Fond rouge, liste les alertes en français courant et le champ concerné. Exemple : `🚨 2 alertes actives — Manque d'eau (Blé) · Manque d'engrais azoté (Maïs)`.

### 4 cartes de champs

Toujours visibles, disposées en grille 2×2. Chaque carte affiche :
- Nom de la culture avec emoji (`🌾 Blé`, `🌽 Maïs`, `🌻 Tournesol`, `🌱 Soja`)
- Badge de statut coloré (vert = sain, orange = attention, rouge = alerte)
- Température (`🌡 28°C`) et humidité (`💧 42%`) moyennes des dernières mesures
- Phase actuelle en français courant (`En croissance`, `Arrosage en cours`, `Prêt à récolter`, etc.)

La carte du champ sélectionné est mise en évidence (bordure bleue). Au clic, le panneau de détail ci-dessous se met à jour.

### Panneau de détail (champ sélectionné)

**Graphique Plotly** — courbe double température (°C) et humidité (%) sur les 24 dernières heures. Légende explicite.

**Jauges NPK** — trois barres de progression pour Azote, Phosphore, Potassium (en mg/kg). Chaque jauge montre la valeur actuelle vs le seuil optimal, avec un libellé couleur : `✓ Correct` (vert) ou `⚠ Trop bas` (rouge).

**Alertes du champ** — liste des alertes actives en français courant. Traduction des codes :

| Code interne | Libellé affiché |
|---|---|
| `sécheresse` | Manque d'eau |
| `excès_humidité` | Excès d'humidité |
| `déficit_azote` | Manque d'engrais azoté |
| `stress_élevé` | Stress végétatif élevé |
| `prêt_à_récolter` | Prêt à être récolté |

**Score de stress** — valeur numérique de 0 à 100 avec libellé : `Faible` (0–30, vert), `Modéré` (31–65, orange), `Élevé` (66–100, rouge).

## Onglet 2 — Rapport journalier

Alimenté par les fichiers JSON dans `smart-farm-reports`. Génération quotidienne à 01h00 par le DAG Airflow `daily_agronomic_report`.

### En-tête

Date du rapport + mention de comparaison. Badge vert `🌻 Prêt à récolter` si `cultures_recolte_imminente` est non vide dans le JSON.

### Tableau comparatif J vs J-1

Une ligne par champ, colonnes : Température moyenne, Humidité moyenne, Azote moyen, Nombre d'alertes. Chaque valeur est accompagnée d'une flèche de tendance colorée :
- `↑` rouge si la valeur indique une dégradation (ex : température qui monte, humidité qui baisse)
- `↑` vert si la valeur indique une amélioration
- `~` gris si variation < 5 %

### Résumé des alertes du jour

Compteurs colorés par type d'alerte (libellés français courant). Affichés uniquement si `total_alertes` est non vide.

### Gestion rapport J-1 absent

Si le fichier JSON de la veille est absent (premiers jours, Airflow non encore déclenché) : affichage des données du jour uniquement, avec note : *"Données du jour précédent non disponibles."*

## Gestion des erreurs

| Cas | Comportement |
|---|---|
| MinIO injoignable au démarrage | Message : *"Données indisponibles — connexion au stockage impossible. Nouvelle tentative dans 5 minutes."* — aucune exception non interceptée |
| Aucun Parquet dans les 24h | Cartes de champs : *"Aucune donnée récente"* · Graphiques vides avec message explicite |
| Rapport J-1 absent | Note discrète sous le tableau : *"Données du jour précédent non disponibles."* |

## Rafraîchissement automatique

`dcc.Interval` configuré à 300 000 ms (5 minutes). Déclenche une relecture MinIO en arrière-plan et met à jour le `dcc.Store`. Un compteur visible (`🔄 Mise à jour dans X:XX`) décompte jusqu'au prochain refresh.

## Correspondances de données

### sensor_id → culture

| sensor_id | Culture | Emoji |
|---|---|---|
| `field_1` | Blé | 🌾 |
| `field_2` | Maïs | 🌽 |
| `field_3` | Tournesol | 🌻 |
| `field_4` | Soja | 🌱 |

### plant_stage → libellé affiché

| Valeur interne | Libellé affiché |
|---|---|
| `germination` | Germination |
| `croissance` | En croissance |
| `développement` | En développement |
| `maturité` | Mature |
| `récolte_imminente` | Prêt à être récolté |
| `inconnu` | Phase inconnue |

Le champ `plant_stage` (colonne enrichie par Spark) est utilisé pour l'affichage dans les cartes et le panneau de détail.

## Règles de présentation (UX)

- **Langue** : français courant exclusivement. Aucun terme technique, aucun identifiant interne affiché.
- **Unités** : toujours affichées explicitement (°C, %, mg/kg).
- **Icônes** : emojis Unicode standard uniquement (🌾🌽🌻🌱🌡💧🚨✓⚠). Pas d'images ou d'icônes générées par outil IA.
- **Couleurs** : vert = normal, orange = attention, rouge = alerte. Cohérent sur toute l'interface.

## Tests

Fichier `dashboard/tests/test_data.py`. Fonctions testées sans connexion MinIO (données mockées) :

| Fonction | Ce qui est testé |
|---|---|
| `humanize_alert(code)` | Chaque code → libellé français correct ; code inconnu → code brut |
| `build_field_cards(df)` | 4 champs → 4 dicts avec statut et libellés corrects ; DataFrame vide → message "Aucune donnée récente" |
| `get_stress_label(score)` | 0→`Faible`, 50→`Modéré`, 80→`Élevé` ; bornes exactes |
| `build_detail(df, sensor_id)` | Filtrage correct par sensor_id ; sensor_id absent → dict vide |
| `build_report_table(today, yesterday)` | J-1 présent → flèches tendance ; J-1 None → valeurs J uniquement sans erreur |

Objectif : **12 tests unitaires**.

## Tech Stack

- `dash==2.17.1`
- `plotly==5.22.0`
- `pandas==2.2.2`
- `boto3==1.34.0` (lecture MinIO S3-compatible)
- `pyarrow==16.1.0` (lecture Parquet)
- `pytest==8.2.0` (tests)
