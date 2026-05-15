# Design : Webots en mode headless dans Docker

**Date** : 2026-05-14
**Branche** : `feat/webots-docker-headless`
**Statut** : Approuvé

---

## Contexte

Le projet smart_agriculture utilise Webots R2025a comme simulateur de ferme.
Actuellement, Webots est une dépendance externe installée sur la machine hôte.
L'objectif est de l'intégrer dans le `docker-compose.yml` existant afin de rendre
l'environnement entièrement autonome, sans installation locale de Webots.

---

## Approche retenue

**Dockerfile custom (option B)** — image Docker autonome basée sur
`cyberbotics/webots:R2025a` qui embarque le monde et les controllers.
Webots tourne en mode `--batch --no-rendering --mode=fast` (headless + vitesse maximale).

---

## Architecture

```
docker-compose.yml
  └── webots (nouveau service)
        ├── build: webots/Dockerfile
        ├── depends_on: kafka (condition: service_healthy)
        ├── KAFKA_BOOTSTRAP_SERVERS=kafka:29092
        ├── restart: on-failure
        └── networks: smart-farm-net

webots/
  └── Dockerfile
        ├── FROM cyberbotics/webots:R2025a
        ├── RUN pip3 install kafka-python-ng
        ├── COPY worlds/      → /app/worlds/
        ├── COPY controllers/ → /app/controllers/
        ├── WORKDIR /app
        └── CMD webots --batch --no-rendering --mode=fast worlds/agrAI.wbt
```

Les 4 `field_robot` et le `field_supervisor` tournent dans le même processus
Webots. L'IPC via `/tmp/farm_field_{fid}.json` fonctionne sans modification
(tous les controllers partagent le même `/tmp` intra-conteneur).

---

## Fichiers à créer / modifier

### Créer : `webots/Dockerfile`

```dockerfile
FROM cyberbotics/webots:R2025a

RUN pip3 install kafka-python-ng --quiet

COPY worlds/      /app/worlds/
COPY controllers/ /app/controllers/

WORKDIR /app

CMD ["webots", "--batch", "--no-rendering", "--mode=fast", "worlds/agrAI.wbt"]
```

### Modifier : `docker-compose.yml`

Ajouter le service `webots` :

```yaml
webots:
  build:
    context: .
    dockerfile: webots/Dockerfile
  depends_on:
    kafka:
      condition: service_healthy
  environment:
    KAFKA_BOOTSTRAP_SERVERS: kafka:29092
  restart: on-failure
  networks: [smart-farm-net]
```

### Modifier : `controllers/field_robot/field_robot.py` (1 ligne)

```python
# Ligne 17 — avant
bootstrap_servers='localhost:9092',

# Ligne 17 — après
bootstrap_servers=os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
```

`os` est déjà importé. Le fallback `localhost:9092` préserve la compatibilité
avec un Webots lancé localement hors Docker.

---

## Ce qui ne change pas

- `controllers/field_supervisor/field_supervisor.py` — aucune modification.
  Les appels Webots API (sol, LEDs, plantes) fonctionnent en headless ;
  ils manipulent le graphe de scène en mémoire sans rendu visible.
- `worlds/agrAI.wbt` — aucune modification.
- Tous les autres services Docker (kafka, minio, spark, airflow, dashboard).

---

## Comportement en mode fast

Webots ne respecte plus le temps réel : la simulation avance aussi vite que
le CPU le permet. Les timestamps dans les payloads JSON restent basés sur
`datetime.datetime.now()` (horloge réelle), donc les données Kafka gardent
des timestamps cohérents même si la simulation tourne plus vite que le temps réel.

---

## Compatibilité locale

Grâce à la variable d'environnement avec fallback, il reste possible de lancer
Webots localement (hors Docker) et de produire sur `localhost:9092` sans
aucune modification de code.

---

## Hors scope

- Mode VNC / visualisation 3D depuis Docker (prévu pour une itération future).
- Refactoring du supervisor pour supprimer les appels visuels en headless.
