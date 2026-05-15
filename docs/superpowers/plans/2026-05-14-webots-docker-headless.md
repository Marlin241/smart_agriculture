# Webots Docker Headless Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Intégrer Webots R2025a en mode headless dans Docker pour supprimer la dépendance à une installation locale de Webots.

**Architecture:** Un service `webots` est ajouté au `docker-compose.yml`, basé sur un `webots/Dockerfile` custom qui copie `worlds/` et `controllers/` dans l'image `cyberbotics/webots:R2025a`. Webots tourne avec `--batch --no-rendering --mode=fast`. L'adresse Kafka est configurable via `KAFKA_BOOTSTRAP_SERVERS` (fallback `localhost:9092` pour usage local).

**Tech Stack:** Docker, cyberbotics/webots:R2025a, kafka-python-ng, docker compose.

---

## Fichiers concernés

| Action  | Fichier                                      | Rôle                                      |
|---------|----------------------------------------------|-------------------------------------------|
| Créer   | `webots/Dockerfile`                          | Image Webots headless autonome            |
| Modifier | `docker-compose.yml`                        | Ajout du service `webots`                 |
| Modifier | `controllers/field_robot/field_robot.py`    | Kafka bootstrap via variable d'environnement |

---

### Task 1 : Créer la branche dédiée

**Files:**
- Aucun fichier modifié — opération git uniquement

- [ ] **Step 1 : Se placer sur main à jour**

```bash
git checkout main
git pull origin main
```

Expected : `Already up to date.` ou liste de commits tirés.

- [ ] **Step 2 : Créer et pousser la branche**

```bash
git checkout -b feat/webots-docker-headless
git push -u origin feat/webots-docker-headless
```

Expected :
```
Switched to a new branch 'feat/webots-docker-headless'
branch 'feat/webots-docker-headless' set up to track 'origin/feat/webots-docker-headless'
```

---

### Task 2 : Configurer Kafka via variable d'environnement

**Files:**
- Modify: `controllers/field_robot/field_robot.py` (ligne 17)

- [ ] **Step 1 : Localiser la ligne à changer**

Ouvrir [controllers/field_robot/field_robot.py](controllers/field_robot/field_robot.py) ligne 17.
La ligne actuelle est :
```python
        bootstrap_servers='localhost:9092',
```

- [ ] **Step 2 : Appliquer la modification**

Remplacer cette ligne par :
```python
        bootstrap_servers=os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
```

`os` est déjà importé en ligne 2 — aucun import supplémentaire.

- [ ] **Step 3 : Vérifier visuellement le bloc KafkaProducer complet**

Le bloc `try` (lignes 14–27) doit ressembler à :
```python
try:
    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        request_timeout_ms=3000,
        max_block_ms=3000,
    )
    KAFKA_ENABLED = True
    print(f"[{robot_name}] Kafka connecté sur {os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')}")
except ImportError:
    print(f"[{robot_name}] kafka-python-ng absent — simulation continue sans streaming Kafka")
except Exception as e:
    print(f"[{robot_name}] Kafka non disponible ({e}) — simulation continue sans streaming")
```

> Note : mettre à jour le `print` de la ligne 23 pour afficher l'adresse réelle utilisée (pas hardcoder `localhost:9092`).

- [ ] **Step 4 : Commiter**

```bash
git add controllers/field_robot/field_robot.py
git commit -m "feat(kafka): lire KAFKA_BOOTSTRAP_SERVERS depuis l'environnement"
```

---

### Task 3 : Créer le Dockerfile Webots

**Files:**
- Create: `webots/Dockerfile`

- [ ] **Step 1 : Créer le répertoire `webots/`**

```bash
mkdir webots
```

- [ ] **Step 2 : Écrire `webots/Dockerfile`**

Contenu exact du fichier :
```dockerfile
FROM cyberbotics/webots:R2025a

RUN pip3 install kafka-python-ng --quiet

COPY worlds/      /app/worlds/
COPY controllers/ /app/controllers/

WORKDIR /app

CMD ["webots", "--batch", "--no-rendering", "--mode=fast", "worlds/agrAI.wbt"]
```

- [ ] **Step 3 : Vérifier que le build réussit**

```bash
docker build -f webots/Dockerfile . -t webots-test
```

Expected : `Successfully built <id>` et `Successfully tagged webots-test:latest`.

> Si l'image `cyberbotics/webots:R2025a` n'existe pas, Docker la téléchargera (~2 Go — prévoir du temps).

- [ ] **Step 4 : Supprimer l'image de test**

```bash
docker rmi webots-test
```

- [ ] **Step 5 : Commiter**

```bash
git add webots/Dockerfile
git commit -m "feat(webots): Dockerfile headless basé sur cyberbotics/webots:R2025a"
```

---

### Task 4 : Ajouter le service `webots` dans docker-compose.yml

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1 : Ajouter le service dans `docker-compose.yml`**

Dans la section `services:`, ajouter après le service `kafka-consumer` :

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

- [ ] **Step 2 : Valider la syntaxe du fichier**

```bash
docker compose config
```

Expected : affichage de la configuration complète sans erreur. Si erreur YAML, vérifier l'indentation (2 espaces, pas de tabulations).

- [ ] **Step 3 : Commiter**

```bash
git add docker-compose.yml
git commit -m "feat(docker): ajouter service webots headless dans docker-compose"
```

---

### Task 5 : Test d'intégration

**Files:**
- Aucune modification — validation uniquement

- [ ] **Step 1 : Démarrer tous les services**

```bash
docker compose up -d
```

Expected : tous les services passent en `Up`, y compris `webots`.

- [ ] **Step 2 : Vérifier que Webots démarre sans erreur**

```bash
docker compose logs webots --follow
```

Attendre ~30 secondes. Expected : messages du type :
```
[Supervisor] Démarrage de la ferme intelligente 4 champs...
[Ble #1] Kafka connecté sur kafka:29092
[Mais #2] Kafka connecté sur kafka:29092
...
```

Si tu vois `Kafka non disponible` au lieu de `Kafka connecté`, Kafka n'était pas encore prêt — le `restart: on-failure` relancera Webots automatiquement.

Quitter le suivi avec `Ctrl+C`.

- [ ] **Step 3 : Vérifier que le topic `sensor_data` reçoit des messages**

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic sensor_data \
  --from-beginning \
  --max-messages 5
```

Expected : 5 lignes JSON du type :
```json
{"sensor_id": "field_1_ble", "timestamp": "2026-05-14T...", "field_id": 1, ...}
```

> Si le topic est vide, attendre 200 itérations de simulation (~quelques secondes en mode fast) car les robots publient toutes les 200 itérations.

- [ ] **Step 4 : Arrêter les services**

```bash
docker compose down
```

---

### Task 6 : Commiter le document de spec et ouvrir la PR

**Files:**
- Modify: `docs/superpowers/specs/2026-05-14-webots-docker-headless-design.md` (déjà créé, à commiter)
- Modify: `docs/superpowers/plans/2026-05-14-webots-docker-headless.md` (ce fichier)

- [ ] **Step 1 : Commiter les docs**

```bash
git add docs/
git commit -m "docs: spec et plan d'impl pour webots headless docker"
```

- [ ] **Step 2 : Pousser la branche**

```bash
git push origin feat/webots-docker-headless
```

- [ ] **Step 3 : Créer la PR**

```bash
gh pr create \
  --title "feat(webots): intégration Webots R2025a headless dans Docker" \
  --body "## Summary
- Ajout de \`webots/Dockerfile\` basé sur \`cyberbotics/webots:R2025a\`
- Service \`webots\` ajouté au \`docker-compose.yml\` (headless, mode fast)
- \`KAFKA_BOOTSTRAP_SERVERS\` configurable via env var (fallback \`localhost:9092\`)

## Test plan
- [ ] \`docker compose up -d\` → tous les services Up
- [ ] \`docker compose logs webots\` → Kafka connecté sur kafka:29092
- [ ] \`kafka-console-consumer\` → messages JSON reçus sur sensor_data" \
  --base main
```

---

## Notes importantes

**Webots et X display** : `--no-rendering` désactive le rendu OpenGL. Si le conteneur échoue avec `cannot open display`, ajouter `xvfb-run` devant la commande dans le Dockerfile :
```dockerfile
CMD ["xvfb-run", "webots", "--batch", "--no-rendering", "--mode=fast", "worlds/agrAI.wbt"]
```
et installer `xvfb` via `RUN apt-get install -y xvfb`.

**Taille de l'image** : `cyberbotics/webots:R2025a` pèse ~2 Go. Le premier `docker compose up --build` sera long.

**Compatibilité locale** : Sans `KAFKA_BOOTSTRAP_SERVERS` dans l'environnement, le contrôleur utilise toujours `localhost:9092`. Webots local continue de fonctionner sans modification.
