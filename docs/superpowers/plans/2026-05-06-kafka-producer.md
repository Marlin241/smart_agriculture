# Kafka Producer + Refactoring Webots — Plan d'implémentation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Connecter les 4 robots Webots à Kafka en publiant des données capteurs brutes (NPK complet, météo, état robot) toutes les 200 itérations, tout en supprimant les calculs dérivés du contrôleur.

**Architecture:** Chaque `field_robot.py` construit un payload JSON à chaque itération (pour le superviseur Webots) et le publie dans Kafka toutes les 200 itérations. Kafka, Zookeeper et Kafka-UI tournent dans Docker Compose. La connexion hôte→Docker se fait sur `localhost:9092`. Si Kafka est absent, Webots continue normalement (fail-silent).

**Tech Stack:** Python 3, kafka-python 2.0.2, Docker Compose, confluentinc/cp-kafka:7.5.0, confluentinc/cp-zookeeper:7.5.0, provectuslabs/kafka-ui

---

### Task 1 : Infrastructure Docker

**Files:**
- Create: `docker-compose.yml`
- Create: `requirements_webots.txt`

- [ ] **Étape 1 : Créer `requirements_webots.txt`** à la racine du projet

```
kafka-python==2.0.2
```

- [ ] **Étape 2 : Créer `docker-compose.yml`** à la racine du projet

```yaml
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

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

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    depends_on:
      - kafka
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
```

- [ ] **Étape 3 : Valider la syntaxe YAML**

```bash
docker compose config
```

Résultat attendu : affichage du YAML normalisé complet, sans erreur.

- [ ] **Étape 4 : Démarrer les services**

```bash
docker compose up -d
```

Attendre 20 secondes puis vérifier :

```bash
docker ps
```

Résultat attendu : les 3 services (`zookeeper`, `kafka`, `kafka-ui`) ont le statut `running`. Ouvrir http://localhost:8080 — la page doit afficher le cluster "local".

- [ ] **Étape 5 : Installer la dépendance Webots sur l'hôte**

```bash
pip install -r requirements_webots.txt
```

Résultat attendu : `Successfully installed kafka-python-2.0.2`

- [ ] **Étape 6 : Commit**

```bash
git add docker-compose.yml requirements_webots.txt
git commit -m "feat: add Kafka + Zookeeper + Kafka-UI docker-compose"
```

---

### Task 2 : Fix cross-platform dans `field_supervisor.py`

**Files:**
- Modify: `controllers/field_supervisor/field_supervisor.py`

Le superviseur lit le fichier JSON produit par chaque robot. Deux corrections nécessaires :
1. Remplacer le chemin `/tmp/` par `tempfile.gettempdir()` (cross-platform).
2. Mettre à jour les clés JSON lues : `action` → `robot_action` et `growth` → `plant_growth_pct`, pour correspondre au nouveau schéma de `field_robot.py`.

- [ ] **Étape 1 : Ajouter `tempfile` aux imports**

Remplacer la ligne :
```python
from controller import Supervisor
import json, os
```

Par :
```python
from controller import Supervisor
import json, os, tempfile
```

- [ ] **Étape 2 : Corriger le chemin dans `read_field_data`**

Remplacer :
```python
def read_field_data(fid):
    """
    Lit le fichier JSON dédié d'un champ (écriture atomique par le robot).
    Chaque robot écrit dans /tmp/farm_field_<fid>.json via os.replace(),
    donc jamais de JSON partiel. 3 tentatives pour robustesse.
    """
    path = f'/tmp/farm_field_{fid}.json'
    for _ in range(3):
        try:
            with open(path, 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            pass
    return None
```

Par :
```python
def read_field_data(fid):
    path = os.path.join(tempfile.gettempdir(), f'farm_field_{fid}.json')
    for _ in range(3):
        try:
            with open(path, 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            pass
    return None
```

- [ ] **Étape 3 : Mettre à jour les clés lues dans la boucle principale**

Dans la boucle `while supervisor.step(ts) != -1:`, remplacer :
```python
            action = info.get('action', 'vide')
            growth = info.get('growth', 0.0)
            phase  = info.get('phase', 'sol_vide')
```

Par :
```python
            action = info.get('robot_action', 'vide')
            growth = info.get('plant_growth_pct', 0.0)
            phase  = info.get('phase', 'sol_vide')
```

- [ ] **Étape 4 : Commit**

```bash
git add controllers/field_supervisor/field_supervisor.py
git commit -m "fix: cross-platform tempfile path and updated JSON field names in supervisor"
```

---

### Task 3 : Imports, PROFILES, état initial et FIELD_FILE dans `field_robot.py`

**Files:**
- Modify: `controllers/field_robot/field_robot.py`

- [ ] **Étape 1 : Mettre à jour les imports**

Remplacer :
```python
from controller import Robot
import json, os, sys, random
```

Par :
```python
from controller import Robot
import json, os, sys, random, math, datetime, tempfile
```

- [ ] **Étape 2 : Remplacer le dict `PROFILES` en entier**

```python
PROFILES = {
    1: {'culture': 'Ble',       'hum': 55, 'nit': 62, 'temp': 21,
        'evap': 0.025, 'nit_consume': 0.022, 'growth_time': 400,
        'phosphorus': 35,  'potassium': 180,
        'phos_consume': 0.012, 'pot_consume': 0.020,
        'solar_base': 420, 'rain_prob': 0.002},
    2: {'culture': 'Mais',      'hum': 38, 'nit': 28, 'temp': 29,
        'evap': 0.038, 'nit_consume': 0.038, 'growth_time': 500,
        'phosphorus': 22,  'potassium': 120,
        'phos_consume': 0.018, 'pot_consume': 0.032,
        'solar_base': 580, 'rain_prob': 0.001},
    3: {'culture': 'Tournesol', 'hum': 52, 'nit': 78, 'temp': 23,
        'evap': 0.018, 'nit_consume': 0.015, 'growth_time': 350,
        'phosphorus': 45,  'potassium': 200,
        'phos_consume': 0.010, 'pot_consume': 0.015,
        'solar_base': 650, 'rain_prob': 0.001},
    4: {'culture': 'Soja',      'hum': 44, 'nit': 32, 'temp': 25,
        'evap': 0.028, 'nit_consume': 0.032, 'growth_time': 450,
        'phosphorus': 28,  'potassium': 150,
        'phos_consume': 0.015, 'pot_consume': 0.025,
        'solar_base': 520, 'rain_prob': 0.003},
}
```

- [ ] **Étape 3 : Mettre à jour le dict `state`**

Remplacer :
```python
state = {
    'hum'   : float(p['hum']),
    'nit'   : float(p['nit']),
    'growth': 0.0,
    'temp'  : float(p['temp']),
    'ph'    : round(6.2 + fid * 0.2 + random.uniform(-0.1, 0.1), 1),
    'stress': 0.0,
}
```

Par :
```python
state = {
    'hum'   : float(p['hum']),
    'nit'   : float(p['nit']),
    'growth': 0.0,
    'temp'  : float(p['temp']),
    'ph'    : round(6.2 + fid * 0.2 + random.uniform(-0.1, 0.1), 1),
    'stress': 0.0,
    'phos'  : float(p['phosphorus']),
    'pot'   : float(p['potassium']),
    'solar' : float(p['solar_base']),
    'rain'  : 0.0,
}
```

- [ ] **Étape 4 : Corriger `FIELD_FILE`**

Remplacer :
```python
FIELD_FILE = f'/tmp/farm_field_{fid}.json'
```

Par :
```python
FIELD_FILE = os.path.join(tempfile.gettempdir(), f'farm_field_{fid}.json')
```

- [ ] **Étape 5 : Supprimer les variables de tracking économique inutilisées**

Remplacer :
```python
harvest_count      = 0
total_yield_kg     = 0.0
total_revenue      = 0.0
water_used_L       = 0.0
fertilizer_used_kg = 0.0
harvest_done       = False
```

Par :
```python
harvest_count      = 0
water_used_L       = 0.0
fertilizer_used_kg = 0.0
```

- [ ] **Étape 6 : Commit**

```bash
git add controllers/field_robot/field_robot.py
git commit -m "refactor: update PROFILES with P/K/solar/rain, fix tempfile path, remove revenue tracking"
```

---

### Task 4 : Simulation physique P/K, rayonnement solaire, pluie

**Files:**
- Modify: `controllers/field_robot/field_robot.py`

- [ ] **Étape 1 : Étendre la consommation des nutriments sol en croissance**

Dans la section `# ── 1. Évolution naturelle`, remplacer :
```python
    if phase == 'en_croissance':
        state['nit'] = max(0.0, state['nit'] - nit_consume + random.uniform(-0.01, 0.01))
```

Par :
```python
    if phase == 'en_croissance':
        state['nit']  = max(0.0, state['nit']  - nit_consume          + random.uniform(-0.01,  0.01))
        state['phos'] = max(0.0, state['phos'] - p['phos_consume']    + random.uniform(-0.005, 0.005))
        state['pot']  = max(0.0, state['pot']  - p['pot_consume']     + random.uniform(-0.008, 0.008))
```

- [ ] **Étape 2 : Ajouter le rayonnement solaire et la pluie juste après**

Immédiatement après le bloc `if phase == 'en_croissance':` (toujours dans la section évolution naturelle), ajouter :

```python
    # Cycle jour/nuit sinusoïdal (période = 1000 itérations) + bruit
    state['solar'] = max(0.0,
        p['solar_base'] * (0.5 + 0.5 * math.sin(2 * math.pi * iteration / 1000.0))
        + random.uniform(-20, 20))

    # Événements pluie rares selon profil culture
    state['rain'] = round(random.uniform(0.5, 8.0), 1) if random.random() < p['rain_prob'] else 0.0
```

- [ ] **Étape 3 : Commit**

```bash
git add controllers/field_robot/field_robot.py
git commit -m "feat: add P/K consumption, solar radiation cycle and rainfall simulation"
```

---

### Task 5 : Payload refactorisé et phase récolte simplifiée

**Files:**
- Modify: `controllers/field_robot/field_robot.py`

- [ ] **Étape 1 : Simplifier le bloc `elif phase == 'en_recolte':`**

Remplacer :
```python
        elif phase == 'en_recolte':
            rf           = max(0.5, min(1.3,
                             1.0 - state['stress'] / 250.0
                             + (0.08 if state['nit'] > 50 else 0)
                             + (0.05 if state['hum'] > 50 else 0)))
            yield_kg     = p['yield_base'] * rf * random.uniform(0.92, 1.08)
            revenue      = yield_kg * p['price_per_kg']
            total_yield_kg  += yield_kg
            total_revenue   += revenue
            harvest_count   += 1
            print(f"[{p['culture']} #{fid}] Recolte #{harvest_count}: "
                  f"{yield_kg:.0f}kg -> {revenue:.2f}EUR  "
                  f"(stress={state['stress']:.1f})")

            # Sol appauvri par la récolte — plancher réaliste
            state['nit']    = max(28.0, state['nit'] - random.uniform(10, 18))
            state['hum']    = max(38.0, state['hum'] - random.uniform(4,  8))
            state['growth'] = 0.0
            harvest_done    = True

            # Signal visuel bref au supervisor (LED verte) puis redémarrage immédiat
            phase          = 'sol_vide'
            current_action = 'recolte_done'
            action_timer   = 8   # très court : juste pour que le supervisor voie le signal
```

Par :
```python
        elif phase == 'en_recolte':
            harvest_count += 1
            print(f"[{p['culture']} #{fid}] Recolte #{harvest_count} (calculs economiques -> Spark)")
            state['nit']    = max(28.0, state['nit'] - random.uniform(10, 18))
            state['hum']    = max(38.0, state['hum'] - random.uniform(4,  8))
            state['growth'] = 0.0
            phase          = 'sol_vide'
            current_action = 'recolte_done'
            action_timer   = 8
```

- [ ] **Étape 2 : Nettoyer le bloc `recolte_done`**

Remplacer :
```python
    if action_timer == 0 and current_action == 'recolte_done':
        harvest_done   = False
        current_action = 'vide'
```

Par :
```python
    if action_timer == 0 and current_action == 'recolte_done':
        current_action = 'vide'
```

- [ ] **Étape 3 : Remplacer le bloc d'export JSON par le nouveau payload**

Remplacer tout le bloc `# ── 7. Export atomique ...` jusqu'à la fin du `try/except` :

```python
    # ── 7. Export atomique (données riches pour le dashboard) ─────────────────
    try:
        payload = {
            # Identité
            'culture'           : p['culture'],
            'fid'               : fid,
            # État courant
            'action'            : current_action,
            'phase'             : phase,
            'growth'            : round(state['growth'], 2),
            # Capteurs sol (données dashboard)
            'hum'               : round(state['hum'], 2),
            'nit'               : round(state['nit'], 2),
            'temp'              : round(state['temp'], 2),
            'ph'                : round(state['ph'], 1),
            'stress'            : round(state['stress'], 2),
            # Santé plante (0-100, calculé)
            'plant_health'      : round(max(0, 100 - state['stress'] * 1.5
                                            - max(0, 30 - state['hum']) * 0.5
                                            - max(0, 20 - state['nit']) * 0.8), 1),
            # Fertilité sol (0-100)
            'soil_fertility'    : round(min(100, state['nit'] * 0.6 + state['hum'] * 0.4), 1),
            # Ressources consommées
            'water_used_L'      : round(water_used_L, 1),
            'fertilizer_used_kg': round(fertilizer_used_kg, 2),
            # Économie
            'harvest_count'     : harvest_count,
            'total_yield_kg'    : round(total_yield_kg, 1),
            'total_revenue'     : round(total_revenue, 2),
            'revenue_per_cycle' : round(total_revenue / max(1, harvest_count), 2),
            'price_per_kg'      : p['price_per_kg'],
            # Flag récolte
            'harvest_done'      : harvest_done,
        }
        tmp = FIELD_FILE + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(payload, f)
        os.replace(tmp, FIELD_FILE)
    except Exception as e:
        print(f"[{p['culture']} #{fid}] Erreur ecriture: {e}")
```

Par :

```python
    # ── 7. Payload capteurs bruts (IPC supervisor + Kafka) ────────────────────
    payload = {
        'sensor_id':             f"field_{fid}_{p['culture'].lower()}",
        'timestamp':             datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'field_id':              fid,
        'culture':               p['culture'],
        'temperature_c':         round(state['temp'], 2),
        'humidity_pct':          round(state['hum'], 2),
        'solar_radiation_wm2':   round(state['solar'], 1),
        'rainfall_mm':           state['rain'],
        'soil_nitrogen_mg_kg':   round(state['nit'], 2),
        'soil_phosphorus_mg_kg': round(state['phos'], 2),
        'soil_potassium_mg_kg':  round(state['pot'], 2),
        'soil_ph':               round(state['ph'], 1),
        'plant_growth_pct':      round(state['growth'], 2),
        'robot_action':          current_action,
        'phase':                 phase,
        'water_used_L':          round(water_used_L, 1),
        'fertilizer_used_kg':    round(fertilizer_used_kg, 2),
        'harvest_count':         harvest_count,
    }
    try:
        tmp = FIELD_FILE + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(payload, f)
        os.replace(tmp, FIELD_FILE)
    except Exception as e:
        print(f"[{p['culture']} #{fid}] Erreur ecriture: {e}")
```

- [ ] **Étape 4 : Mettre à jour le log console toutes les 200 itérations**

Remplacer :
```python
    if iteration % 200 == 0:
        print(f"[{p['culture']} #{fid}] phase={phase:<18} action={current_action:<14} "
              f"hum={state['hum']:5.1f}% nit={state['nit']:5.1f} "
              f"growth={state['growth']:5.1f}% stress={state['stress']:4.1f} "
              f"timer={action_timer:3d}")
```

Par :
```python
    if iteration % 200 == 0:
        print(f"[{p['culture']} #{fid}] phase={phase:<18} action={current_action:<14} "
              f"hum={state['hum']:5.1f}% nit={state['nit']:5.1f} "
              f"phos={state['phos']:5.1f} pot={state['pot']:5.1f} "
              f"growth={state['growth']:5.1f}% solar={state['solar']:5.0f}W/m² "
              f"stress={state['stress']:4.1f} timer={action_timer:3d}")
```

- [ ] **Étape 5 : Commit**

```bash
git add controllers/field_robot/field_robot.py
git commit -m "refactor: new sensor payload schema, remove derived calculations from field_robot"
```

---

### Task 6 : Intégration Kafka producer (fail-silent)

**Files:**
- Modify: `controllers/field_robot/field_robot.py`

- [ ] **Étape 1 : Ajouter l'initialisation Kafka après le check `robot_name`**

Juste après le bloc :
```python
if not robot_name.startswith('robot_field'):
    sys.exit(0)
```

Ajouter :
```python
# Kafka producer optionnel — Webots continue si Kafka n'est pas démarré
KAFKA_ENABLED = False
producer = None
try:
    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    KAFKA_ENABLED = True
    print(f"[{robot_name}] Kafka connecté sur localhost:9092")
except Exception as e:
    print(f"[{robot_name}] Kafka non disponible: {e} — simulation continue sans streaming")
```

- [ ] **Étape 2 : Ajouter la publication Kafka après l'écriture JSON**

Dans la boucle principale, à la fin du bloc `# ── 7. Payload capteurs bruts`, après le `try/except` d'écriture JSON, ajouter :

```python
    if KAFKA_ENABLED and iteration % 200 == 0:
        try:
            producer.send('sensor_data', payload)
        except Exception as e:
            print(f"[{p['culture']} #{fid}] Kafka send error: {e}")
```

- [ ] **Étape 3 : Commit**

```bash
git add controllers/field_robot/field_robot.py
git commit -m "feat: add fail-silent Kafka producer to field_robot controllers"
```

---

### Task 7 : Vérification end-to-end

**Files:** aucun — validation uniquement

- [ ] **Étape 1 : Démarrer l'infrastructure**

```bash
docker compose up -d
docker compose ps
```

Résultat attendu : `zookeeper`, `kafka` et `kafka-ui` apparaissent dans la liste avec le statut `Up`.

- [ ] **Étape 2 : Lancer Webots et vérifier la connexion Kafka**

Ouvrir Webots, charger `worlds/agrAI.wbt`, démarrer la simulation.

Dans la console Webots, chaque robot doit afficher l'une des deux lignes suivantes :
- `[robot_field_1] Kafka connecté sur localhost:9092` ✅
- `[robot_field_1] Kafka non disponible: ... — simulation continue sans streaming` → relancer `docker compose up -d` et redémarrer Webots.

- [ ] **Étape 3 : Vérifier les messages dans Kafka-UI**

Ouvrir http://localhost:8080. Dans **Topics**, le topic `sensor_data` doit apparaître dans les 30 secondes.

Cliquer sur `sensor_data` → **Messages**. Un message doit ressembler à :
```json
{
  "sensor_id": "field_1_ble",
  "timestamp": "2026-05-06T10:15:30Z",
  "field_id": 1,
  "culture": "Ble",
  "temperature_c": 21.4,
  "humidity_pct": 55.2,
  "solar_radiation_wm2": 210.0,
  "rainfall_mm": 0.0,
  "soil_nitrogen_mg_kg": 61.8,
  "soil_phosphorus_mg_kg": 34.9,
  "soil_potassium_mg_kg": 179.5,
  "soil_ph": 6.4,
  "plant_growth_pct": 0.0,
  "robot_action": "vide",
  "phase": "sol_vide",
  "water_used_L": 0.0,
  "fertilizer_used_kg": 0.0,
  "harvest_count": 0
}
```

Vérifier l'**absence** des champs suivants (s'ils apparaissent, un bug subsiste) :
`stress`, `plant_health`, `soil_fertility`, `total_revenue`, `hum`, `nit`, `temp`, `growth`, `action`

- [ ] **Étape 4 : Arrêter les services**

```bash
docker compose down
```
