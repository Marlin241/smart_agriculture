# Spec — Sous-système A : Kafka Producer + Refactoring Webots

**Date :** 2026-05-06
**Périmètre :** Intégration Kafka dans les contrôleurs Webots, refactoring des données capteurs, infrastructure Docker initiale.

---

## 1. Objectif

Connecter la simulation Webots au pipeline Big Data en publiant les données capteurs brutes dans Kafka, toutes les 200 itérations par robot (~6 secondes). Simultanément, refactoriser `field_robot.py` pour qu'il ne produise que des lectures capteurs pures — tous les calculs dérivés (stress, santé plante, fertilité sol, économie) sont retirés du payload et seront traités par Spark.

---

## 2. Architecture

```
Webots (hôte)                    Docker Compose
──────────────                   ──────────────────────────────────
field_robot_1.py  ─┐
field_robot_2.py  ──┤─ kafka-python ──► Kafka (localhost:9092)
field_robot_3.py  ──┤                        topic: sensor_data
field_robot_4.py  ─┘

field_supervisor.py  ← lit /tmp (inchangé fonctionnellement, fix cross-platform)
```

Webots tourne sur la machine hôte. Kafka, Zookeeper et Kafka-UI tournent dans Docker Compose. La connexion hôte → Docker se fait sur `localhost:9092`, ce qui fonctionne identiquement sur Windows, macOS et Linux.

---

## 3. Nouveau schéma des messages Kafka

**Topic :** `sensor_data`
**Format :** JSON
**Fréquence :** toutes les 200 itérations par robot (≈ 6 secondes), soit ≈ 40 messages/minute au total (4 robots)

```json
{
  "sensor_id":              "field_1_ble",
  "timestamp":              "2026-05-06T14:32:00Z",
  "field_id":               1,
  "culture":                "Ble",

  "temperature_c":          21.4,
  "humidity_pct":           55.2,
  "solar_radiation_wm2":    620.0,
  "rainfall_mm":            0.0,

  "soil_nitrogen_mg_kg":    62.1,
  "soil_phosphorus_mg_kg":  28.4,
  "soil_potassium_mg_kg":   41.7,
  "soil_ph":                6.4,

  "plant_growth_pct":       34.5,

  "robot_action":           "arrosage",
  "phase":                  "en_croissance",
  "water_used_L":           12.4,
  "fertilizer_used_kg":     0.08,
  "harvest_count":          0
}
```

### Champs supprimés (calculs déportés vers Spark)

| Champ supprimé     | Pourquoi                              |
|--------------------|---------------------------------------|
| `stress`           | Calculé par Spark depuis hum + nit    |
| `plant_health`     | Calculé par Spark                     |
| `soil_fertility`   | Calculé par Spark                     |
| `total_revenue`    | Agrégation Spark                      |
| `revenue_per_cycle`| Agrégation Spark                      |
| `total_yield_kg`   | Agrégation Spark                      |
| `price_per_kg`     | Table de référence statique dans Spark |

### Nouveaux capteurs simulés

| Champ                   | Simulation                                                         |
|-------------------------|--------------------------------------------------------------------|
| `soil_phosphorus_mg_kg` | Taux de consommation propre par culture, variation aléatoire ±0.5  |
| `soil_potassium_mg_kg`  | Idem, taux différent de P                                          |
| `solar_radiation_wm2`   | Base par culture + variation sinusoïdale simulant cycle jour/nuit  |
| `rainfall_mm`           | Événements aléatoires rares (probabilité par profil, 0 sinon)      |

---

## 4. Modifications de `field_robot.py`

### Profils de culture — ajouts

Chaque profil `PROFILES[fid]` reçoit 6 nouvelles clés (valeurs réalistes par culture) :

```python
# Blé (1)       : sol tempéré, ensoleillement modéré
# Maïs (2)      : fort besoin en K, ensoleillement élevé
# Tournesol (3) : sol riche en P, ensoleillement maximal
# Soja (4)      : fixateur d'azote, besoins P/K modérés

'phosphorus':   35 / 22 / 45 / 28,   # mg/kg initial (Blé/Maïs/Tournesol/Soja)
'potassium':   180 /120 /200 /150,   # mg/kg initial
'phos_consume': 0.012/0.018/0.010/0.015, # consommation par itération
'pot_consume':  0.020/0.032/0.015/0.025, # consommation par itération
'solar_base':   420 / 580 / 650 /520,    # W/m² de base
'rain_prob':    0.002/0.001/0.001/0.003, # probabilité pluie par itération
```

### État interne — ajouts

```python
state['phos']     = float(p['phosphorus'])
state['pot']      = float(p['potassium'])
state['solar']    = float(p['solar_base'])
state['rain']     = 0.0
```

### Fix cross-platform

```python
import tempfile
FIELD_FILE = os.path.join(tempfile.gettempdir(), f'farm_field_{fid}.json')
```

### Intégration Kafka

```python
from kafka import KafkaProducer
import json, datetime

KAFKA_ENABLED = False
producer = None
try:
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    KAFKA_ENABLED = True
except Exception as e:
    print(f"[{robot_name}] Kafka non disponible: {e}")
```

Publication dans la boucle principale toutes les 200 itérations :

```python
if KAFKA_ENABLED and iteration % 200 == 0:
    try:
        producer.send('sensor_data', payload)
    except Exception as e:
        print(f"[{culture} #{fid}] Kafka send error: {e}")
```

### Calcul interne de stress conservé

Le calcul de `stress` reste dans l'état interne du robot — il pilote les décisions de la machine à états (quand arroser, quand fertiliser). Il n'est simplement plus exporté dans le payload Kafka ni dans le fichier JSON.

---

## 5. Modifications de `field_supervisor.py`

Uniquement le fix cross-platform :

```python
import tempfile
path = os.path.join(tempfile.gettempdir(), f'farm_field_{fid}.json')
```

Aucune autre modification.

---

## 6. `docker-compose.yml`

Services pour le sous-système A uniquement. Les services Spark, Airflow et Dashboard seront ajoutés dans les specs suivantes.

```yaml
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on: [zookeeper]
    ports:
      - "9092:9092"   # accès depuis l'hôte (Webots)
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      # Deux listeners : un pour l'hôte, un pour le réseau Docker interne
      KAFKA_LISTENERS: PLAINTEXT_HOST://0.0.0.0:9092,PLAINTEXT://0.0.0.0:29092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT_HOST://localhost:9092,PLAINTEXT://kafka:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT_HOST:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    depends_on: [kafka]
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092  # réseau Docker interne
```

Kafka-UI accessible sur `http://localhost:8080` — permet de vérifier que les messages arrivent bien pendant le développement.

---

## 7. `requirements_webots.txt`

Installé sur la machine hôte (pas dans Docker) :

```
kafka-python==2.0.2
```

Installation : `pip install -r requirements_webots.txt`

---

## 8. Stratégie d'erreur

- **Kafka indisponible au démarrage** : `KAFKA_ENABLED = False`, log d'avertissement, Webots tourne normalement
- **Erreur réseau ponctuelle** : `try/except` silencieux autour de chaque `producer.send()`, log console uniquement
- **Pas de retry** : inutile dans ce contexte — si Kafka redémarre, relancer Webots suffit
- **Fichier JSON /tmp** : écriture atomique via `os.replace()` conservée, chemin corrigé cross-platform

---

## 9. Fichiers créés / modifiés

| Fichier | Action |
|---|---|
| `controllers/field_robot/field_robot.py` | Modifié |
| `controllers/field_supervisor/field_supervisor.py` | Modifié (tempfile uniquement) |
| `docker-compose.yml` | Créé |
| `requirements_webots.txt` | Créé |
