import json
import math
import os
import random
import datetime
import time

from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC                   = os.environ.get('KAFKA_TOPIC', 'sensor_data')
MESSAGES_PER_SECOND     = int(os.environ.get('MESSAGES_PER_SECOND', '1000'))
NUM_FIELDS              = int(os.environ.get('NUM_FIELDS', '4'))
POOL_SIZE               = int(os.environ.get('POOL_SIZE', '50000'))

CULTURES = {1: 'Ble', 2: 'Mais', 3: 'Tournesol', 4: 'Soja'}


def generate_payload(fid, i):
    culture = CULTURES[fid]
    return {
        'sensor_id':             f"load_{fid}_{culture.lower()}",
        'timestamp':             datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'field_id':              fid,
        'culture':               culture,
        'temperature_c':         round(random.uniform(15.0, 42.0), 2),
        'humidity_pct':          round(random.uniform(20.0, 90.0), 2),
        'solar_radiation_wm2':   round(abs(600 * math.sin(2 * math.pi * i / 1000)), 1),
        'rainfall_mm':           round(random.uniform(0.5, 8.0), 1) if random.random() < 0.002 else 0.0,
        'soil_nitrogen_mg_kg':   round(random.uniform(10.0, 100.0), 2),
        'soil_phosphorus_mg_kg': round(random.uniform(10.0, 60.0), 2),
        'soil_potassium_mg_kg':  round(random.uniform(80.0, 250.0), 2),
        'soil_ph':               round(random.uniform(5.5, 7.5), 1),
        'plant_growth_pct':      round(random.uniform(0.0, 100.0), 2),
        'robot_action':          random.choice(['vide', 'arrosage', 'fertilisation', 'recolte']),
        'phase':                 random.choice(['sol_vide', 'en_croissance', 'mature', 'en_recolte']),
        'water_used_L':          round(random.uniform(0.0, 500.0), 1),
        'fertilizer_used_kg':    round(random.uniform(0.0, 50.0), 2),
        'harvest_count':         random.randint(0, 10),
    }


# ── Pré-génération du pool (fait une seule fois au démarrage) ────────────────
print(f"[load-generator] Pré-génération du pool ({POOL_SIZE:,} payloads)…", flush=True)
pool = [
    json.dumps(generate_payload((i % NUM_FIELDS) + 1, i)).encode('utf-8')
    for i in range(POOL_SIZE)
]
print(f"[load-generator] Pool prêt — taille moyenne : {len(pool[0])} octets/msg", flush=True)

# ── Producer confluent-kafka (librdkafka C) ──────────────────────────────────
producer = Producer({
    'bootstrap.servers':  KAFKA_BOOTSTRAP_SERVERS,
    'linger.ms':          20,
    'batch.size':         1048576,      # 1 Mo par batch
    'compression.type':   'lz4',
    'acks':               '1',
    'queue.buffering.max.messages': 500000,
})

print(
    f"[load-generator] Démarrage — cible {MESSAGES_PER_SECOND:,} msg/s → topic '{TOPIC}'",
    flush=True,
)

pool_idx  = 0
pool_len  = len(pool)
sent      = 0
report_at = time.monotonic() + 5.0

while True:
    t0 = time.monotonic()

    for _ in range(MESSAGES_PER_SECOND):
        # produce() est non-bloquant côté C — overhead < 1 µs
        producer.produce(TOPIC, value=pool[pool_idx])
        pool_idx = (pool_idx + 1) % pool_len
        sent += 1

    # poll() déclenche les callbacks de livraison et libère le buffer interne
    producer.poll(0)

    elapsed   = time.monotonic() - t0
    sleep_for = 1.0 - elapsed
    if sleep_for > 0:
        time.sleep(sleep_for)

    now = time.monotonic()
    if now >= report_at:
        print(
            f"[load-generator] boucle {elapsed:.3f}s — "
            f"{'sous' if elapsed < 1 else 'SUR'}-cadence | total : {sent:,}",
            flush=True,
        )
        report_at = now + 5.0
