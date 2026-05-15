import json
import math
import os
import random
import datetime
import time

from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC                   = os.environ.get('KAFKA_TOPIC', 'sensor_data')
MESSAGES_PER_SECOND     = int(os.environ.get('MESSAGES_PER_SECOND', '1000'))
NUM_FIELDS              = int(os.environ.get('NUM_FIELDS', '4'))

CULTURES = {1: 'Ble', 2: 'Mais', 3: 'Tournesol', 4: 'Soja'}

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=10,      # batch pendant 10 ms pour maximiser le débit
    batch_size=65536,
    compression_type='gzip',
)

print(f"[load-generator] Démarrage — cible : {MESSAGES_PER_SECOND} msg/s → topic '{TOPIC}'")

iteration = 0

while True:
    t0 = time.monotonic()

    for _ in range(MESSAGES_PER_SECOND):
        fid     = (iteration % NUM_FIELDS) + 1
        culture = CULTURES[fid]

        payload = {
            'sensor_id':             f"load_{fid}_{culture.lower()}",
            'timestamp':             datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
            'field_id':              fid,
            'culture':               culture,
            'temperature_c':         round(random.uniform(15.0, 42.0), 2),
            'humidity_pct':          round(random.uniform(20.0, 90.0), 2),
            'solar_radiation_wm2':   round(abs(600 * math.sin(2 * math.pi * iteration / 1000)), 1),
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

        producer.send(TOPIC, payload)
        iteration += 1

    producer.flush()
    elapsed = time.monotonic() - t0
    sleep_for = 1.0 - elapsed
    if sleep_for > 0:
        time.sleep(sleep_for)

    print(f"[load-generator] {MESSAGES_PER_SECOND} msg envoyés en {elapsed:.3f}s "
          f"({'sous' if elapsed < 1 else 'sur'}-cadence)")
