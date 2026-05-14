from controller import Robot
import json, os, sys, random, math, datetime, tempfile

robot      = Robot()
timestep   = int(robot.getBasicTimeStep())
robot_name = robot.getName()

if not robot_name.startswith('robot_field'):
    sys.exit(0)

# Kafka producer optionnel — Webots continue si Kafka n'est pas démarré
KAFKA_ENABLED = False
producer = None
try:
    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        request_timeout_ms=3000,   # abandonne après 3 s si Kafka ne répond pas
        max_block_ms=3000,
    )
    KAFKA_ENABLED = True
    print(f"[{robot_name}] Kafka connecté sur {os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')}")
except ImportError:
    print(f"[{robot_name}] kafka-python-ng absent — simulation continue sans streaming Kafka")
except Exception as e:
    print(f"[{robot_name}] Kafka non disponible ({e}) — simulation continue sans streaming")

fid = int(robot_name.split('_')[-1])

# ── Profils par culture ───────────────────────────────────────────────────────
# Champs volontairement très différents pour le dashboard :
#   Champ 1 (Blé)       : sol moyen, température fraîche, rendement stable
#   Champ 2 (Maïs)      : sol pauvre en azote, chaleur, forte évaporation → plus stressé
#   Champ 3 (Tournesol) : sol riche, faible évaporation, très rentable
#   Champ 4 (Soja)      : sol pauvre, rendement modeste mais bon prix
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

p           = PROFILES[fid]
evap        = p['evap']
nit_consume = p['nit_consume']

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

led = robot.getDevice('status_led')
LED_COLORS = {
    'vide'         : 0x00FF00,
    'arrosage'     : 0x0000FF,
    'fertilisation': 0xFF8800,
    'plantation'   : 0x00FF88,
    'recolte'      : 0xFFFF00,
    'recolte_done' : 0x00FF00,
}

FIELD_FILE = os.path.join(tempfile.gettempdir(), f'farm_field_{fid}.json')

harvest_count      = 0
water_used_L       = 0.0
fertilizer_used_kg = 0.0

# ════════════════════════════════════════════════════════════════════
# MACHINE À ÉTATS — RÈGLE UNIQUE :
#   action_timer > 0  → on est EN TRAIN de faire quelque chose
#   action_timer == 0 → action terminée, on DÉCIDE la prochaine
#
# Les transitions se font TOUJOURS dans le bloc "action_timer == 0".
# On ne peut jamais rester bloqué car chaque branche fixe soit un
# nouveau timer, soit passe à 'vide' qui déclenche une décision.
# ════════════════════════════════════════════════════════════════════

# PHASES : sol_vide / plantation / arrosage_initial / en_croissance / mature / en_recolte
phase          = 'sol_vide'
current_action = 'vide'
action_timer   = 0
iteration      = 0

def decider_sol_vide():
    """Retourne (action, timer, nouvelle_phase) selon l'état du sol."""
    if state['hum'] < 45.0:
        return ('arrosage', 100, 'sol_vide')
    elif state['nit'] < 55.0:
        return ('fertilisation', 140, 'sol_vide')
    else:
        return ('plantation', 80, 'plantation')

while robot.step(timestep) != -1:
    iteration += 1

    # ── 1. Évolution naturelle ────────────────────────────────────────────────
    evap_rate    = evap * (1.0 + (state['temp'] - 20.0) / 40.0)
    state['hum'] = max(0.0, min(100.0, state['hum'] - evap_rate + random.uniform(-0.02, 0.02)))
    state['temp']= max(15.0, min(42.0, state['temp'] + random.uniform(-0.08, 0.12)))

    if phase == 'en_croissance':
        state['nit']  = max(0.0, state['nit']  - nit_consume          + random.uniform(-0.01,  0.01))
        state['phos'] = max(0.0, state['phos'] - p['phos_consume']    + random.uniform(-0.005, 0.005))
        state['pot']  = max(0.0, state['pot']  - p['pot_consume']     + random.uniform(-0.008, 0.008))

    # Cycle jour/nuit sinusoïdal (période = 1000 itérations) + bruit
    state['solar'] = max(0.0,
        p['solar_base'] * (0.5 + 0.5 * math.sin(2 * math.pi * iteration / 1000.0))
        + random.uniform(-20, 20))

    # Événements pluie rares selon profil culture
    state['rain'] = round(random.uniform(0.5, 8.0), 1) if random.random() < p['rain_prob'] else 0.0

    stress_hum      = max(0.0, 40.0 - state['hum']) * 1.2
    stress_nit      = max(0.0, 30.0 - state['nit']) * 0.8
    state['stress'] = min(100.0, (stress_hum + stress_nit) / 2.0)

    # ── 2. Effets continus pendant action active ──────────────────────────────
    if action_timer > 0:
        action_timer -= 1
        if current_action == 'arrosage':
            state['hum']  = min(100.0, state['hum'] + random.uniform(0.22, 0.38))
            water_used_L += 0.15
        elif current_action == 'fertilisation':
            state['nit']       = min(100.0, state['nit'] + random.uniform(0.38, 0.58))
            fertilizer_used_kg += 0.01

    # ── 3. Machine à états : décision quand timer == 0 ───────────────────────
    if action_timer == 0:

        # ── SOL VIDE : préparer puis planter ─────────────────────────────────
        if phase == 'sol_vide':
            action, timer, new_phase = decider_sol_vide()
            current_action = action
            action_timer   = timer
            phase          = new_phase

        # ── PLANTATION : attend la fin du timer puis lance arrosage initial ──
        elif phase == 'plantation':
            phase          = 'arrosage_initial'
            current_action = 'arrosage'
            action_timer   = 200
            harvest_done   = False

        # ── ARROSAGE INITIAL : plantes apparaissent chez le supervisor ────────
        elif phase == 'arrosage_initial':
            phase          = 'en_croissance'
            current_action = 'vide'

        # ── EN CROISSANCE : entretien ou transition mature ────────────────────
        elif phase == 'en_croissance':
            if state['growth'] >= 100.0:
                phase          = 'mature'
                current_action = 'vide'
            elif state['hum'] < 28.0:
                current_action = 'arrosage'
                action_timer   = 140
            elif state['nit'] < 18.0:
                current_action = 'fertilisation'
                action_timer   = 110
            else:
                current_action = 'vide'
                # pas de timer : on repassera ici au prochain step

        # ── MATURE : lancer la récolte ────────────────────────────────────────
        elif phase == 'mature':
            phase          = 'en_recolte'
            current_action = 'recolte'
            action_timer   = 220

        # ── EN RÉCOLTE : calculer le rendement puis repartir ─────────────────
        elif phase == 'en_recolte':
            harvest_count += 1
            print(f"[{p['culture']} #{fid}] Recolte #{harvest_count} (calculs economiques -> Spark)")
            state['nit']    = max(28.0, state['nit'] - random.uniform(10, 18))
            state['hum']    = max(38.0, state['hum'] - random.uniform(4,  8))
            state['growth'] = 0.0
            phase          = 'sol_vide'
            current_action = 'recolte_done'
            action_timer   = 8

        # ── RECOLTE_DONE : signal envoyé, reprise du cycle ───────────────────
        # Note: phase est déjà 'sol_vide' ici, current_action = 'recolte_done'
        # Ce cas est géré automatiquement car au prochain step action_timer == 0
        # ET phase == 'sol_vide', ce qui appelle decider_sol_vide() directement.
        # Mais current_action serait encore 'recolte_done', on doit le remettre à 'vide'.

    # Cas particulier : timer vient d'expirer sur recolte_done → forcer vide
    if action_timer == 0 and current_action == 'recolte_done':
        current_action = 'vide'
        # Le prochain step recallera la branche sol_vide avec current_action='vide'

    # ── 4. Croissance ─────────────────────────────────────────────────────────
    if phase == 'en_croissance':
        rate = 100.0 / p['growth_time']
        if state['hum'] > 50: rate *= 1.12
        if state['nit'] > 40: rate *= 1.08
        rate *= max(0.5, 1.0 - state['stress'] / 200.0)
        state['growth'] = min(100.0, state['growth'] + rate + random.uniform(0, 0.008))

    # ── 5. LED ────────────────────────────────────────────────────────────────
    if led:
        led.set(LED_COLORS.get(current_action, 0x00FF00))

    # ── 6. Log toutes les 200 itérations ─────────────────────────────────────
    if iteration % 200 == 0:
        print(f"[{p['culture']} #{fid}] phase={phase:<18} action={current_action:<14} "
              f"hum={state['hum']:5.1f}% nit={state['nit']:5.1f} "
              f"phos={state['phos']:5.1f} pot={state['pot']:5.1f} "
              f"growth={state['growth']:5.1f}% solar={state['solar']:5.0f}W/m² "
              f"stress={state['stress']:4.1f} timer={action_timer:3d}")

    # ── 7. Payload capteurs bruts (IPC supervisor + Kafka) ────────────────────
    payload = {
        'sensor_id':             f"field_{fid}_{p['culture'].lower()}",
        'timestamp':             datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
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
        with open(FIELD_FILE, 'w') as f:
            json.dump(payload, f)
    except Exception as e:
        print(f"[{p['culture']} #{fid}] Erreur ecriture: {e}")

    if KAFKA_ENABLED and iteration % 200 == 0:
        try:
            producer.send('sensor_data', payload)
        except Exception as e:
            print(f"[{p['culture']} #{fid}] Kafka send error: {e}")
