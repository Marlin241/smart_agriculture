import os
import io
import json
from datetime import datetime, timedelta, timezone
from typing import Optional

import boto3
import pandas as pd

SENSOR_NAMES = {
    'field_1': ('Blé', '🌾'),
    'field_2': ('Maïs', '🌽'),
    'field_3': ('Tournesol', '🌻'),
    'field_4': ('Soja', '🌱'),
}

ALERT_LABELS = {
    'sécheresse': "Manque d'eau",
    'excès_humidité': "Excès d'humidité",
    'déficit_azote': "Manque d'engrais azoté",
    'stress_élevé': "Stress végétatif élevé",
    'prêt_à_récolter': "Prêt à être récolté",
}

STAGE_LABELS = {
    'germination': 'Germination',
    'croissance': 'En croissance',
    'développement': 'En développement',
    'maturité': 'Mature',
    'récolte_imminente': 'Prêt à être récolté',
    'inconnu': 'Phase inconnue',
}

NPK_THRESHOLDS = {
    'nitrogen': 50,
    'phosphorus': 20,
    'potassium': 80,
}


def humanize_alert(code: str) -> str:
    return ALERT_LABELS.get(code, code)


def get_stress_label(score: float) -> tuple:
    if score <= 30:
        return 'Faible', 'green'
    elif score <= 65:
        return 'Modéré', 'orange'
    return 'Élevé', 'red'


def build_field_cards(df: pd.DataFrame) -> list:
    if df.empty:
        return [
            {'sensor_id': sid, 'name': name, 'emoji': emoji, 'no_data': True}
            for sid, (name, emoji) in SENSOR_NAMES.items()
        ]

    cards = []
    for sensor_id, (name, emoji) in SENSOR_NAMES.items():
        field_df = df[df['sensor_id'] == sensor_id]
        if field_df.empty:
            cards.append({'sensor_id': sensor_id, 'name': name, 'emoji': emoji, 'no_data': True})
            continue

        last = field_df.sort_values('timestamp').iloc[-1]
        alerts_flat = [a for row in field_df['alerts'] for a in (row if isinstance(row, list) else [])]

        if alerts_flat:
            status, color = 'Alerte', 'red'
        elif 'stress_score' in field_df.columns and float(last['stress_score']) > 65:
            status, color = 'Attention', 'orange'
        else:
            status, color = 'Sain', 'green'

        cards.append({
            'sensor_id': sensor_id,
            'name': name,
            'emoji': emoji,
            'no_data': False,
            'temp': round(float(field_df['temperature_c'].mean()), 1),
            'humidity': round(float(field_df['humidity_pct'].mean()), 1),
            'stage': STAGE_LABELS.get(str(last.get('plant_stage', 'inconnu')), 'Phase inconnue'),
            'status': status,
            'status_color': color,
            'alerts': list({humanize_alert(a) for a in alerts_flat}),
        })
    return cards
