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


def build_detail(df: pd.DataFrame, sensor_id: str) -> dict:
    if df.empty or sensor_id not in df['sensor_id'].values:
        return {}

    field_df = df[df['sensor_id'] == sensor_id].sort_values('timestamp')
    alerts_flat = [a for row in field_df['alerts'] for a in (row if isinstance(row, list) else [])]
    alert_counts = {}
    for a in alerts_flat:
        label = humanize_alert(a)
        alert_counts[label] = alert_counts.get(label, 0) + 1

    last = field_df.iloc[-1]
    stress = float(last['stress_score']) if 'stress_score' in field_df.columns else 0.0
    stress_label, stress_color = get_stress_label(stress)

    def safe_mean(col):
        if col in field_df.columns:
            return round(float(field_df[col].mean()), 1)
        return 0.0

    return {
        'timestamps': field_df['timestamp'].tolist(),
        'temperatures': field_df['temperature_c'].tolist(),
        'humidities': field_df['humidity_pct'].tolist(),
        'nitrogen': safe_mean('soil_nitrogen_mg_kg'),
        'phosphorus': safe_mean('soil_phosphorus_mg_kg'),
        'potassium': safe_mean('soil_potassium_mg_kg'),
        'alert_counts': alert_counts,
        'stress_score': round(stress, 1),
        'stress_label': stress_label,
        'stress_color': stress_color,
    }


def _trend(today_val, yesterday_val, higher_is_worse=True) -> tuple:
    if yesterday_val is None or yesterday_val == 0:
        return '~', 'gray'
    pct = (today_val - yesterday_val) / abs(yesterday_val) * 100
    if abs(pct) < 5:
        return '~', 'gray'
    going_up = pct > 0
    if higher_is_worse:
        return ('↑', 'red') if going_up else ('↓', 'green')
    return ('↑', 'green') if going_up else ('↓', 'red')


def build_report_table(today: dict, yesterday: Optional[dict]) -> list:
    rows = []
    for sensor_id, (name, emoji) in SENSOR_NAMES.items():
        t = today.get('par_champ', {}).get(sensor_id, {})
        y = yesterday.get('par_champ', {}).get(sensor_id) if yesterday else None

        def get_trend(key, higher_is_worse=True):
            tv = t.get(key)
            yv = y.get(key) if y else None
            if tv is None:
                return '-', '', ''
            arrow, color = _trend(tv, yv, higher_is_worse)
            return tv, arrow, color

        temp, t_arr, t_col = get_trend('temperature_moy', higher_is_worse=True)
        hum, h_arr, h_col = get_trend('humidity_moy', higher_is_worse=False)
        nit, n_arr, n_col = get_trend('nitrogen_moy', higher_is_worse=False)

        rows.append({
            'sensor_id': sensor_id,
            'name': f"{emoji} {name}",
            'temp': temp, 'temp_arrow': t_arr, 'temp_color': t_col,
            'humidity': hum, 'humidity_arrow': h_arr, 'humidity_color': h_col,
            'nitrogen': nit, 'nitrogen_arrow': n_arr, 'nitrogen_color': n_col,
            'nb_alertes': t.get('nb_alertes', 0),
            'no_yesterday': yesterday is None,
        })
    return rows
